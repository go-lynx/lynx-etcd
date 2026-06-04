package etcd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/selector"
	"github.com/go-lynx/lynx"
	"github.com/go-lynx/lynx-etcd/conf"
	"github.com/go-lynx/lynx/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// HTTPRateLimit implements the RateLimiter interface for HTTP rate limiting
// Etcd is a configuration center, not a rate limiting service, so this returns nil
func (p *PlugEtcd) HTTPRateLimit() middleware.Middleware {
	log.Debugf("Etcd plugin does not support HTTP rate limiting, returning nil")
	return nil
}

// GRPCRateLimit implements the RateLimiter interface for gRPC rate limiting
// Etcd is a configuration center, not a rate limiting service, so this returns nil
func (p *PlugEtcd) GRPCRateLimit() middleware.Middleware {
	log.Debugf("Etcd plugin does not support gRPC rate limiting, returning nil")
	return nil
}

// NewServiceRegistry implements the ServiceRegistry interface for service registration
func (p *PlugEtcd) NewServiceRegistry() registry.Registrar {
	if err := p.checkInitialized(); err != nil {
		log.Warnf("Etcd plugin not initialized, returning nil registrar: %v", err)
		return nil
	}

	if !p.conf.EnableRegister {
		log.Debugf("Service registration is disabled in etcd configuration")
		return nil
	}

	if p.client == nil {
		log.Errorf("Etcd client is nil")
		return nil
	}

	// Get registry namespace
	registryNamespace := p.conf.RegistryNamespace
	if registryNamespace == "" {
		registryNamespace = conf.DefaultRegistryNamespace
	}

	// Get TTL
	ttl := conf.DefaultTTL
	if p.conf.Ttl != nil {
		ttl = p.conf.Ttl.AsDuration()
		if ttl <= 0 {
			ttl = conf.DefaultTTL
		}
	}

	registrar := NewEtcdRegistrar(p.client, registryNamespace, ttl)
	// Track the registrar so its leases are revoked and keepalive goroutines are
	// stopped during cleanup.
	p.mu.Lock()
	p.registrar = registrar
	p.mu.Unlock()
	return registrar
}

// NewServiceDiscovery implements the ServiceRegistry interface for service discovery
func (p *PlugEtcd) NewServiceDiscovery() registry.Discovery {
	if err := p.checkInitialized(); err != nil {
		log.Warnf("Etcd plugin not initialized, returning nil discovery: %v", err)
		return nil
	}

	if !p.conf.EnableDiscovery {
		log.Debugf("Service discovery is disabled in etcd configuration")
		return nil
	}

	if p.client == nil {
		log.Errorf("Etcd client is nil")
		return nil
	}

	// Get registry namespace
	registryNamespace := p.conf.RegistryNamespace
	if registryNamespace == "" {
		registryNamespace = conf.DefaultRegistryNamespace
	}

	discovery := NewEtcdDiscovery(p.client, registryNamespace)
	// Track the discovery handle so its watcher goroutines are stopped during cleanup.
	p.mu.Lock()
	p.discovery = discovery
	p.mu.Unlock()
	return discovery
}

// NewNodeRouter implements the RouteManager interface for service routing
// Etcd is a configuration center, not a routing service, so this returns nil
func (p *PlugEtcd) NewNodeRouter(serviceName string) selector.NodeFilter {
	log.Debugf("Etcd plugin does not support service routing, returning nil for service: %s", serviceName)
	return nil
}

// GetConfig gets configuration from etcd configuration center
// This method retrieves the corresponding configuration source from etcd based on the provided prefix
func (p *PlugEtcd) GetConfig(fileName string, group string) (config.Source, error) {
	if err := p.checkInitialized(); err != nil {
		return nil, err
	}

	// For etcd, fileName is treated as key prefix
	prefix := fileName
	if prefix == "" {
		prefix = p.conf.Namespace
	}

	log.Infof("Getting config from etcd - Prefix: [%s]", prefix)

	// Create etcd config source
	source := NewEtcdConfigSource(p.client, prefix)
	// Back-reference so watchers created via source.Watch() are tracked for cleanup.
	source.plugin = p

	return source, nil
}

// registerConfigWatcher tracks a config watcher under p.mu so cleanup can stop it.
// If the plugin has already been destroyed, the watcher is stopped immediately.
func (p *PlugEtcd) registerConfigWatcher(prefix string, watcher *EtcdConfigWatcher) {
	if watcher == nil {
		return
	}
	p.mu.Lock()
	if atomic.LoadInt32(&p.destroyed) == 1 {
		p.mu.Unlock()
		_ = watcher.Stop()
		return
	}
	if p.configWatchers == nil {
		p.configWatchers = make(map[string]*EtcdConfigWatcher)
	}
	// Stop any prior watcher registered under the same prefix to avoid leaking it.
	if existing, ok := p.configWatchers[prefix]; ok && existing != nil && existing != watcher {
		_ = existing.Stop()
	}
	p.configWatchers[prefix] = watcher
	p.mu.Unlock()
}

// GetConfigSources gets all configuration sources for multi-config loading
// This method implements the MultiConfigControlPlane interface
func (p *PlugEtcd) GetConfigSources() ([]config.Source, error) {
	if err := p.checkInitialized(); err != nil {
		return nil, err
	}

	var sources []config.Source

	// Get main configuration source
	mainSource, err := p.getMainConfigSource()
	if err != nil {
		return nil, fmt.Errorf("failed to get main config source: %w", err)
	}
	if mainSource != nil {
		sources = append(sources, mainSource)
	}

	// Get additional configuration sources
	additionalSources, err := p.getAdditionalConfigSources()
	if err != nil {
		return nil, fmt.Errorf("failed to get additional config sources: %w", err)
	}
	sources = append(sources, additionalSources...)

	return sources, nil
}

// GetConfigWatchTargets returns the etcd prefixes that should feed the global config snapshot.
func (p *PlugEtcd) GetConfigWatchTargets(appName string) ([]lynx.ControlPlaneConfigTarget, error) {
	if err := p.checkInitialized(); err != nil {
		return nil, err
	}

	mainPrefix := p.conf.Namespace
	mainPriority := 0
	mainStrategy := ""
	if p.conf.ServiceConfig != nil {
		if p.conf.ServiceConfig.Prefix != "" {
			mainPrefix = p.conf.ServiceConfig.Prefix
		}
		mainPriority = int(p.conf.ServiceConfig.Priority)
		mainStrategy = p.conf.ServiceConfig.MergeStrategy
	}
	if mainPrefix == "" {
		mainPrefix = conf.DefaultNamespace
	}

	targets := []lynx.ControlPlaneConfigTarget{{
		FileName:      mainPrefix,
		Priority:      mainPriority,
		MergeStrategy: mainStrategy,
	}}
	if p.conf.ServiceConfig == nil {
		return targets, nil
	}

	for _, prefix := range p.conf.ServiceConfig.AdditionalPrefixes {
		if prefix == "" {
			prefix = p.conf.ServiceConfig.Prefix
		}
		if prefix == "" {
			prefix = p.conf.Namespace
		}
		if prefix == "" {
			continue
		}
		targets = append(targets, lynx.ControlPlaneConfigTarget{
			FileName:      prefix,
			Priority:      int(p.conf.ServiceConfig.Priority),
			MergeStrategy: p.conf.ServiceConfig.MergeStrategy,
		})
	}

	return targets, nil
}

// WatchControlPlaneConfig opens a running watcher for an etcd-backed config target.
func (p *PlugEtcd) WatchControlPlaneConfig(ctx context.Context, target lynx.ControlPlaneConfigTarget) (config.Watcher, error) {
	source, err := p.GetConfig(target.FileName, target.Group)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, fmt.Errorf("etcd config source is nil for prefix %s", target.FileName)
	}
	if _, err := source.Load(); err != nil {
		return nil, fmt.Errorf("failed to prime etcd config source %s: %w", target.FileName, err)
	}
	watcher, err := source.Watch()
	if err != nil {
		return nil, err
	}
	if err := lynx.StartControlPlaneWatcher(ctx, watcher); err != nil {
		_ = watcher.Stop()
		return nil, err
	}
	return watcher, nil
}

// getMainConfigSource gets the main configuration source based on service_config
func (p *PlugEtcd) getMainConfigSource() (config.Source, error) {
	if p.conf.ServiceConfig == nil {
		// Fallback to default behavior if service_config is not configured
		prefix := p.conf.Namespace
		if prefix == "" {
			prefix = conf.DefaultNamespace
		}

		log.Infof("Loading main configuration - Prefix: [%s]", prefix)

		return p.GetConfig(prefix, "")
	}

	// Use service_config configuration
	serviceConfig := p.conf.ServiceConfig

	// Determine prefix
	prefix := serviceConfig.Prefix
	if prefix == "" {
		prefix = p.conf.Namespace
	}

	log.Infof("Loading main configuration - Prefix: [%s]", prefix)

	return p.GetConfig(prefix, "")
}

// getAdditionalConfigSources gets additional configuration sources
func (p *PlugEtcd) getAdditionalConfigSources() ([]config.Source, error) {
	if p.conf.ServiceConfig == nil || len(p.conf.ServiceConfig.AdditionalPrefixes) == 0 {
		return nil, nil
	}

	serviceConfig := p.conf.ServiceConfig

	var sources []config.Source
	for _, prefix := range serviceConfig.AdditionalPrefixes {
		// Use service_config prefix as default if not specified
		if prefix == "" {
			prefix = serviceConfig.Prefix
		}
		if prefix == "" {
			prefix = p.conf.Namespace
		}

		log.Infof("Loading additional configuration - Prefix: [%s] Priority: [%d] Strategy: [%s]",
			prefix, serviceConfig.Priority, serviceConfig.MergeStrategy)

		source, err := p.GetConfig(prefix, "")
		if err != nil {
			log.Errorf("Failed to load additional configuration - Prefix: [%s] Error: %v",
				prefix, err)
			return nil, fmt.Errorf("failed to load additional config %s: %w", prefix, err)
		}

		sources = append(sources, source)
	}

	return sources, nil
}

// GetConfigValue gets configuration value from etcd
func (p *PlugEtcd) GetConfigValue(prefix, key string) (string, error) {
	if err := p.checkInitialized(); err != nil {
		return "", err
	}

	// Record configuration operation metrics
	if p.metrics != nil {
		p.metrics.RecordConfigOperation(prefix, "get", "start")
		defer func() {
			if p.metrics != nil {
				p.metrics.RecordConfigOperation(prefix, "get", "success")
			}
		}()
	}

	log.Infof("Getting config value - Prefix: [%s], Key: [%s]", prefix, key)

	// Execute with circuit breaker and retry mechanism
	var value string
	var lastErr error

	ctx := context.Background()
	err := p.circuitBreaker.Do(func() error {
		if p.retryManager != nil {
			return p.retryManager.DoWithRetry(func() error {
				val, err := p.getConfigValueFromEtcd(ctx, prefix, key)
				if err != nil {
					lastErr = err
					return err
				}
				value = val
				return nil
			})
		}
		val, err := p.getConfigValueFromEtcd(ctx, prefix, key)
		if err != nil {
			lastErr = err
			return err
		}
		value = val
		return nil
	})

	if err != nil {
		log.Errorf("Failed to get config value %s:%s after retries: %v", prefix, key, err)
		if p.metrics != nil {
			p.metrics.RecordConfigOperation(prefix, "get", "error")
		}
		return "", WrapClientError(lastErr, ErrCodeConfigGetFailed, "failed to get config value")
	}

	log.Infof("Successfully got config value - Prefix: [%s], Key: [%s]", prefix, key)
	return value, nil
}

// ControlPlaneCapabilities declares Etcd's explicit control plane contract.
func (p *PlugEtcd) ControlPlaneCapabilities() []lynx.ControlPlaneCapability {
	capabilities := []lynx.ControlPlaneCapability{
		lynx.ControlPlaneCapabilityConfig,
		lynx.ControlPlaneCapabilityWatcher,
	}
	if p.conf != nil && p.conf.EnableRegister {
		capabilities = append(capabilities, lynx.ControlPlaneCapabilityRegistry)
	}
	if p.conf != nil && p.conf.EnableDiscovery {
		capabilities = append(capabilities, lynx.ControlPlaneCapabilityDiscovery)
	}
	return capabilities
}

// getConfigValueFromEtcd gets configuration value from etcd client.
// The caller-supplied ctx governs the etcd request; an operation timeout is
// layered on top of it.
func (p *PlugEtcd) getConfigValueFromEtcd(ctx context.Context, prefix, key string) (string, error) {
	if p.client == nil {
		return "", fmt.Errorf("etcd client not initialized")
	}

	// Build full key path
	fullKey := buildKey(prefix, key)

	// Check cache first if enabled
	if p.conf.EnableCache {
		cacheKey := fmt.Sprintf("%s:%s", prefix, key)
		p.cacheMutex.RLock()
		if cached, exists := p.configCache[cacheKey]; exists {
			if value, ok := cached.(string); ok {
				p.cacheMutex.RUnlock()
				if p.metrics != nil {
					p.metrics.RecordCacheHit(p.GetNamespace())
				}
				return value, nil
			}
		}
		p.cacheMutex.RUnlock()
		if p.metrics != nil {
			p.metrics.RecordCacheMiss(p.GetNamespace())
		}
	}

	// Get from etcd. Derive the request context from the caller's ctx and apply
	// the configured operation timeout (falling back to the default when unset).
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := conf.DefaultTimeout
	if p.conf.Timeout != nil {
		timeout = p.conf.Timeout.AsDuration()
	}
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := p.client.Get(reqCtx, fullKey)
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("config key %s not found", fullKey)
	}

	value := string(resp.Kvs[0].Value)

	// Cache the value if enabled
	if p.conf.EnableCache {
		cacheKey := fmt.Sprintf("%s:%s", prefix, key)
		p.cacheMutex.Lock()
		p.configCache[cacheKey] = value
		p.cacheMutex.Unlock()
	}

	return value, nil
}

// EtcdConfigSource implements config.Source for etcd
type EtcdConfigSource struct {
	client *clientv3.Client
	prefix string
	// plugin, when set, is used to register watchers created by this source into
	// the plugin's watcher map so they are stopped during cleanup.
	plugin *PlugEtcd
}

// NewEtcdConfigSource creates a new etcd config source
func NewEtcdConfigSource(client *clientv3.Client, prefix string) *EtcdConfigSource {
	return &EtcdConfigSource{
		client: client,
		prefix: prefix,
	}
}

// Load implements config.Source interface
func (s *EtcdConfigSource) Load() ([]*config.KeyValue, error) {
	if s.client == nil {
		return nil, fmt.Errorf("etcd client not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get all keys with prefix
	resp, err := s.client.Get(ctx, s.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to load config from etcd: %w", err)
	}

	var kvs []*config.KeyValue
	for _, kv := range resp.Kvs {
		// Extract key name (remove prefix)
		key := strings.TrimPrefix(string(kv.Key), s.prefix)
		key = strings.TrimPrefix(key, "/")

		// Convert etcd key path to config key (replace / with .)
		key = strings.ReplaceAll(key, "/", ".")

		kvs = append(kvs, &config.KeyValue{
			Key:   key,
			Value: kv.Value,
		})
	}

	return kvs, nil
}

// Watch implements config.Source interface
func (s *EtcdConfigSource) Watch() (config.Watcher, error) {
	if s.client == nil {
		return nil, fmt.Errorf("etcd client not initialized")
	}

	// Create a config watcher adapter
	watcher := NewEtcdConfigWatcher(s.client, s.prefix)
	// Register the watcher with the owning plugin so cleanup can stop it.
	if s.plugin != nil {
		s.plugin.registerConfigWatcher(s.prefix, watcher)
	}
	return watcher, nil
}

// buildKey builds full key path from prefix and key
func buildKey(prefix, key string) string {
	if prefix == "" {
		return key
	}
	if key == "" {
		return prefix
	}
	return filepath.Join(prefix, key)
}
