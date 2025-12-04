package etcd

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/selector"
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
// Etcd is a configuration center, not a service registry, so this returns nil
func (p *PlugEtcd) NewServiceRegistry() registry.Registrar {
	log.Debugf("Etcd plugin does not support service registration, returning nil")
	return nil
}

// NewServiceDiscovery implements the ServiceRegistry interface for service discovery
// Etcd is a configuration center, not a service discovery service, so this returns nil
func (p *PlugEtcd) NewServiceDiscovery() registry.Discovery {
	log.Debugf("Etcd plugin does not support service discovery, returning nil")
	return nil
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

	return source, nil
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

	// Sort by priority if needed (for now, just append in order)
	sort.Slice(sources, func(i, j int) bool {
		return false
	})

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

	err := p.circuitBreaker.Do(func() error {
		if p.retryManager != nil {
			return p.retryManager.DoWithRetry(func() error {
				val, err := p.getConfigValueFromEtcd(prefix, key)
				if err != nil {
					lastErr = err
					return err
				}
				value = val
				return nil
			})
		}
		val, err := p.getConfigValueFromEtcd(prefix, key)
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

// getConfigValueFromEtcd gets configuration value from etcd client
func (p *PlugEtcd) getConfigValueFromEtcd(prefix, key string) (string, error) {
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

	// Get from etcd
	ctx, cancel := context.WithTimeout(context.Background(), p.conf.Timeout.AsDuration())
	defer cancel()

	resp, err := p.client.Get(ctx, fullKey)
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
