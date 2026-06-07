// Package etcd provides an etcd configuration-centre plugin for the Lynx framework.
// It connects to an etcd cluster, watches configuration key prefixes for live changes,
// and exposes those values to other plugins through the Lynx runtime resource registry.
// Enhanced components include a circuit breaker, a configurable retry manager, and
// Prometheus metrics covering client operations, config changes, and cache hit rates.
package etcd

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-lynx/lynx-etcd/conf"
	"github.com/go-lynx/lynx/plugins"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	pluginName        = "etcd.config.center"
	pluginVersion     = "v1.6.3"
	pluginDescription = "etcd configuration center plugin for lynx framework"
	// confPrefix is the config subtree this plugin reads its settings from.
	confPrefix = "lynx.etcd"
)

// PlugEtcd is the etcd configuration-centre plugin instance.
type PlugEtcd struct {
	*plugins.BasePlugin
	conf *conf.Etcd
	rt   plugins.Runtime

	client *clientv3.Client

	metrics        *Metrics
	retryManager   *RetryManager
	circuitBreaker *CircuitBreaker

	// initialized/destroyed are accessed atomically so state can be checked
	// without holding mu on the hot path.
	mu                   sync.RWMutex
	initialized          int32
	destroyed            int32
	healthCheckCh        chan struct{}
	healthCheckCloseOnce sync.Once

	// configWatchers is always accessed under mu.
	configWatchers map[string]*EtcdConfigWatcher

	// registrar and discovery are tracked under mu so their background
	// goroutines (lease keepalive, service watchers) can be stopped on cleanup.
	registrar *EtcdRegistrar
	discovery *EtcdDiscovery

	configCache map[string]any
	cacheMutex  sync.RWMutex
}

// NewEtcdConfigCenter returns an uninitialized etcd config-centre plugin.
// The weight is MaxInt so it loads before plugins that depend on its config.
func NewEtcdConfigCenter() *PlugEtcd {
	return &PlugEtcd{
		BasePlugin: plugins.NewBasePlugin(
			plugins.GeneratePluginID("", pluginName, pluginVersion),
			pluginName,
			pluginDescription,
			pluginVersion,
			confPrefix,
			math.MaxInt,
		),
		healthCheckCh:  make(chan struct{}),
		configWatchers: make(map[string]*EtcdConfigWatcher),
		configCache:    make(map[string]any),
	}
}

// InitializeResources loads, defaults, and validates the etcd config and builds
// the retry/circuit-breaker/metrics components. It does not yet dial etcd; that
// happens in StartupTasks.
func (p *PlugEtcd) InitializeResources(rt plugins.Runtime) error {
	p.rt = rt
	p.conf = &conf.Etcd{}

	err := rt.GetConfig().Value(confPrefix).Scan(p.conf)
	if err != nil {
		return WrapInitError(err, "failed to scan etcd configuration")
	}

	p.setDefaultConfig()

	if err := p.validateConfig(); err != nil {
		return WrapInitError(err, "configuration validation failed")
	}

	if err := p.initComponents(); err != nil {
		return WrapInitError(err, "failed to initialize components")
	}

	return nil
}

// setDefaultConfig fills in defaults for any unset config fields.
func (p *PlugEtcd) setDefaultConfig() {
	if p.conf.Namespace == "" {
		p.conf.Namespace = conf.DefaultNamespace
	}
	if p.conf.Timeout == nil {
		p.conf.Timeout = conf.GetDefaultTimeout()
	}
	if p.conf.DialTimeout == nil {
		p.conf.DialTimeout = conf.GetDefaultDialTimeout()
	}
}

func (p *PlugEtcd) validateConfig() error {
	if p.conf == nil {
		return NewConfigError("configuration is required")
	}

	if len(p.conf.Endpoints) == 0 {
		return NewConfigError("etcd endpoints are required")
	}

	validator := NewValidator(p.conf)
	result := validator.Validate()
	if !result.IsValid {
		return NewConfigError(result.Errors[0].Error())
	}

	return nil
}

// initComponents wires up metrics, the retry manager, and the circuit breaker.
// Retries default to 3 attempts with a 1s base interval; the breaker trips at a
// 50% failure rate.
func (p *PlugEtcd) initComponents() error {
	if p.conf.EnableMetrics {
		p.metrics = NewEtcdMetrics()
	}

	if p.conf.EnableRetry {
		maxRetries := int(p.conf.MaxRetryTimes)
		if maxRetries <= 0 {
			maxRetries = 3
		}
		retryInterval := 1 * time.Second
		if p.conf.RetryInterval != nil {
			retryInterval = p.conf.RetryInterval.AsDuration()
		}
		p.retryManager = NewRetryManager(maxRetries, retryInterval)
	}

	p.circuitBreaker = NewCircuitBreaker(0.5)

	return nil
}

// checkInitialized reports an error unless the plugin is initialized and not yet
// destroyed. Safe to call concurrently.
func (p *PlugEtcd) checkInitialized() error {
	if atomic.LoadInt32(&p.initialized) == 0 {
		return NewInitError("Etcd plugin not initialized")
	}
	if atomic.LoadInt32(&p.destroyed) == 1 {
		return NewInitError("Etcd plugin has been destroyed")
	}
	return nil
}

func (p *PlugEtcd) setInitialized() {
	atomic.StoreInt32(&p.initialized, 1)
}

func (p *PlugEtcd) clearInitialized() {
	atomic.StoreInt32(&p.initialized, 0)
}

func (p *PlugEtcd) setDestroyed() {
	atomic.StoreInt32(&p.destroyed, 1)
}

// StartupTasks dials etcd, verifies connectivity, and wires the plugin into the
// Lynx control plane. It runs under a startup-timeout context.
func (p *PlugEtcd) StartupTasks() error {
	ctx, cancel := p.startupContext()
	defer cancel()
	return p.startupTasksContext(ctx)
}

// GetMetrics returns the metrics instance, or nil when metrics are disabled.
func (p *PlugEtcd) GetMetrics() *Metrics {
	return p.metrics
}

func (p *PlugEtcd) IsInitialized() bool {
	return atomic.LoadInt32(&p.initialized) == 1
}

func (p *PlugEtcd) IsDestroyed() bool {
	return atomic.LoadInt32(&p.destroyed) == 1
}

func (p *PlugEtcd) GetEtcdConfig() *conf.Etcd {
	return p.conf
}

// GetNamespace returns the configured key namespace, or the default if unset.
func (p *PlugEtcd) GetNamespace() string {
	if p.conf != nil {
		return p.conf.Namespace
	}
	return conf.DefaultNamespace
}

// GetClient returns the underlying etcd client (nil before startup / after cleanup).
func (p *PlugEtcd) GetClient() *clientv3.Client {
	return p.client
}
