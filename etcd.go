package etcd

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-lynx/lynx/app"
	"github.com/go-lynx/lynx/app/log"
	"github.com/go-lynx/lynx/plugins"
	"github.com/go-lynx/lynx/plugins/etcd/conf"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Plugin metadata
// Plugin metadata defining basic plugin information
const (
	// pluginName is the unique identifier for the Etcd configuration center plugin, used to identify this plugin in the plugin system.
	pluginName = "etcd.config.center"

	// pluginVersion represents the current version of the Etcd configuration center plugin.
	pluginVersion = "v1.0.0"

	// pluginDescription briefly describes the functionality of the Etcd configuration center plugin.
	pluginDescription = "etcd configuration center plugin for lynx framework"

	// confPrefix is the configuration prefix used when loading Etcd configuration.
	confPrefix = "lynx.etcd"
)

// PlugEtcd represents an Etcd configuration center plugin instance
type PlugEtcd struct {
	*plugins.BasePlugin
	conf *conf.Etcd

	// Etcd client
	client *clientv3.Client

	// Enhanced components
	metrics        *Metrics
	retryManager   *RetryManager
	circuitBreaker *CircuitBreaker

	// State management - using atomic operations to improve concurrency safety
	mu                   sync.RWMutex
	initialized          int32 // Use int32 instead of bool to support atomic operations
	destroyed            int32 // Use int32 instead of bool to support atomic operations
	healthCheckCh        chan struct{}
	healthCheckCloseOnce sync.Once // Protect against multiple close operations

	// Configuration watchers
	configWatchers map[string]*ConfigWatcher
	watcherMutex   sync.RWMutex // Watcher mutex

	// Cache system
	configCache map[string]interface{} // Configuration cache
	cacheMutex  sync.RWMutex           // Cache mutex
}

// NewEtcdConfigCenter creates a new Etcd configuration center.
// This function initializes the plugin's basic information and returns a pointer to PlugEtcd.
func NewEtcdConfigCenter() *PlugEtcd {
	return &PlugEtcd{
		BasePlugin: plugins.NewBasePlugin(
			// Generate unique plugin ID
			plugins.GeneratePluginID("", pluginName, pluginVersion),
			// Plugin name
			pluginName,
			// Plugin description
			pluginDescription,
			// Plugin version
			pluginVersion,
			// Configuration prefix
			confPrefix,
			// Weight
			math.MaxInt,
		),
		healthCheckCh:  make(chan struct{}),
		configWatchers: make(map[string]*ConfigWatcher),
		configCache:    make(map[string]interface{}),
	}
}

// InitializeResources implements custom initialization logic for the Etcd plugin.
// This function loads and validates Etcd configuration, using default configuration if none is provided.
func (p *PlugEtcd) InitializeResources(rt plugins.Runtime) error {
	// Initialize an empty configuration structure
	p.conf = &conf.Etcd{}

	// Scan and load Etcd configuration from runtime configuration
	err := rt.GetConfig().Value(confPrefix).Scan(p.conf)
	if err != nil {
		return WrapInitError(err, "failed to scan etcd configuration")
	}

	// Set default configuration
	p.setDefaultConfig()

	// Validate configuration
	if err := p.validateConfig(); err != nil {
		return WrapInitError(err, "configuration validation failed")
	}

	// Initialize enhanced components
	if err := p.initComponents(); err != nil {
		return WrapInitError(err, "failed to initialize components")
	}

	return nil
}

// setDefaultConfig sets default configuration
func (p *PlugEtcd) setDefaultConfig() {
	// Default namespace
	if p.conf.Namespace == "" {
		p.conf.Namespace = conf.DefaultNamespace
	}
	// Default timeout is 10 seconds
	if p.conf.Timeout == nil {
		p.conf.Timeout = conf.GetDefaultTimeout()
	}
	// Default dial timeout is 5 seconds
	if p.conf.DialTimeout == nil {
		p.conf.DialTimeout = conf.GetDefaultDialTimeout()
	}
}

// validateConfig validates configuration
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

// initComponents initializes enhanced components
func (p *PlugEtcd) initComponents() error {
	// Initialize monitoring metrics
	if p.conf.EnableMetrics {
		p.metrics = NewEtcdMetrics()
	}

	// Initialize retry manager
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

	// Initialize circuit breaker
	p.circuitBreaker = NewCircuitBreaker(0.5)

	return nil
}

// checkInitialized unified state checking method ensuring thread safety
func (p *PlugEtcd) checkInitialized() error {
	if atomic.LoadInt32(&p.initialized) == 0 {
		return NewInitError("Etcd plugin not initialized")
	}
	if atomic.LoadInt32(&p.destroyed) == 1 {
		return NewInitError("Etcd plugin has been destroyed")
	}
	return nil
}

// setInitialized atomically sets initialization status
func (p *PlugEtcd) setInitialized() {
	atomic.StoreInt32(&p.initialized, 1)
}

// setDestroyed atomically sets destruction status
func (p *PlugEtcd) setDestroyed() {
	atomic.StoreInt32(&p.destroyed, 1)
}

// StartupTasks implements custom startup logic for the Etcd plugin.
// This function configures and starts the Etcd configuration center, adding necessary middleware and configuration options.
func (p *PlugEtcd) StartupTasks() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if atomic.LoadInt32(&p.initialized) == 1 {
		return NewInitError("Etcd plugin already initialized")
	}

	// Record startup operation metrics
	if p.metrics != nil {
		p.metrics.RecordClientOperation("startup", "start")
		defer func() {
			if p.metrics != nil {
				p.metrics.RecordClientOperation("startup", "success")
			}
		}()
	}

	// Use Lynx application Helper to log Etcd plugin initialization information.
	log.Infof("Initializing etcd plugin with endpoints: %v, namespace: %s", p.conf.Endpoints, p.conf.Namespace)

	// Initialize Etcd client
	client, err := p.initEtcdClient()
	if err != nil {
		log.Errorf("Failed to initialize Etcd client: %v", err)
		if p.metrics != nil {
			p.metrics.RecordClientOperation("startup", "error")
		}
		return WrapInitError(err, "failed to initialize Etcd client")
	}

	// Save client instance
	p.client = client

	// Set the Etcd configuration center as the Lynx application's control plane.
	err = app.Lynx().SetControlPlane(p)
	if err != nil {
		log.Errorf("Failed to set control plane: %v", err)
		if p.metrics != nil {
			p.metrics.RecordClientOperation("startup", "error")
		}
		return WrapInitError(err, "failed to set control plane")
	}

	// Get the Lynx application's control plane startup configuration.
	cfg, err := app.Lynx().InitControlPlaneConfig()
	if err != nil {
		log.Errorf("Failed to init control plane config: %v", err)
		if p.metrics != nil {
			p.metrics.RecordClientOperation("startup", "error")
		}
		return WrapInitError(err, "failed to init control plane config")
	}

	// Load plugins from the plugin list.
	app.Lynx().GetPluginManager().LoadPlugins(cfg)

	p.setInitialized()
	log.Infof("Etcd plugin initialized successfully")
	return nil
}

// initEtcdClient initializes Etcd client
func (p *PlugEtcd) initEtcdClient() (*clientv3.Client, error) {
	if len(p.conf.Endpoints) == 0 {
		return nil, fmt.Errorf("etcd endpoints are required")
	}

	// Get timeout
	timeout := conf.DefaultTimeout
	if p.conf.Timeout != nil {
		timeout = p.conf.Timeout.AsDuration()
	}

	dialTimeout := conf.DefaultDialTimeout
	if p.conf.DialTimeout != nil {
		dialTimeout = p.conf.DialTimeout.AsDuration()
	}

	// Build client configuration
	cfg := clientv3.Config{
		Endpoints:   p.conf.Endpoints,
		DialTimeout: dialTimeout,
		Username:    p.conf.Username,
		Password:    p.conf.Password,
	}

	// Configure TLS if enabled
	if p.conf.EnableTls {
		tlsConfig, err := buildTLSConfig(p.conf.CertFile, p.conf.KeyFile, p.conf.CaFile)
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		cfg.TLS = tlsConfig
	}

	// Create client
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	log.Infof("Etcd client initialized - Endpoints: %v, Namespace: %s",
		p.conf.Endpoints, p.conf.Namespace)

	return client, nil
}

// GetMetrics gets monitoring metrics
func (p *PlugEtcd) GetMetrics() *Metrics {
	return p.metrics
}

// IsInitialized checks if initialized
func (p *PlugEtcd) IsInitialized() bool {
	return atomic.LoadInt32(&p.initialized) == 1
}

// IsDestroyed checks if destroyed
func (p *PlugEtcd) IsDestroyed() bool {
	return atomic.LoadInt32(&p.destroyed) == 1
}

// GetEtcdConfig gets Etcd configuration
func (p *PlugEtcd) GetEtcdConfig() *conf.Etcd {
	return p.conf
}

// GetNamespace returns the namespace
func (p *PlugEtcd) GetNamespace() string {
	if p.conf != nil {
		return p.conf.Namespace
	}
	return conf.DefaultNamespace
}

// GetClient returns the etcd client
func (p *PlugEtcd) GetClient() *clientv3.Client {
	return p.client
}
