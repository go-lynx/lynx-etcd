package etcd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-lynx/lynx"
	"github.com/go-lynx/lynx-etcd/conf"
	"github.com/go-lynx/lynx/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// IsContextAware asserts that the plugin's lifecycle genuinely observes context
// cancellation: the core BasePlugin drives StartContext/StopContext and routes
// into the context-aware step hooks below.
func (p *PlugEtcd) IsContextAware() bool {
	return true
}

// StartupTasksContext is the context-aware startup hook. The core BasePlugin
// drives the lifecycle state machine (status transitions, events, health check)
// and passes the caller's context straight through so cancellation is real.
func (p *PlugEtcd) StartupTasksContext(ctx context.Context) error {
	return p.startupTasksContext(ctx)
}

// CleanupTasksContext is the context-aware cleanup hook driven by the core BasePlugin.
func (p *PlugEtcd) CleanupTasksContext(ctx context.Context) error {
	return p.cleanupTasksContext(ctx)
}

func (p *PlugEtcd) startupTasksContext(ctx context.Context) (startErr error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("etcd startup canceled before execution: %w", err)
	}
	if atomic.LoadInt32(&p.initialized) == 1 {
		return NewInitError("Etcd plugin already initialized")
	}
	if atomic.LoadInt32(&p.destroyed) == 1 {
		return NewInitError("Etcd plugin already destroyed")
	}

	if p.metrics != nil {
		p.metrics.RecordClientOperation("startup", "start")
		defer func() {
			if p.metrics == nil {
				return
			}
			if startErr != nil {
				p.metrics.RecordClientOperation("startup", "error")
				return
			}
			p.metrics.RecordClientOperation("startup", "success")
		}()
	}

	log.Infof("Initializing etcd plugin with endpoints: %v, namespace: %s", p.conf.Endpoints, p.conf.Namespace)

	client, err := p.initEtcdClientContext(ctx)
	if err != nil {
		log.Errorf("Failed to initialize Etcd client: %v", err)
		return WrapInitError(err, "failed to initialize Etcd client")
	}
	p.client = client

	defer func() {
		if startErr == nil || p.client == nil {
			return
		}
		if closeErr := p.client.Close(); closeErr != nil {
			log.Errorf("Failed to close etcd client after startup failure: %v", closeErr)
			startErr = errors.Join(startErr, fmt.Errorf("close etcd client after startup failure: %w", closeErr))
		}
		p.client = nil
	}()

	log.Infof("Verifying etcd connectivity to endpoints: %v", p.conf.Endpoints)
	if p.retryManager != nil {
		err = p.retryManager.DoWithRetryContext(ctx, func() error {
			return p.checkClientConnectionContext(ctx)
		})
	} else {
		err = p.checkClientConnectionContext(ctx)
	}
	if err != nil {
		log.Errorf("Etcd connectivity verification failed: %v", err)
		return WrapInitError(err, "etcd endpoints are unreachable during startup")
	}
	log.Infof("Etcd connectivity verified")
	p.setInitialized()

	defer func() {
		if startErr != nil {
			p.clearInitialized()
		}
	}()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("etcd startup canceled before setting control plane: %w", err)
	}
	if err := lynx.Lynx().SetControlPlane(p); err != nil {
		log.Errorf("Failed to set control plane: %v", err)
		return WrapInitError(err, "failed to set control plane")
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("etcd startup canceled before loading control plane config: %w", err)
	}
	cfg, err := lynx.Lynx().InitControlPlaneConfig()
	if err != nil {
		log.Errorf("Failed to init control plane config: %v", err)
		return WrapInitError(err, "failed to init control plane config")
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("etcd startup canceled before publishing runtime resources: %w", err)
	}
	if p.rt != nil {
		if err := lynx.RegisterControlPlaneCapabilityResources(p.rt, pluginName, p); err != nil {
			return fmt.Errorf("failed to register etcd runtime capability resources: %w", err)
		}
		if err := p.rt.RegisterPrivateResource("client", p.client); err != nil {
			log.Warnf("failed to register etcd private client resource: %v", err)
		}
		if err := p.rt.RegisterPrivateResource("etcd_client", p.client); err != nil {
			log.Warnf("failed to register etcd private sdk client resource: %v", err)
		}
		if p.conf.EnableRegister {
			if registrar := p.NewServiceRegistry(); registrar != nil {
				if err := p.rt.RegisterSharedResource(pluginName+".service_registry", registrar); err != nil {
					log.Warnf("failed to register etcd service registry resource: %v", err)
				}
			}
		}
		if p.conf.EnableDiscovery {
			if discovery := p.NewServiceDiscovery(); discovery != nil {
				if err := p.rt.RegisterSharedResource(pluginName+".service_discovery", discovery); err != nil {
					log.Warnf("failed to register etcd service discovery resource: %v", err)
				}
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("etcd startup canceled before loading dependent plugins: %w", err)
	}
	if err := lynx.Lynx().GetPluginManager().LoadPlugins(cfg); err != nil {
		log.Errorf("Failed to load dependent plugins from Etcd control plane config: %v", err)
		return WrapInitError(err, "failed to load dependent plugins")
	}

	log.Infof("Etcd plugin initialized successfully")
	return nil
}

func (p *PlugEtcd) initEtcdClientContext(ctx context.Context) (*clientv3.Client, error) {
	if len(p.conf.Endpoints) == 0 {
		return nil, fmt.Errorf("etcd endpoints are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	dialTimeout := conf.DefaultDialTimeout
	if p.conf.DialTimeout != nil {
		dialTimeout = p.conf.DialTimeout.AsDuration()
	}

	cfg := clientv3.Config{
		Endpoints:   p.conf.Endpoints,
		DialTimeout: dialTimeout,
		Username:    p.conf.Username,
		Password:    p.conf.Password,
		Context:     ctx,
	}

	if p.conf.EnableTls {
		tlsConfig, err := buildTLSConfig(p.conf.CertFile, p.conf.KeyFile, p.conf.CaFile)
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		cfg.TLS = tlsConfig
	}

	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	log.Infof("Etcd client initialized - Endpoints: %v, Namespace: %s", p.conf.Endpoints, p.conf.Namespace)
	return client, nil
}

func (p *PlugEtcd) checkEtcdHealthContext(ctx context.Context) error {
	if err := p.checkInitialized(); err != nil {
		return err
	}
	if p.client == nil {
		return NewInitError("Etcd client is nil")
	}

	if p.metrics != nil {
		p.metrics.RecordHealthCheck("start")
		defer func() {
			if p.metrics != nil && ctx.Err() == nil {
				p.metrics.RecordHealthCheck("success")
			}
		}()
	}

	log.Infof("Checking etcd configuration center health")

	var healthErr error
	err := p.circuitBreaker.Do(func() error {
		if p.retryManager != nil {
			return p.retryManager.DoWithRetryContext(ctx, func() error {
				if err := p.checkClientConnectionContext(ctx); err != nil {
					healthErr = err
					return err
				}
				if err := p.checkConfigManagementHealth(); err != nil {
					healthErr = err
					return err
				}
				return nil
			})
		}
		if err := p.checkClientConnectionContext(ctx); err != nil {
			healthErr = err
			return err
		}
		if err := p.checkConfigManagementHealth(); err != nil {
			healthErr = err
			return err
		}
		return nil
	})

	if err != nil {
		if healthErr == nil {
			healthErr = err
		}
		log.Errorf("Etcd configuration center health check failed: %v", healthErr)
		if p.metrics != nil {
			p.metrics.RecordHealthCheck("error")
		}
		return WrapClientError(healthErr, ErrCodeHealthCheckFailed, "Etcd configuration center health check failed")
	}

	log.Infof("Etcd configuration center health check passed")
	return nil
}

func (p *PlugEtcd) checkClientConnectionContext(ctx context.Context) error {
	if p.client == nil {
		return NewInitError("Etcd client is nil")
	}

	testPrefix := p.conf.Namespace
	if testPrefix == "" {
		testPrefix = conf.DefaultNamespace
	}

	checkCtx, cancel := p.withOperationTimeout(ctx)
	defer cancel()

	_, err := p.client.Get(checkCtx, buildKey(testPrefix, "health.check.key"))
	if err != nil {
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "key not found") {
			log.Debugf("Client connection test passed (key not found is expected)")
			return nil
		}
		return fmt.Errorf("client connection test failed: %w", err)
	}

	return nil
}

func (p *PlugEtcd) cleanupTasksContext(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if atomic.LoadInt32(&p.destroyed) == 1 {
		return NewInitError("Etcd plugin already destroyed")
	}

	log.Infof("Cleaning up etcd plugin")
	var errs []error

	// mu is already held; stop watchers directly without acquiring watcherMutex to
	// avoid nested-lock ordering issues (cleanupTasksContext always runs under mu).
	for namespace, watcher := range p.configWatchers {
		if watcher == nil {
			continue
		}
		if err := watcher.Stop(); err != nil {
			log.Errorf("Failed to stop config watcher for namespace %s: %v", namespace, err)
			errs = append(errs, fmt.Errorf("stop config watcher %s: %w", namespace, err))
			continue
		}
		log.Debugf("Stopped config watcher for namespace: %s", namespace)
	}
	p.configWatchers = make(map[string]*EtcdConfigWatcher)

	// Revoke leases and stop keepalive goroutines owned by the registrar.
	if p.registrar != nil {
		if err := p.registrar.Close(); err != nil {
			log.Errorf("Failed to close etcd registrar: %v", err)
			errs = append(errs, fmt.Errorf("close etcd registrar: %w", err))
		}
		p.registrar = nil
	}

	// Stop discovery watcher goroutines.
	if p.discovery != nil {
		if err := p.discovery.Close(); err != nil {
			log.Errorf("Failed to close etcd discovery: %v", err)
			errs = append(errs, fmt.Errorf("close etcd discovery: %w", err))
		}
		p.discovery = nil
	}

	if p.client != nil {
		if err := p.client.Close(); err != nil {
			log.Errorf("Failed to close etcd client: %v", err)
			errs = append(errs, fmt.Errorf("close etcd client: %w", err))
		} else {
			log.Infof("Etcd client closed successfully")
		}
		p.client = nil
	}

	p.healthCheckCloseOnce.Do(func() {
		close(p.healthCheckCh)
	})

	p.cacheMutex.Lock()
	p.configCache = make(map[string]any)
	p.cacheMutex.Unlock()

	p.setDestroyed()
	if err := ctx.Err(); err != nil {
		errs = append(errs, fmt.Errorf("etcd cleanup canceled: %w", err))
	}

	if joined := errors.Join(errs...); joined != nil {
		return joined
	}

	log.Infof("Etcd plugin cleanup completed")
	return nil
}

func (p *PlugEtcd) startupContext() (context.Context, context.CancelFunc) {
	timeout := conf.DefaultTimeout
	if p.conf != nil && p.conf.Timeout != nil {
		timeout = p.conf.Timeout.AsDuration()
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (p *PlugEtcd) shutdownContext() (context.Context, context.CancelFunc) {
	timeout := 10 * time.Second
	if p.conf != nil && p.conf.ShutdownTimeout != nil {
		timeout = p.conf.ShutdownTimeout.AsDuration()
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (p *PlugEtcd) withOperationTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := conf.DefaultTimeout
	if p.conf != nil && p.conf.Timeout != nil {
		timeout = p.conf.Timeout.AsDuration()
	}
	return context.WithTimeout(ctx, timeout)
}
