package etcd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-lynx/lynx/app/log"
	"github.com/go-lynx/lynx/plugins/etcd/conf"
)

// CheckHealth performs a health check.
func (p *PlugEtcd) CheckHealth() error {
	if err := p.checkInitialized(); err != nil {
		return err
	}

	// Check etcd client
	if p.client == nil {
		return NewInitError("Etcd client is nil")
	}

	// Perform actual health check of the etcd configuration center
	return p.checkEtcdHealth()
}

// checkEtcdHealth checks the health of the etcd configuration center.
func (p *PlugEtcd) checkEtcdHealth() error {
	// Record the start of the health check
	if p.metrics != nil {
		p.metrics.RecordHealthCheck("start")
		defer func() {
			if p.metrics != nil {
				p.metrics.RecordHealthCheck("success")
			}
		}()
	}

	log.Infof("Checking etcd configuration center health")

	// Execute health checks using circuit breaker and retry mechanisms
	var healthErr error
	err := p.circuitBreaker.Do(func() error {
		if p.retryManager != nil {
			return p.retryManager.DoWithRetry(func() error {
				// 1) Check client connection status
				if err := p.checkClientConnection(); err != nil {
					healthErr = err
					return err
				}

				// 2) Check configuration management functionality
				if err := p.checkConfigManagementHealth(); err != nil {
					healthErr = err
					return err
				}

				return nil
			})
		}
		// Without retry manager
		if err := p.checkClientConnection(); err != nil {
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
		log.Errorf("Etcd configuration center health check failed: %v", healthErr)
		if p.metrics != nil {
			p.metrics.RecordHealthCheck("error")
		}
		return WrapClientError(healthErr, ErrCodeHealthCheckFailed, "Etcd configuration center health check failed")
	}

	log.Infof("Etcd configuration center health check passed")
	return nil
}

// checkClientConnection verifies client connection status.
func (p *PlugEtcd) checkClientConnection() error {
	// Try to get a configuration value to validate connectivity
	testPrefix := p.conf.Namespace
	if testPrefix == "" {
		testPrefix = conf.DefaultNamespace
	}

	// Try to get a test configuration key
	// If the key doesn't exist, that's fine - we just need to verify we can connect
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := p.client.Get(ctx, buildKey(testPrefix, "health.check.key"))
	if err != nil {
		// Check if it's a not found error (which is acceptable)
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "key not found") {
			log.Debugf("Client connection test passed (key not found is expected)")
			return nil
		}
		// Other errors indicate connection problems
		return fmt.Errorf("client connection test failed: %v", err)
	}

	// Key exists and was retrieved successfully
	return nil
}

// checkConfigManagementHealth checks configuration management functionality.
func (p *PlugEtcd) checkConfigManagementHealth() error {
	// Check status of components related to configuration management
	if p.configWatchers == nil {
		return fmt.Errorf("config watchers not initialized")
	}

	// Check whether there are active config watchers
	configWatcherCount := len(p.configWatchers)
	log.Debugf("Config management health: %d active config watchers", configWatcherCount)

	return nil
}
