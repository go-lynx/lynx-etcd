package etcd

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/go-lynx/lynx/log"
)

// CleanupTasks implements custom cleanup logic for the Etcd plugin.
// This function gracefully closes connections and releases resources.
func (p *PlugEtcd) CleanupTasks() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if atomic.LoadInt32(&p.destroyed) == 1 {
		return NewInitError("Etcd plugin already destroyed")
	}

	log.Infof("Cleaning up etcd plugin")

	// Get shutdown timeout
	shutdownTimeout := 10 * time.Second
	if p.conf != nil && p.conf.ShutdownTimeout != nil {
		shutdownTimeout = p.conf.ShutdownTimeout.AsDuration()
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Stop all config watchers
	p.watcherMutex.Lock()
	for namespace, watcher := range p.configWatchers {
		if watcher != nil {
			watcher.Stop()
			log.Debugf("Stopped config watcher for namespace: %s", namespace)
		}
	}
	p.configWatchers = make(map[string]*EtcdConfigWatcher)
	p.watcherMutex.Unlock()

	// Close etcd client
	if p.client != nil {
		if err := p.client.Close(); err != nil {
			log.Errorf("Failed to close etcd client: %v", err)
		} else {
			log.Infof("Etcd client closed successfully")
		}
		p.client = nil
	}

	// Close health check channel
	p.healthCheckCloseOnce.Do(func() {
		close(p.healthCheckCh)
	})

	// Clear cache
	p.cacheMutex.Lock()
	p.configCache = make(map[string]interface{})
	p.cacheMutex.Unlock()

	// Wait for context timeout or completion
	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			log.Warnf("Etcd plugin cleanup timeout after %v", shutdownTimeout)
		}
	case <-time.After(100 * time.Millisecond):
		// Give a short time for cleanup to complete
	}

	p.setDestroyed()
	log.Infof("Etcd plugin cleanup completed")
	return nil
}
