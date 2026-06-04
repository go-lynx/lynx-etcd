package etcd

import (
	"fmt"

	"github.com/go-lynx/lynx/log"
)

// CheckHealth performs a health check.
func (p *PlugEtcd) CheckHealth() error {
	if err := p.checkInitialized(); err != nil {
		return err
	}

	ctx, cancel := p.startupContext()
	defer cancel()
	return p.checkEtcdHealthContext(ctx)
}

// checkConfigManagementHealth checks configuration management functionality.
func (p *PlugEtcd) checkConfigManagementHealth() error {
	// Check status of components related to configuration management.
	// Read configWatchers under mu since cleanup mutates it under the same lock.
	p.mu.RLock()
	watchers := p.configWatchers
	configWatcherCount := len(watchers)
	p.mu.RUnlock()

	if watchers == nil {
		return fmt.Errorf("config watchers not initialized")
	}

	log.Debugf("Config management health: %d active config watchers", configWatcherCount)

	return nil
}
