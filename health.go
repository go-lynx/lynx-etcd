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

// checkEtcdHealth checks the health of the etcd configuration center.
func (p *PlugEtcd) checkEtcdHealth() error {
	ctx, cancel := p.startupContext()
	defer cancel()
	return p.checkEtcdHealthContext(ctx)
}

// checkClientConnection verifies client connection status.
func (p *PlugEtcd) checkClientConnection() error {
	ctx, cancel := p.startupContext()
	defer cancel()
	return p.checkClientConnectionContext(ctx)
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
