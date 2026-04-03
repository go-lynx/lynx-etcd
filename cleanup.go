package etcd

// CleanupTasks implements custom cleanup logic for the Etcd plugin.
// This function gracefully closes connections and releases resources.
func (p *PlugEtcd) CleanupTasks() error {
	ctx, cancel := p.shutdownContext()
	defer cancel()
	return p.cleanupTasksContext(ctx)
}
