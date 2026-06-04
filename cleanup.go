package etcd

// CleanupTasks gracefully closes the etcd client and stops background goroutines.
func (p *PlugEtcd) CleanupTasks() error {
	ctx, cancel := p.shutdownContext()
	defer cancel()
	return p.cleanupTasksContext(ctx)
}
