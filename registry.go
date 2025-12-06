package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-lynx/lynx/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Note: DefaultRegistryNamespace and DefaultTTL are defined in conf package

// ServiceLease represents a service instance with its lease information
type ServiceLease struct {
	instance    *registry.ServiceInstance
	leaseID     clientv3.LeaseID
	keepAliveCh <-chan *clientv3.LeaseKeepAliveResponse
}

// EtcdRegistrar implements registry.Registrar interface for etcd
type EtcdRegistrar struct {
	client      *clientv3.Client
	namespace   string
	ttl         time.Duration
	mu          sync.RWMutex
	registered  map[string]*ServiceLease // key: instanceID
	leaseCancel context.CancelFunc
	leaseCtx    context.Context
}

// NewEtcdRegistrar creates a new etcd registrar
func NewEtcdRegistrar(client *clientv3.Client, namespace string, ttl time.Duration) *EtcdRegistrar {
	ctx, cancel := context.WithCancel(context.Background())
	return &EtcdRegistrar{
		client:      client,
		namespace:   namespace,
		ttl:         ttl,
		registered:  make(map[string]*registry.ServiceInstance),
		leaseCtx:    ctx,
		leaseCancel: cancel,
	}
}

// Register registers a service instance to etcd
func (r *EtcdRegistrar) Register(ctx context.Context, service *registry.ServiceInstance) error {
	if r.client == nil {
		return fmt.Errorf("etcd client is nil")
	}

	if service == nil {
		return fmt.Errorf("service instance is nil")
	}

	// Parse endpoint to get host and port
	host, port, err := parseEndpoint(service.Endpoints)
	if err != nil {
		return fmt.Errorf("failed to parse endpoint: %w", err)
	}

	// Build service key: namespace/serviceName/instanceID
	instanceID := service.ID
	if instanceID == "" {
		instanceID = fmt.Sprintf("%s-%s-%d", service.Name, host, port)
	}
	serviceKey := buildServiceKey(r.namespace, service.Name, instanceID)

	// Build service value (JSON format)
	serviceValue, err := buildServiceValue(service, host, port)
	if err != nil {
		return fmt.Errorf("failed to build service value: %w", err)
	}

	// Grant lease
	lease, err := r.client.Grant(ctx, int64(r.ttl.Seconds()))
	if err != nil {
		return WrapClientError(err, ErrCodeRegisterFailed, "failed to grant lease")
	}

	// Put service with lease
	_, err = r.client.Put(ctx, serviceKey, serviceValue, clientv3.WithLease(lease.ID))
	if err != nil {
		return WrapClientError(err, ErrCodeRegisterFailed, "failed to register service")
	}

	// Keep lease alive
	keepAliveCh, err := r.client.KeepAlive(r.leaseCtx, lease.ID)
	if err != nil {
		// If keep-alive fails, revoke the lease
		_, _ = r.client.Revoke(ctx, lease.ID)
		return WrapClientError(err, ErrCodeRegisterFailed, "failed to keep lease alive")
	}

	// Check if instance already registered and revoke old lease
	r.mu.Lock()
	if oldLease, exists := r.registered[instanceID]; exists {
		// Revoke old lease
		_, _ = r.client.Revoke(ctx, oldLease.leaseID)
		log.Debugf("Revoked old lease for instance %s", instanceID)
	}

	// Store registration info
	r.registered[instanceID] = &ServiceLease{
		instance:    service,
		leaseID:     lease.ID,
		keepAliveCh: keepAliveCh,
	}
	r.mu.Unlock()

	// Start goroutine to handle keep-alive responses
	go r.handleKeepAlive(instanceID, keepAliveCh)

	log.Infof("Service instance registered to etcd - Service: %s, InstanceID: %s, Host: %s, Port: %d",
		service.Name, instanceID, host, port)

	return nil
}

// Deregister deregisters a service instance from etcd
func (r *EtcdRegistrar) Deregister(ctx context.Context, service *registry.ServiceInstance) error {
	if r.client == nil {
		return fmt.Errorf("etcd client is nil")
	}

	if service == nil {
		return fmt.Errorf("service instance is nil")
	}

	// Parse endpoint to get host and port
	host, port, err := parseEndpoint(service.Endpoints)
	if err != nil {
		return fmt.Errorf("failed to parse endpoint: %w", err)
	}

	// Get instance ID
	instanceID := service.ID
	if instanceID == "" {
		instanceID = fmt.Sprintf("%s-%s-%d", service.Name, host, port)
	}

	// Build service key
	serviceKey := buildServiceKey(r.namespace, service.Name, instanceID)

	// Get and revoke lease if exists
	r.mu.Lock()
	serviceLease, exists := r.registered[instanceID]
	if exists {
		delete(r.registered, instanceID)
	}
	r.mu.Unlock()

	if exists && serviceLease != nil {
		_, err = r.client.Revoke(ctx, serviceLease.leaseID)
		if err != nil {
			log.Warnf("Failed to revoke lease for instance %s: %v", instanceID, err)
		}
	}

	// Delete service key
	_, err = r.client.Delete(ctx, serviceKey)
	if err != nil {
		return WrapClientError(err, ErrCodeDeregisterFailed, "failed to deregister service")
	}

	log.Infof("Service instance deregistered from etcd - Service: %s, InstanceID: %s, Host: %s, Port: %d",
		service.Name, instanceID, host, port)

	return nil
}

// handleKeepAlive handles keep-alive responses
func (r *EtcdRegistrar) handleKeepAlive(instanceID string, ch <-chan *clientv3.LeaseKeepAliveResponse) {
	for {
		select {
		case resp, ok := <-ch:
			if !ok {
				log.Warnf("Keep-alive channel closed for instance %s", instanceID)
				// Remove from registered map when keep-alive fails
				r.mu.Lock()
				delete(r.registered, instanceID)
				r.mu.Unlock()
				return
			}
			if resp != nil {
				log.Debugf("Lease keep-alive response for instance %s: ID=%d, TTL=%d", instanceID, resp.ID, resp.TTL)
			}
		case <-r.leaseCtx.Done():
			return
		}
	}
}

// Close closes the registrar and revokes all leases
func (r *EtcdRegistrar) Close() error {
	r.leaseCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r.mu.Lock()
	registered := make(map[string]*ServiceLease)
	for k, v := range r.registered {
		registered[k] = v
	}
	r.registered = make(map[string]*ServiceLease)
	r.mu.Unlock()

	// Revoke all leases
	for instanceID, serviceLease := range registered {
		if serviceLease != nil {
			_, err := r.client.Revoke(ctx, serviceLease.leaseID)
			if err != nil {
				log.Warnf("Failed to revoke lease for instance %s on close: %v", instanceID, err)
			}
		}
	}

	return nil
}

// EtcdDiscovery implements registry.Discovery interface for etcd
type EtcdDiscovery struct {
	client    *clientv3.Client
	namespace string
	mu        sync.RWMutex
	watchers  map[string]*EtcdWatcher
}

// NewEtcdDiscovery creates a new etcd discovery client
func NewEtcdDiscovery(client *clientv3.Client, namespace string) *EtcdDiscovery {
	return &EtcdDiscovery{
		client:    client,
		namespace: namespace,
		watchers:  make(map[string]*EtcdWatcher),
	}
}

// GetService gets service instances from etcd
func (d *EtcdDiscovery) GetService(ctx context.Context, serviceName string) ([]*registry.ServiceInstance, error) {
	if d.client == nil {
		return nil, fmt.Errorf("etcd client is nil")
	}

	// Build service prefix: namespace/serviceName/
	servicePrefix := buildServicePrefix(d.namespace, serviceName)

	// Get all instances with prefix
	resp, err := d.client.Get(ctx, servicePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, WrapClientError(err, ErrCodeDiscoveryFailed, "failed to get service instances")
	}

	var instances []*registry.ServiceInstance
	for _, kv := range resp.Kvs {
		instance, err := parseServiceValue(kv.Value, serviceName)
		if err != nil {
			log.Warnf("Failed to parse service value for key %s: %v", string(kv.Key), err)
			continue
		}
		instances = append(instances, instance)
	}

	return instances, nil
}

// Watch watches service changes
func (d *EtcdDiscovery) Watch(ctx context.Context, serviceName string) (registry.Watcher, error) {
	if d.client == nil {
		return nil, fmt.Errorf("etcd client is nil")
	}

	// Check if watcher already exists
	d.mu.RLock()
	if watcher, exists := d.watchers[serviceName]; exists {
		d.mu.RUnlock()
		return watcher, nil
	}
	d.mu.RUnlock()

	// Build service prefix
	servicePrefix := buildServicePrefix(d.namespace, serviceName)

	// Create new watcher
	watcher := NewEtcdWatcher(d.client, servicePrefix, serviceName)

	// Store watcher
	d.mu.Lock()
	d.watchers[serviceName] = watcher
	d.mu.Unlock()

	// Start watching
	if err := watcher.Start(ctx); err != nil {
		d.mu.Lock()
		delete(d.watchers, serviceName)
		d.mu.Unlock()
		return nil, fmt.Errorf("failed to start watcher: %w", err)
	}

	return watcher, nil
}

// EtcdWatcher implements registry.Watcher interface
type EtcdWatcher struct {
	client        *clientv3.Client
	servicePrefix string
	serviceName   string
	watchCh       clientv3.WatchChan
	stopCh        chan struct{}
	eventCh       chan []*registry.ServiceInstance
	mu            sync.RWMutex
	running       bool
	stopOnce      sync.Once
	closed        int32
}

// NewEtcdWatcher creates a new etcd watcher
func NewEtcdWatcher(client *clientv3.Client, servicePrefix, serviceName string) *EtcdWatcher {
	return &EtcdWatcher{
		client:        client,
		servicePrefix: servicePrefix,
		serviceName:   serviceName,
		stopCh:        make(chan struct{}),
		eventCh:       make(chan []*registry.ServiceInstance, 10),
	}
}

// Start starts watching service changes
func (w *EtcdWatcher) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("watcher is already running")
	}
	w.running = true
	w.mu.Unlock()

	// Start watching
	w.watchCh = w.client.Watch(ctx, w.servicePrefix, clientv3.WithPrefix())

	// Start background goroutine to handle watch events
	go w.handleWatchEvents(ctx)

	// Start background goroutine to handle context cancellation
	go func() {
		select {
		case <-ctx.Done():
			w.Stop()
		case <-w.stopCh:
		}
	}()

	return nil
}

// handleWatchEvents handles watch events
func (w *EtcdWatcher) handleWatchEvents(ctx context.Context) {
	for {
		select {
		case resp, ok := <-w.watchCh:
			if !ok {
				log.Warnf("Watch channel closed for service %s", w.serviceName)
				return
			}

			if resp.Err() != nil {
				log.Errorf("Watch error for service %s: %v", w.serviceName, resp.Err())
				continue
			}

			// Convert events to service instances
			var instances []*registry.ServiceInstance
			for _, event := range resp.Events {
				switch event.Type {
				case clientv3.EventTypePut:
					// Service instance added or updated
					instance, err := parseServiceValue(event.Kv.Value, w.serviceName)
					if err != nil {
						log.Warnf("Failed to parse service value: %v", err)
						continue
					}
					instances = append(instances, instance)
				case clientv3.EventTypeDelete:
					// Service instance deleted
					// Extract instance ID from key
					key := string(event.Kv.Key)
					parts := strings.Split(key, "/")
					if len(parts) > 0 {
						instanceID := parts[len(parts)-1]
						instance := &registry.ServiceInstance{
							ID:   instanceID,
							Name: w.serviceName,
						}
						instances = append(instances, instance)
					}
				}
			}

			if len(instances) > 0 {
				// Send event (non-blocking)
				select {
				case w.eventCh <- instances:
				case <-w.stopCh:
					return
				default:
					log.Warnf("Event channel full, dropping event for service %s", w.serviceName)
				}
			}

		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Next returns the next service change event
func (w *EtcdWatcher) Next() ([]*registry.ServiceInstance, error) {
	select {
	case instances := <-w.eventCh:
		return instances, nil
	case <-w.stopCh:
		return nil, fmt.Errorf("watcher stopped")
	}
}

// Stop stops the watcher
func (w *EtcdWatcher) Stop() error {
	var wasRunning bool
	w.mu.Lock()
	wasRunning = w.running
	w.running = false
	w.mu.Unlock()

	if !wasRunning {
		return nil
	}

	// Use sync.Once to ensure channels are closed only once
	w.stopOnce.Do(func() {
		// Mark as closed atomically
		atomic.StoreInt32(&w.closed, 1)

		// Close stop channel
		close(w.stopCh)

		// Close event channel
		close(w.eventCh)
	})

	return nil
}

// Helper functions

// parseEndpoint parses endpoint string to extract host and port
func parseEndpoint(endpoints []string) (string, int, error) {
	if len(endpoints) == 0 {
		return "", 0, fmt.Errorf("no endpoints provided")
	}

	endpoint := endpoints[0]

	// Remove protocol prefix
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "grpc://")
	endpoint = strings.TrimPrefix(endpoint, "grpcs://")

	// Parse host and port
	parts := strings.Split(endpoint, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid endpoint format: %s", endpoints[0])
	}

	host := parts[0]
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid port in endpoint: %w", err)
	}

	return host, port, nil
}

// buildServiceKey builds service key: namespace/serviceName/instanceID
// Note: etcd uses forward slashes, so we use strings.Join instead of filepath.Join
func buildServiceKey(namespace, serviceName, instanceID string) string {
	parts := []string{namespace, serviceName, instanceID}
	return strings.Join(parts, "/")
}

// buildServicePrefix builds service prefix: namespace/serviceName/
// Note: etcd uses forward slashes, so we use strings.Join instead of filepath.Join
func buildServicePrefix(namespace, serviceName string) string {
	return strings.Join([]string{namespace, serviceName}, "/") + "/"
}

// ServiceValue represents service instance data stored in etcd
type ServiceValue struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Version   string            `json:"version"`
	Endpoints []string          `json:"endpoints"`
	Metadata  map[string]string `json:"metadata"`
}

// buildServiceValue builds service value (JSON format)
func buildServiceValue(service *registry.ServiceInstance, host string, port int) (string, error) {
	sv := ServiceValue{
		ID:        service.ID,
		Name:      service.Name,
		Version:   service.Version,
		Endpoints: service.Endpoints,
		Metadata:  service.Metadata,
	}

	// If endpoints are empty, build from host and port
	if len(sv.Endpoints) == 0 {
		sv.Endpoints = []string{fmt.Sprintf("%s:%d", host, port)}
	}

	data, err := json.Marshal(sv)
	if err != nil {
		return "", fmt.Errorf("failed to marshal service value: %w", err)
	}

	return string(data), nil
}

// parseServiceValue parses service value from JSON
func parseServiceValue(data []byte, serviceName string) (*registry.ServiceInstance, error) {
	var sv ServiceValue
	if err := json.Unmarshal(data, &sv); err != nil {
		return nil, fmt.Errorf("failed to unmarshal service value: %w", err)
	}

	// Use provided service name if available
	if sv.Name == "" {
		sv.Name = serviceName
	}

	return &registry.ServiceInstance{
		ID:        sv.ID,
		Name:      sv.Name,
		Version:   sv.Version,
		Endpoints: sv.Endpoints,
		Metadata:  sv.Metadata,
	}, nil
}
