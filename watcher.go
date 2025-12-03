package etcd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-lynx/lynx/app/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdConfigWatcher implements config.Watcher for etcd
type EtcdConfigWatcher struct {
	client  *clientv3.Client
	prefix  string
	watchCh clientv3.WatchChan
	stopCh  chan struct{}
	done    chan struct{}
}

// NewEtcdConfigWatcher creates a new etcd config watcher
func NewEtcdConfigWatcher(client *clientv3.Client, prefix string) *EtcdConfigWatcher {
	return &EtcdConfigWatcher{
		client: client,
		prefix: prefix,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// Next returns the next set of configuration changes
func (w *EtcdConfigWatcher) Next() ([]*config.KeyValue, error) {
	if w.watchCh == nil {
		// Start watching
		w.watchCh = w.client.Watch(context.Background(), w.prefix, clientv3.WithPrefix())
	}

	for {
		select {
		case <-w.stopCh:
			return nil, fmt.Errorf("watcher stopped")
		case resp := <-w.watchCh:
			if resp.Err() != nil {
				return nil, fmt.Errorf("watch error: %w", resp.Err())
			}

			var kvs []*config.KeyValue
			for _, event := range resp.Events {
				// Extract key name (remove prefix)
				key := strings.TrimPrefix(string(event.Kv.Key), w.prefix)
				key = strings.TrimPrefix(key, "/")

				// Convert etcd key path to config key (replace / with .)
				key = strings.ReplaceAll(key, "/", ".")

				switch event.Type {
				case clientv3.EventTypePut:
					// Key was created or updated
					kvs = append(kvs, &config.KeyValue{
						Key:   key,
						Value: event.Kv.Value,
					})
				case clientv3.EventTypeDelete:
					// Key was deleted
					kvs = append(kvs, &config.KeyValue{
						Key:   key,
						Value: nil,
					})
				}
			}

			if len(kvs) > 0 {
				return kvs, nil
			}
		}
	}
}

// Stop stops the watcher
func (w *EtcdConfigWatcher) Stop() error {
	close(w.stopCh)

	// Wait for done signal with timeout
	select {
	case <-w.done:
	case <-time.After(5 * time.Second):
		log.Warnf("Etcd watcher stop timeout")
	}

	return nil
}
