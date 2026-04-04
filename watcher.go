package etcd

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-kratos/kratos/v2/config"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdConfigWatcher implements config.Watcher for etcd
type EtcdConfigWatcher struct {
	client      *clientv3.Client
	prefix      string
	watchCh     clientv3.WatchChan
	stopCh      chan struct{}
	done        chan struct{}
	watchCtx    context.Context
	cancelWatch context.CancelFunc
	stopOnce    sync.Once
	doneOnce    sync.Once
}

// NewEtcdConfigWatcher creates a new etcd config watcher
func NewEtcdConfigWatcher(client *clientv3.Client, prefix string) *EtcdConfigWatcher {
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	return &EtcdConfigWatcher{
		client:      client,
		prefix:      prefix,
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
		watchCtx:    watchCtx,
		cancelWatch: cancelWatch,
	}
}

// Next returns the next set of configuration changes
func (w *EtcdConfigWatcher) Next() ([]*config.KeyValue, error) {
	if w.watchCh == nil {
		if err := w.watchCtx.Err(); err != nil {
			return nil, fmt.Errorf("watcher stopped")
		}
		// Start watching
		w.watchCh = w.client.Watch(w.watchCtx, w.prefix, clientv3.WithPrefix())
	}

	for {
		select {
		case <-w.stopCh:
			return nil, fmt.Errorf("watcher stopped")
		case resp, ok := <-w.watchCh:
			if !ok {
				if w.watchCtx.Err() != nil {
					return nil, fmt.Errorf("watcher stopped")
				}
				return nil, fmt.Errorf("watch channel closed")
			}
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
	w.stopOnce.Do(func() {
		if w.cancelWatch != nil {
			w.cancelWatch()
		}
		close(w.stopCh)
	})
	w.doneOnce.Do(func() {
		close(w.done)
	})
	return nil
}
