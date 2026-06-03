package etcd

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/go-kratos/kratos/v2/config"
	"github.com/go-lynx/lynx/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdConfigWatcher implements config.Watcher for etcd.
// It reconnects the underlying watch channel automatically when etcd closes it
// (e.g., after a compaction or a transient network partition), so callers
// always receive a clean error-or-data stream rather than a one-shot channel.
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

// NewEtcdConfigWatcher creates a new etcd config watcher for the given prefix.
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

// openWatchChan establishes (or re-establishes) the etcd watch channel.
// It is a no-op when the client is nil (e.g., in unit tests that inject
// channels directly via the watchCh field).
func (w *EtcdConfigWatcher) openWatchChan() {
	if w.client == nil {
		return
	}
	w.watchCh = w.client.Watch(w.watchCtx, w.prefix, clientv3.WithPrefix())
}

// Next returns the next set of configuration changes.
// When the underlying etcd watch channel is closed without the watcher being
// stopped (e.g., server-side compaction or transient partition), Next
// transparently reopens the channel and keeps waiting rather than returning
// an error to the caller.
func (w *EtcdConfigWatcher) Next() ([]*config.KeyValue, error) {
	// Lazily open the channel on first call.
	if w.watchCh == nil {
		if err := w.watchCtx.Err(); err != nil {
			return nil, fmt.Errorf("watcher stopped")
		}
		w.openWatchChan()
	}

	for {
		select {
		case <-w.stopCh:
			return nil, fmt.Errorf("watcher stopped")
		case resp, ok := <-w.watchCh:
			if !ok {
				// Channel was closed by the server or by a network interruption.
				if w.watchCtx.Err() != nil {
					// The watcher was deliberately stopped — surface the error.
					return nil, fmt.Errorf("watcher stopped")
				}
				// Transient close: reopen the channel and keep waiting.
				log.Warnf("etcd watch channel closed unexpectedly for prefix %s, reconnecting", w.prefix)
				w.openWatchChan()
				continue
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
					// Key was created or updated.
					kvs = append(kvs, &config.KeyValue{
						Key:   key,
						Value: event.Kv.Value,
					})
				case clientv3.EventTypeDelete:
					// Key was deleted.
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

// Stop stops the watcher and releases all associated resources.
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
