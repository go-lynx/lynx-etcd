package etcd

import (
	"strings"
	"testing"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestEtcdConfigWatcherStopCancelsWatchContext(t *testing.T) {
	watcher := NewEtcdConfigWatcher(nil, "/lynx/config")

	if err := watcher.Stop(); err != nil {
		t.Fatalf("Stop() returned error: %v", err)
	}
	if err := watcher.Stop(); err != nil {
		t.Fatalf("second Stop() returned error: %v", err)
	}

	select {
	case <-watcher.watchCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected Stop to cancel the underlying watch context")
	}
}

func TestEtcdConfigWatcherNextReturnsOnStop(t *testing.T) {
	watcher := NewEtcdConfigWatcher(nil, "/lynx/config")
	watcher.watchCh = make(chan clientv3.WatchResponse)

	resultCh := make(chan error, 1)
	go func() {
		_, err := watcher.Next()
		resultCh <- err
	}()

	time.Sleep(20 * time.Millisecond)
	if err := watcher.Stop(); err != nil {
		t.Fatalf("Stop() returned error: %v", err)
	}

	select {
	case err := <-resultCh:
		if err == nil || !strings.Contains(err.Error(), "watcher stopped") {
			t.Fatalf("expected watcher stopped error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected Next() to unblock after Stop()")
	}
}

func TestEtcdConfigWatcherConvertsPutEvents(t *testing.T) {
	watcher := NewEtcdConfigWatcher(nil, "/lynx/config")
	watchCh := make(chan clientv3.WatchResponse, 1)
	watcher.watchCh = watchCh

	watchCh <- clientv3.WatchResponse{
		Events: []*clientv3.Event{
			{
				Type: clientv3.EventTypePut,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("/lynx/config/service/name"),
					Value: []byte("demo"),
				},
			},
		},
	}

	kvs, err := watcher.Next()
	if err != nil {
		t.Fatalf("Next() returned error: %v", err)
	}
	if len(kvs) != 1 {
		t.Fatalf("expected 1 key/value, got %d", len(kvs))
	}
	if kvs[0].Key != "service.name" {
		t.Fatalf("unexpected key: %s", kvs[0].Key)
	}
	if string(kvs[0].Value) != "demo" {
		t.Fatalf("unexpected value: %s", kvs[0].Value)
	}
}
