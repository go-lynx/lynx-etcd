package etcd

import (
	"strings"
	"testing"
	"time"

	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ── watcher reconnect test ────────────────────────────────────────────────────

// TestEtcdConfigWatcherReconnectsOnChannelClose verifies that Next() transparently
// loops when the watch channel is closed by the server (e.g., after an etcd
// compaction or a transient network partition) and eventually returns data once
// the channel is replenished — instead of surfacing an error to the caller.
//
// Implementation note: EtcdConfigWatcher.openWatchChan() is a no-op when
// client is nil, so the test drives reconnection by swapping watchCh from a
// separate goroutine while Next() is spinning on the closed channel.
func TestEtcdConfigWatcherReconnectsOnChannelClose(t *testing.T) {
	watcher := NewEtcdConfigWatcher(nil, "/lynx/config")

	// First channel: closed immediately to simulate a server-side close.
	firstCh := make(chan clientv3.WatchResponse)
	close(firstCh)
	watcher.watchCh = firstCh

	// Second channel: delivers a real PUT event so Next() can return.
	secondCh := make(chan clientv3.WatchResponse, 1)
	secondCh <- clientv3.WatchResponse{
		Events: []*clientv3.Event{
			{
				Type: clientv3.EventTypePut,
				Kv: &mvccpb.KeyValue{
					Key:   []byte("/lynx/config/reconnect"),
					Value: []byte("ok"),
				},
			},
		},
	}

	// After a short delay swap the channel so Next() picks up the real event.
	// Next() loops on the closed channel (openWatchChan is a no-op for nil client)
	// until watchCh is replaced here.
	go func() {
		time.Sleep(20 * time.Millisecond)
		watcher.watchCh = secondCh
	}()

	resultCh := make(chan error, 1)
	var gotKey string
	go func() {
		kvs, err := watcher.Next()
		if err == nil && len(kvs) == 1 {
			gotKey = kvs[0].Key
		}
		resultCh <- err
	}()

	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("Next() should not error after reconnect, got: %v", err)
		}
		if gotKey != "reconnect" {
			t.Fatalf("unexpected key after reconnect: %s", gotKey)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Next() did not return after channel swap")
	}
}

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
