package etcd

import (
	"testing"
	"time"

	"github.com/go-lynx/lynx-etcd/conf"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestStartupTasks_ReturnsErrorOnUnreachableEndpoints verifies that StartupTasks
// propagates connection failure as an error rather than only logging it,
// so the framework rollback mechanism can be triggered.
func TestStartupTasks_ReturnsErrorOnUnreachableEndpoints(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{
		Endpoints:   []string{"127.0.0.1:12399"}, // port that has no etcd
		Namespace:   "test",
		DialTimeout: durationpb.New(500 * time.Millisecond),
	}
	// disable retry so the test is fast
	p.conf.EnableRetry = false

	if err := p.initComponents(); err != nil {
		t.Fatalf("initComponents: %v", err)
	}

	err := p.StartupTasks()
	if err == nil {
		t.Fatal("expected error when etcd endpoint is unreachable, got nil")
	}
}

// TestCleanupTasks_BeforeStartup verifies CleanupTasks is safe to call even
// when the client was never started (e.g., after a failed StartupTasks rollback).
func TestCleanupTasks_BeforeStartup(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{
		Endpoints: []string{"127.0.0.1:2379"},
		Namespace: "test",
	}

	// client is nil — CleanupTasks must not panic
	if err := p.CleanupTasks(); err != nil {
		// First call: not yet destroyed, so it sets destroyed flag. Acceptable.
		_ = err
	}
}

// TestCheckInitialized_NotInitialized verifies that operations requiring
// initialisation return a meaningful error before StartupTasks is called.
func TestCheckInitialized_NotInitialized(t *testing.T) {
	p := NewEtcdConfigCenter()
	if err := p.checkInitialized(); err == nil {
		t.Fatal("expected error when plugin is not initialized, got nil")
	}
}
