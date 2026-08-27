package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/go-lynx/lynx-etcd/conf"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPlugEtcd_StartupTasksContext_ReturnsConnectivityError(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{
		Endpoints:   []string{"127.0.0.1:1"},
		Namespace:   "test",
		Timeout:     durationpb.New(200 * time.Millisecond),
		DialTimeout: durationpb.New(100 * time.Millisecond),
		EnableRetry: false,
	}

	if err := p.initComponents(); err != nil {
		t.Fatalf("initComponents: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := p.StartupTasksContext(ctx)
	if err == nil {
		t.Fatal("expected error when etcd endpoint is unreachable, got nil")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("expected connectivity failure to respect caller context, took %v", elapsed)
	}
}
