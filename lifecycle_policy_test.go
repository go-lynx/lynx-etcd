package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/go-lynx/lynx-etcd/conf"
	"github.com/go-lynx/lynx/pkg/security"
	"github.com/go-lynx/lynx/plugins"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

// The production lifecycle policy (lynx/internal/app/lifecycle_policy.go) rejects
// any plugin for which plugins.HasTrueContextLifecycle is false when
// security.IsProduction() is true. These tests pin the plugin to that contract.
func TestPlugEtcd_HasTrueContextLifecycle(t *testing.T) {
	p := NewEtcdConfigCenter()

	caps := plugins.DescribePluginCapabilities(p)
	assert.True(t, caps.HasLifecycleWithCtx, "plugin must expose StartContext/StopContext/InitializeContext")
	assert.True(t, caps.HasContextSteps, "plugin must implement a context-aware step hook")
	assert.True(t, caps.IsTrulyContextAware)
	assert.True(t, plugins.HasTrueContextLifecycle(p))

	_, ok := plugins.GetTrueContextLifecycle(p)
	assert.True(t, ok)

	var _ plugins.ContextStartupTasker = p
	var _ plugins.ContextCleanupTasker = p
}

func TestPlugEtcd_ProductionLifecyclePolicyAccepts(t *testing.T) {
	t.Setenv("LYNX_ENV", "production")
	require.True(t, security.IsProduction())

	p := NewEtcdConfigCenter()
	assert.True(t, plugins.HasTrueContextLifecycle(p),
		"plugin %s would be rejected by the production lifecycle policy", p.Name())
}

func TestPlugEtcd_StartupTasksContext_ObservesCancellation(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{
		Endpoints:   []string{"127.0.0.1:1"},
		Namespace:   "test",
		DialTimeout: durationpb.New(5 * time.Second),
	}
	require.NoError(t, p.initComponents())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := p.StartupTasksContext(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), time.Second, "cancelled startup must return promptly without dialing")
	assert.Nil(t, p.client)
	assert.False(t, p.IsInitialized())
}

func TestPlugEtcd_CleanupTasksContext_ReportsCancellation(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{Endpoints: []string{"127.0.0.1:2379"}, Namespace: "test"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.CleanupTasksContext(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.True(t, p.IsDestroyed(), "cleanup still releases local state")
}
