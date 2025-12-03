package conf

import (
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// DefaultNamespace is the default namespace prefix for etcd keys
	DefaultNamespace = "lynx/config"

	// DefaultTimeout is the default timeout for etcd operations
	DefaultTimeout = 10 * time.Second

	// DefaultDialTimeout is the default timeout for establishing connections
	DefaultDialTimeout = 5 * time.Second
)

// GetDefaultTimeout returns the default timeout duration
func GetDefaultTimeout() *durationpb.Duration {
	return durationpb.New(DefaultTimeout)
}

// GetDefaultDialTimeout returns the default dial timeout duration
func GetDefaultDialTimeout() *durationpb.Duration {
	return durationpb.New(DefaultDialTimeout)
}
