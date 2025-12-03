# lynx-etcd

Etcd configuration center plugin for using etcd as a configuration center in the Lynx framework.

## Features

- ✅ Configuration center functionality, implements ControlPlane interface
- ✅ Configuration monitoring and automatic updates
- ✅ Multiple configuration source support
- ✅ Local cache support
- ✅ Health check
- ✅ Metrics monitoring
- ✅ Retry mechanism and circuit breaker
- ✅ TLS encryption support
- ✅ Graceful shutdown

## Configuration Example

```yaml
lynx:
  etcd:
    endpoints:
      - "127.0.0.1:2379"
    timeout: 10s
    dial_timeout: 5s
    namespace: "lynx/config"
    enable_tls: false
    enable_cache: true
    enable_metrics: true
    enable_retry: true
    max_retry_times: 3
    retry_interval: 1s
    enable_graceful_shutdown: true
    shutdown_timeout: 10s
    service_config:
      prefix: "lynx/config"
      additional_prefixes:
        - "lynx/config/app"
      priority: 0
      merge_strategy: "override"
```

## Usage Example

The plugin will automatically register with the Lynx framework and be used as a configuration center. The framework will automatically load configurations from etcd.

## Dependencies

- `go.etcd.io/etcd/client/v3` - etcd client library
