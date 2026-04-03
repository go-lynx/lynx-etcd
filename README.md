# lynx-etcd

Etcd configuration center plugin for Lynx. The current runtime loads configuration from `lynx.etcd`, supports optional service registry and discovery, and uses context-aware startup and cleanup paths.

## Current Scope

- Remote configuration loading from a main namespace plus optional additional prefixes
- Optional service registry and service discovery through Kratos registry interfaces
- Optional local cache, retry manager, TLS client configuration, and metrics collector
- Health check plus context-aware startup and cleanup
- Validation for endpoints, timeout windows, retry counts, and TLS path length

The following schema fields are currently accepted but should be treated as compatibility metadata rather than active runtime switches:

- `enable_graceful_shutdown`
- `enable_logging`
- `log_level`
- `service_config.priority`
- `service_config.merge_strategy`

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
    shutdown_timeout: 10s

    enable_register: false
    enable_discovery: false
    registry_namespace: "lynx/services"
    ttl: 30s

    service_config:
      prefix: "lynx/config"
      additional_prefixes:
        - "lynx/config/app"
```

## Effective Defaults

- `namespace`: `lynx/config`
- `timeout`: `10s`
- `dial_timeout`: `5s`
- `registry_namespace`: `lynx/services` when registry or discovery is enabled
- `ttl`: `30s` when service registration is enabled and no custom TTL is provided
- `shutdown_timeout`: fallback `10s` when omitted during cleanup
- `enable_cache`, `enable_metrics`, `enable_retry`, `enable_register`, `enable_discovery`: `false` unless set explicitly
- If `enable_retry` is `true` and `max_retry_times <= 0`, the retry manager falls back to `3`
- If `enable_retry` is `true` and `retry_interval` is omitted, the retry manager falls back to `1s`

## Validation Boundaries

- `endpoints` is required and each endpoint entry must be non-empty
- `namespace`, `cert_file`, `key_file`, `ca_file`: max length `512`
- `timeout`: `100ms` to `60s`
- `dial_timeout`: `100ms` to `30s`
- `retry_interval`: `100ms` to `10s`
- `shutdown_timeout`: `1s` to `60s`
- `max_retry_times`: `0` to `10`

## Service Config Notes

- `service_config.prefix` falls back to top-level `namespace`
- `service_config.additional_prefixes` are loaded in declaration order
- `service_config.priority` and `service_config.merge_strategy` are retained in the schema for future or framework-level merge semantics; the current plugin does not reorder sources or apply a custom merge algorithm by itself

## Service Registry And Discovery

- `NewServiceRegistry()` returns `nil` unless `enable_register` is `true`
- `NewServiceDiscovery()` returns `nil` unless `enable_discovery` is `true`
- Lease keepalive and service watch loops are created only after successful client initialization

## Validation And Examples

- Example file: `conf/example_config.yml`
- Validation baseline and compatibility notes: `VALIDATION.md`
