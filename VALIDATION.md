# Validation

## Automated Baseline

Run from the module root:

```bash
GOWORK=off go test ./...
```

## Enforced Validation

The current validator enforces these boundaries before startup:

- `endpoints` must be present and each item must be non-empty
- `namespace`, `cert_file`, `key_file`, `ca_file` must not exceed `512` characters
- `timeout` must stay within `100ms` to `60s`
- `dial_timeout` must stay within `100ms` to `30s`
- `retry_interval` must stay within `100ms` to `10s`
- `shutdown_timeout` must stay within `1s` to `60s`
- `max_retry_times` must stay within `0` to `10`

## Effective Runtime Defaults

- `namespace`: `lynx/config`
- `timeout`: `10s`
- `dial_timeout`: `5s`
- `registry_namespace`: `lynx/services`
- `ttl`: `30s`
- `shutdown_timeout`: `10s` fallback if omitted
- All feature booleans default to `false` unless configured explicitly
- Retry manager fallback defaults are applied only when `enable_retry` is `true`

## Compatibility And Reserved Fields

These fields are still part of the schema but are not runtime feature switches in the current module implementation:

- `enable_graceful_shutdown`
- `enable_logging`
- `log_level`
- `service_config.priority`
- `service_config.merge_strategy`

`service_config.additional_prefixes` is effective, but the plugin currently loads additional sources in declaration order and does not implement priority-based reordering or plugin-local merge strategies.

## Recommended Manual Checks

- Verify `GetConfigValue()` and `GetConfigSources()` against a reachable etcd cluster
- If `enable_register` or `enable_discovery` is enabled, verify registrar lease renewal and discovery watch behavior
- If TLS is enabled, verify certificate paths and actual connectivity with the target cluster
