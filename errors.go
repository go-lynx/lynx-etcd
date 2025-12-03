package etcd

import (
	"fmt"
	"strings"
)

// ErrorCode error code type
type ErrorCode string

// Error code constants
const (
	// ErrCodeConfigInvalid Configuration related errors
	ErrCodeConfigInvalid    ErrorCode = "CONFIG_INVALID"
	ErrCodeConfigMissing    ErrorCode = "CONFIG_MISSING"
	ErrCodeConfigValidation ErrorCode = "CONFIG_VALIDATION"

	// ErrCodeInitFailed Initialization related errors
	ErrCodeInitFailed         ErrorCode = "INIT_FAILED"
	ErrCodeAlreadyInitialized ErrorCode = "ALREADY_INITIALIZED"
	ErrCodeNotInitialized     ErrorCode = "NOT_INITIALIZED"

	// ErrCodeClientFailed Client related errors
	ErrCodeClientFailed     ErrorCode = "CLIENT_FAILED"
	ErrCodeClientInitFailed ErrorCode = "CLIENT_INIT_FAILED"
	ErrCodeClientDestroyed  ErrorCode = "CLIENT_DESTROYED"

	// ErrCodeConfigNotFound Configuration management related errors
	ErrCodeConfigNotFound    ErrorCode = "CONFIG_NOT_FOUND"
	ErrCodeConfigGetFailed   ErrorCode = "CONFIG_GET_FAILED"
	ErrCodeConfigWatchFailed ErrorCode = "CONFIG_WATCH_FAILED"

	// ErrCodeHealthCheckFailed Health check related errors
	ErrCodeHealthCheckFailed  ErrorCode = "HEALTH_CHECK_FAILED"
	ErrCodeHealthCheckTimeout ErrorCode = "HEALTH_CHECK_TIMEOUT"

	// ErrCodeNetworkError Network related errors
	ErrCodeNetworkError     ErrorCode = "NETWORK_ERROR"
	ErrCodeTimeout          ErrorCode = "TIMEOUT"
	ErrCodeConnectionFailed ErrorCode = "CONNECTION_FAILED"

	// ErrCodeRetryExhausted Retry related errors
	ErrCodeRetryExhausted     ErrorCode = "RETRY_EXHAUSTED"
	ErrCodeCircuitBreakerOpen ErrorCode = "CIRCUIT_BREAKER_OPEN"

	// ErrCodeMetricsFailed Metrics related errors
	ErrCodeMetricsFailed ErrorCode = "METRICS_FAILED"

	// ErrCodeShutdownFailed Graceful shutdown related errors
	ErrCodeShutdownFailed  ErrorCode = "SHUTDOWN_FAILED"
	ErrCodeShutdownTimeout ErrorCode = "SHUTDOWN_TIMEOUT"
)

// EtcdError Etcd plugin error
type EtcdError struct {
	Code    ErrorCode
	Message string
	Cause   error
	Context map[string]interface{}
}

// NewEtcdError creates new Etcd error
func NewEtcdError(code ErrorCode, message string) *EtcdError {
	return &EtcdError{
		Code:    code,
		Message: message,
		Context: make(map[string]interface{}),
	}
}

// WithCause sets error cause
func (e *EtcdError) WithCause(cause error) *EtcdError {
	e.Cause = cause
	return e
}

// WithContext adds context information
func (e *EtcdError) WithContext(key string, value interface{}) *EtcdError {
	e.Context[key] = value
	return e
}

// Error implements error interface
func (e *EtcdError) Error() string {
	var parts []string

	// Add error code
	parts = append(parts, fmt.Sprintf("[%s]", e.Code))

	// Add error message
	parts = append(parts, e.Message)

	// Add cause
	if e.Cause != nil {
		parts = append(parts, fmt.Sprintf("caused by: %v", e.Cause))
	}

	// Add context
	if len(e.Context) > 0 {
		var contextParts []string
		for k, v := range e.Context {
			contextParts = append(contextParts, fmt.Sprintf("%s=%v", k, v))
		}
		parts = append(parts, fmt.Sprintf("context: {%s}", strings.Join(contextParts, ", ")))
	}

	return strings.Join(parts, " ")
}

// Unwrap returns error cause
func (e *EtcdError) Unwrap() error {
	return e.Cause
}

// Is checks error type
func (e *EtcdError) Is(target error) bool {
	if targetError, ok := target.(*EtcdError); ok {
		return e.Code == targetError.Code
	}
	return false
}

// Convenient error creation functions

// NewConfigError creates configuration error
func NewConfigError(message string) *EtcdError {
	return NewEtcdError(ErrCodeConfigInvalid, message)
}

// NewInitError creates initialization error
func NewInitError(message string) *EtcdError {
	return NewEtcdError(ErrCodeInitFailed, message)
}

// NewClientError creates client error
func NewClientError(code ErrorCode, message string) *EtcdError {
	return NewEtcdError(code, message)
}

// NewNetworkError creates network error
func NewNetworkError(message string) *EtcdError {
	return NewEtcdError(ErrCodeNetworkError, message)
}

// NewTimeoutError creates timeout error
func NewTimeoutError(operation string) *EtcdError {
	return NewEtcdError(ErrCodeTimeout, fmt.Sprintf("operation '%s' timed out", operation))
}

// NewRetryError creates retry error
func NewRetryError(message string) *EtcdError {
	return NewEtcdError(ErrCodeRetryExhausted, message)
}

// NewHealthCheckError creates health check error
func NewHealthCheckError(message string) *EtcdError {
	return NewEtcdError(ErrCodeHealthCheckFailed, message)
}

// Error wrapping functions

// WrapError wraps error
func WrapError(err error, code ErrorCode, message string) *EtcdError {
	return NewEtcdError(code, message).WithCause(err)
}

// WrapConfigError wraps configuration error
func WrapConfigError(err error, message string) *EtcdError {
	return WrapError(err, ErrCodeConfigInvalid, message)
}

// WrapInitError wraps initialization error
func WrapInitError(err error, message string) *EtcdError {
	return WrapError(err, ErrCodeInitFailed, message)
}

// WrapClientError wraps client error
func WrapClientError(err error, code ErrorCode, message string) *EtcdError {
	return WrapError(err, code, message)
}

// WrapNetworkError wraps network error
func WrapNetworkError(err error, message string) *EtcdError {
	return WrapError(err, ErrCodeNetworkError, message)
}
