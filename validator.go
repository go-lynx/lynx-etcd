package etcd

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-lynx/lynx-etcd/conf"
)

// ValidationError configuration validation error
type ValidationError struct {
	Field   string
	Message string
	Value   interface{}
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error for field '%s': %s (value: %v)", e.Field, e.Message, e.Value)
}

// ValidationResult validation result
type ValidationResult struct {
	IsValid bool
	Errors  []*ValidationError
}

// NewValidationResult creates validation result
func NewValidationResult() *ValidationResult {
	return &ValidationResult{
		IsValid: true,
		Errors:  make([]*ValidationError, 0),
	}
}

// AddError adds error
func (r *ValidationResult) AddError(field, message string, value interface{}) {
	r.IsValid = false
	r.Errors = append(r.Errors, &ValidationError{
		Field:   field,
		Message: message,
		Value:   value,
	})
}

// Error returns error message
func (r *ValidationResult) Error() string {
	if r.IsValid {
		return ""
	}

	var messages []string
	for _, err := range r.Errors {
		messages = append(messages, err.Error())
	}
	return strings.Join(messages, "; ")
}

// Validator configuration validator
type Validator struct {
	config *conf.Etcd
}

// NewValidator creates new validator
func NewValidator(config *conf.Etcd) *Validator {
	return &Validator{
		config: config,
	}
}

// Validate validates configuration
func (v *Validator) Validate() *ValidationResult {
	result := NewValidationResult()

	// Validate basic fields
	v.validateBasicFields(result)

	// Validate time-related configurations
	v.validateTimeConfigs(result)

	// Validate dependencies
	v.validateDependencies(result)

	return result
}

// validateBasicFields validates basic fields
func (v *Validator) validateBasicFields(result *ValidationResult) {
	// Validate endpoints (required)
	if len(v.config.Endpoints) == 0 {
		result.AddError("endpoints", "endpoints cannot be empty", v.config.Endpoints)
	}

	// Validate each endpoint format
	for i, endpoint := range v.config.Endpoints {
		if endpoint == "" {
			result.AddError(fmt.Sprintf("endpoints[%d]", i), "endpoint cannot be empty", endpoint)
		}
	}

	// Validate namespace
	if v.config.Namespace != "" && len(v.config.Namespace) > 512 {
		result.AddError("namespace", "namespace length must not exceed 512 characters", v.config.Namespace)
	}

	// Validate TLS files if TLS is enabled
	if v.config.EnableTls {
		if v.config.CertFile != "" && len(v.config.CertFile) > 512 {
			result.AddError("cert_file", "cert_file path length must not exceed 512 characters", v.config.CertFile)
		}
		if v.config.KeyFile != "" && len(v.config.KeyFile) > 512 {
			result.AddError("key_file", "key_file path length must not exceed 512 characters", v.config.KeyFile)
		}
		if v.config.CaFile != "" && len(v.config.CaFile) > 512 {
			result.AddError("ca_file", "ca_file path length must not exceed 512 characters", v.config.CaFile)
		}
	}
}

// validateTimeConfigs validates time-related configurations
func (v *Validator) validateTimeConfigs(result *ValidationResult) {
	// Validate timeout
	if v.config.Timeout != nil {
		timeout := v.config.Timeout.AsDuration()
		if timeout < 100*time.Millisecond {
			result.AddError("timeout", "timeout should be at least 100ms", timeout)
		}
		if timeout > 60*time.Second {
			result.AddError("timeout", "timeout should not exceed 60s", timeout)
		}
	}

	// Validate dial_timeout
	if v.config.DialTimeout != nil {
		timeout := v.config.DialTimeout.AsDuration()
		if timeout < 100*time.Millisecond {
			result.AddError("dial_timeout", "dial_timeout should be at least 100ms", timeout)
		}
		if timeout > 30*time.Second {
			result.AddError("dial_timeout", "dial_timeout should not exceed 30s", timeout)
		}
	}

	// Validate retry_interval
	if v.config.RetryInterval != nil {
		interval := v.config.RetryInterval.AsDuration()
		if interval < 100*time.Millisecond {
			result.AddError("retry_interval", "retry_interval should be at least 100ms", interval)
		}
		if interval > 10*time.Second {
			result.AddError("retry_interval", "retry_interval should not exceed 10s", interval)
		}
	}

	// Validate shutdown_timeout
	if v.config.ShutdownTimeout != nil {
		timeout := v.config.ShutdownTimeout.AsDuration()
		if timeout < 1*time.Second {
			result.AddError("shutdown_timeout", "shutdown_timeout should be at least 1s", timeout)
		}
		if timeout > 60*time.Second {
			result.AddError("shutdown_timeout", "shutdown_timeout should not exceed 60s", timeout)
		}
	}
}

// validateDependencies validates dependencies
func (v *Validator) validateDependencies(result *ValidationResult) {
	// Validate max_retry_times
	if v.config.MaxRetryTimes < 0 {
		result.AddError("max_retry_times", "max_retry_times cannot be negative", v.config.MaxRetryTimes)
	}
	if v.config.MaxRetryTimes > 10 {
		result.AddError("max_retry_times", "max_retry_times should not exceed 10", v.config.MaxRetryTimes)
	}
}
