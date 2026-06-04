package etcd

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-lynx/lynx-etcd/conf"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ---------------------------------------------------------------------------
// Validator tests
// ---------------------------------------------------------------------------

func TestValidationError_Error(t *testing.T) {
	err := &ValidationError{Field: "endpoints", Message: "cannot be empty", Value: nil}
	s := err.Error()
	if s == "" {
		t.Error("expected non-empty error string")
	}
}

func TestValidationResult_NewAndAddError(t *testing.T) {
	r := NewValidationResult()
	if !r.IsValid {
		t.Error("expected new result to be valid")
	}
	r.AddError("field1", "some problem", "bad-value")
	if r.IsValid {
		t.Error("expected IsValid=false after AddError")
	}
	if len(r.Errors) != 1 {
		t.Errorf("expected 1 error, got %d", len(r.Errors))
	}
	errStr := r.Error()
	if errStr == "" {
		t.Error("expected non-empty error string")
	}
}

func TestValidationResult_Error_WhenValid(t *testing.T) {
	r := NewValidationResult()
	if r.Error() != "" {
		t.Errorf("expected empty string for valid result, got %q", r.Error())
	}
}

func TestValidator_ValidConfig(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints: []string{"localhost:2379"},
		Namespace: "test",
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if !result.IsValid {
		t.Errorf("expected valid config, got errors: %v", result.Error())
	}
}

func TestValidator_EmptyEndpoints(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints: []string{},
		Namespace: "test",
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for empty endpoints")
	}
}

func TestValidator_EmptyEndpointInList(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints: []string{""},
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for empty string endpoint")
	}
}

func TestValidator_LongNamespace(t *testing.T) {
	ns := ""
	for i := 0; i < 513; i++ {
		ns += "x"
	}
	cfg := &conf.Etcd{
		Endpoints: []string{"localhost:2379"},
		Namespace: ns,
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for long namespace")
	}
}

func TestValidator_TooSmallTimeout(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints: []string{"localhost:2379"},
		Timeout:   durationpb.New(50 * time.Millisecond), // < 100ms
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for small timeout")
	}
}

func TestValidator_TooLargeTimeout(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints: []string{"localhost:2379"},
		Timeout:   durationpb.New(61 * time.Second), // > 60s
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for large timeout")
	}
}

func TestValidator_TooSmallDialTimeout(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: durationpb.New(50 * time.Millisecond),
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for small dial timeout")
	}
}

func TestValidator_TooLargeDialTimeout(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: durationpb.New(31 * time.Second),
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for large dial timeout")
	}
}

func TestValidator_RetryInterval_TooSmall(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints:     []string{"localhost:2379"},
		RetryInterval: durationpb.New(50 * time.Millisecond), // too small
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for small retry interval")
	}
}

func TestValidator_TooLargeRetryInterval(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints:     []string{"localhost:2379"},
		RetryInterval: durationpb.New(11 * time.Second),
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for large retry interval")
	}
}

func TestValidator_ShutdownTimeout_TooSmall(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints:       []string{"localhost:2379"},
		ShutdownTimeout: durationpb.New(500 * time.Millisecond), // < 1s
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for small shutdown timeout")
	}
}

func TestValidator_TooLargeShutdownTimeout(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints:       []string{"localhost:2379"},
		ShutdownTimeout: durationpb.New(61 * time.Second),
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for large shutdown timeout")
	}
}

func TestValidator_NegativeRetryTimes(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints:     []string{"localhost:2379"},
		MaxRetryTimes: -1,
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for negative retry times")
	}
}

func TestValidator_TooManyRetryTimes(t *testing.T) {
	cfg := &conf.Etcd{
		Endpoints:     []string{"localhost:2379"},
		MaxRetryTimes: 11,
	}
	v := NewValidator(cfg)
	result := v.Validate()
	if result.IsValid {
		t.Error("expected invalid for too many retry times")
	}
}

// ---------------------------------------------------------------------------
// Errors tests
// ---------------------------------------------------------------------------

func TestEtcdError_Error(t *testing.T) {
	e := NewEtcdError(ErrCodeConfigInvalid, "test message")
	s := e.Error()
	if s == "" {
		t.Error("expected non-empty error string")
	}
}

func TestEtcdError_WithCause(t *testing.T) {
	cause := fmt.Errorf("root cause")
	e := NewEtcdError(ErrCodeClientFailed, "test").WithCause(cause)
	if e.Unwrap() != cause {
		t.Error("expected Unwrap() to return cause")
	}
	s := e.Error()
	if s == "" {
		t.Error("expected non-empty error string")
	}
}

func TestEtcdError_WithContext(t *testing.T) {
	e := NewEtcdError(ErrCodeTimeout, "timeout").WithContext("operation", "get")
	s := e.Error()
	if s == "" {
		t.Error("expected non-empty error string with context")
	}
}

func TestEtcdError_Is(t *testing.T) {
	e1 := NewEtcdError(ErrCodeConfigInvalid, "msg1")
	e2 := NewEtcdError(ErrCodeConfigInvalid, "msg2")
	e3 := NewEtcdError(ErrCodeInitFailed, "msg3")
	if !e1.Is(e2) {
		t.Error("same code errors should be equal")
	}
	if e1.Is(e3) {
		t.Error("different code errors should not be equal")
	}
	if e1.Is(fmt.Errorf("other")) {
		t.Error("non-EtcdError should not match")
	}
}

func TestEtcdError_UnwrapNil(t *testing.T) {
	e := NewEtcdError(ErrCodeConfigInvalid, "msg")
	if e.Unwrap() != nil {
		t.Error("expected nil Unwrap when no cause set")
	}
}

func TestNewConfigError(t *testing.T) {
	e := NewConfigError("bad config")
	if e.Code != ErrCodeConfigInvalid {
		t.Errorf("expected ConfigInvalid, got %s", e.Code)
	}
}

func TestNewInitError(t *testing.T) {
	e := NewInitError("not init")
	if e.Code != ErrCodeInitFailed {
		t.Errorf("expected InitFailed, got %s", e.Code)
	}
}

func TestNewClientError(t *testing.T) {
	e := NewClientError(ErrCodeClientFailed, "client error")
	if e.Code != ErrCodeClientFailed {
		t.Errorf("expected ClientFailed, got %s", e.Code)
	}
}

func TestNewNetworkError(t *testing.T) {
	e := NewNetworkError("network fail")
	if e.Code != ErrCodeNetworkError {
		t.Errorf("expected NetworkError, got %s", e.Code)
	}
}

func TestNewTimeoutError(t *testing.T) {
	e := NewTimeoutError("get")
	if e.Code != ErrCodeTimeout {
		t.Errorf("expected Timeout, got %s", e.Code)
	}
}

func TestNewRetryError(t *testing.T) {
	e := NewRetryError("retries exhausted")
	if e.Code != ErrCodeRetryExhausted {
		t.Errorf("expected RetryExhausted, got %s", e.Code)
	}
}

func TestNewHealthCheckError(t *testing.T) {
	e := NewHealthCheckError("health fail")
	if e.Code != ErrCodeHealthCheckFailed {
		t.Errorf("expected HealthCheckFailed, got %s", e.Code)
	}
}

func TestWrapConfigError(t *testing.T) {
	cause := fmt.Errorf("scan error")
	e := WrapConfigError(cause, "wrap msg")
	if e.Cause != cause {
		t.Error("expected cause to be set")
	}
}

func TestWrapClientError(t *testing.T) {
	cause := fmt.Errorf("client err")
	e := WrapClientError(cause, ErrCodeClientFailed, "wrap")
	if e.Cause != cause {
		t.Error("expected cause to be set")
	}
}

func TestWrapNetworkError(t *testing.T) {
	cause := fmt.Errorf("net err")
	e := WrapNetworkError(cause, "wrap network")
	if e.Cause != cause {
		t.Error("expected cause to be set")
	}
}

// Ensure errors package is used
var _ = errors.New

// ---------------------------------------------------------------------------
// Metrics tests
// ---------------------------------------------------------------------------

func TestEtcdMetrics_AllRecordMethods(t *testing.T) {
	m := NewEtcdMetrics()
	if m == nil {
		t.Fatal("NewEtcdMetrics returned nil")
	}

	m.RecordClientOperation("startup", "start")
	m.RecordClientOperation("startup", "success")
	m.RecordClientOperation("startup", "error")

	m.RecordConfigOperation("prefix", "get", "start")
	m.RecordConfigOperation("prefix", "get", "success")
	m.RecordConfigOperation("prefix", "get", "error")

	m.RecordConfigChange("prefix")

	m.RecordHealthCheck("start")
	m.RecordHealthCheck("success")
	m.RecordHealthCheck("error")

	m.RecordConnectionError("timeout")

	m.RecordCacheHit("prefix")
	m.RecordCacheMiss("prefix")
}

func TestEtcdMetrics_NilSafety(t *testing.T) {
	var m *Metrics
	m.RecordClientOperation("op", "status")
	m.RecordConfigOperation("prefix", "op", "status")
	m.RecordConfigChange("prefix")
	m.RecordHealthCheck("status")
	m.RecordConnectionError("errtype")
	m.RecordCacheHit("prefix")
	m.RecordCacheMiss("prefix")
}

// ---------------------------------------------------------------------------
// Resilience: DoWithRetryContext
// ---------------------------------------------------------------------------

func TestDoWithRetryContext_Success(t *testing.T) {
	r := NewRetryManager(3, time.Millisecond)
	count := 0
	err := r.DoWithRetryContext(context.Background(), func() error {
		count++
		if count < 3 {
			return fmt.Errorf("transient")
		}
		return nil
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 attempts, got %d", count)
	}
}

func TestDoWithRetryContext_Cancelled(t *testing.T) {
	r := NewRetryManager(5, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := r.DoWithRetryContext(ctx, func() error {
		return fmt.Errorf("fail")
	})
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

// ---------------------------------------------------------------------------
// buildKey
// ---------------------------------------------------------------------------

func TestBuildKey(t *testing.T) {
	if buildKey("prefix", "key") == "" {
		t.Error("expected non-empty key")
	}
	if buildKey("", "key") != "key" {
		t.Errorf("expected key when prefix empty")
	}
	if buildKey("prefix", "") != "prefix" {
		t.Errorf("expected prefix when key empty")
	}
}

// ---------------------------------------------------------------------------
// PlugEtcd: state management
// ---------------------------------------------------------------------------

func TestPlugEtcd_StateManagement(t *testing.T) {
	p := NewEtcdConfigCenter()

	if p.IsInitialized() {
		t.Error("expected not initialized")
	}
	if p.IsDestroyed() {
		t.Error("expected not destroyed")
	}

	p.setInitialized()
	if !p.IsInitialized() {
		t.Error("expected initialized")
	}

	p.clearInitialized()
	if p.IsInitialized() {
		t.Error("expected not initialized after clear")
	}

	p.setDestroyed()
	if !p.IsDestroyed() {
		t.Error("expected destroyed")
	}
}

func TestPlugEtcd_GetMetrics(t *testing.T) {
	p := NewEtcdConfigCenter()
	if p.GetMetrics() != nil {
		t.Error("expected nil metrics initially")
	}
	// Set a non-nil metrics reference without calling NewEtcdMetrics() which
	// uses promauto and panics on duplicate registration across test runs.
	p.metrics = &Metrics{}
	if p.GetMetrics() == nil {
		t.Error("expected non-nil metrics after setting")
	}
}

func TestPlugEtcd_GetNamespace(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{Namespace: "test-ns"}
	if p.GetNamespace() != "test-ns" {
		t.Errorf("expected test-ns, got %q", p.GetNamespace())
	}
}

func TestPlugEtcd_GetNamespace_NilConf(t *testing.T) {
	p := NewEtcdConfigCenter()
	// conf is nil
	ns := p.GetNamespace()
	if ns != conf.DefaultNamespace {
		t.Errorf("expected default namespace, got %q", ns)
	}
}

func TestPlugEtcd_GetEtcdConfig(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{Namespace: "test"}
	c := p.GetEtcdConfig()
	if c == nil {
		t.Error("expected non-nil config")
	}
	if c.Namespace != "test" {
		t.Errorf("unexpected namespace %q", c.Namespace)
	}
}

func TestPlugEtcd_GetClient(t *testing.T) {
	p := NewEtcdConfigCenter()
	if p.GetClient() != nil {
		t.Error("expected nil client initially")
	}
}

// ---------------------------------------------------------------------------
// checkInitialized edge cases
// ---------------------------------------------------------------------------

func TestPlugEtcd_CheckInitialized_NotInit(t *testing.T) {
	p := NewEtcdConfigCenter()
	err := p.checkInitialized()
	if err == nil {
		t.Error("expected error when not initialized")
	}
}

func TestPlugEtcd_CheckInitialized_Destroyed(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.setInitialized()
	p.setDestroyed()
	err := p.checkInitialized()
	if err == nil {
		t.Error("expected error when destroyed")
	}
}

// ---------------------------------------------------------------------------
// initComponents
// ---------------------------------------------------------------------------

func TestInitComponents_NoMetricsNoRetry(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{
		EnableMetrics: false,
		EnableRetry:   false,
	}
	err := p.initComponents()
	if err != nil {
		t.Errorf("initComponents: %v", err)
	}
	if p.metrics != nil {
		t.Error("expected nil metrics when disabled")
	}
	if p.retryManager != nil {
		t.Error("expected nil retry manager when disabled")
	}
	if p.circuitBreaker == nil {
		t.Error("expected non-nil circuit breaker always")
	}
}

func TestInitComponents_WithMetricsAndRetry(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{
		EnableMetrics: true,
		EnableRetry:   true,
		MaxRetryTimes: 3,
		RetryInterval: durationpb.New(10 * time.Millisecond),
	}
	err := p.initComponents()
	if err != nil {
		t.Errorf("initComponents: %v", err)
	}
	if p.metrics == nil {
		t.Error("expected non-nil metrics")
	}
	if p.retryManager == nil {
		t.Error("expected non-nil retry manager")
	}
}

// ---------------------------------------------------------------------------
// setDefaultConfig
// ---------------------------------------------------------------------------

func TestSetDefaultConfig(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{}
	p.setDefaultConfig()

	if p.conf.Namespace != conf.DefaultNamespace {
		t.Errorf("expected default namespace, got %q", p.conf.Namespace)
	}
	if p.conf.Timeout == nil {
		t.Error("expected default timeout")
	}
	if p.conf.DialTimeout == nil {
		t.Error("expected default dial timeout")
	}
}

// ---------------------------------------------------------------------------
// validateConfig
// ---------------------------------------------------------------------------

func TestValidateConfig_NoConf(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = nil
	err := p.validateConfig()
	if err == nil {
		t.Error("expected error with nil conf")
	}
}

func TestValidateConfig_NoEndpoints(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{}
	err := p.validateConfig()
	if err == nil {
		t.Error("expected error with empty endpoints")
	}
}

func TestValidateConfig_Valid(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{
		Endpoints: []string{"localhost:2379"},
	}
	err := p.validateConfig()
	if err != nil {
		t.Errorf("unexpected error for valid config: %v", err)
	}
}

// ---------------------------------------------------------------------------
// IsContextAware
// ---------------------------------------------------------------------------

func TestPlugEtcd_IsContextAware(t *testing.T) {
	p := NewEtcdConfigCenter()
	if !p.IsContextAware() {
		t.Error("expected IsContextAware=true")
	}
}

// ---------------------------------------------------------------------------
// InitializeContext – cancelled context
// ---------------------------------------------------------------------------

func TestPlugEtcd_InitializeContext_Cancelled(t *testing.T) {
	p := NewEtcdConfigCenter()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := p.InitializeContext(ctx, p, nil)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

// ---------------------------------------------------------------------------
// StopContext – not active
// ---------------------------------------------------------------------------

func TestPlugEtcd_StopContext_NotActive(t *testing.T) {
	p := NewEtcdConfigCenter()
	err := p.StopContext(context.Background(), p)
	if err == nil {
		t.Error("expected error when plugin is not active")
	}
}

// ---------------------------------------------------------------------------
// startupContext / shutdownContext
// ---------------------------------------------------------------------------

func TestStartupContext_Default(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{}
	ctx, cancel := p.startupContext()
	defer cancel()
	if ctx == nil {
		t.Error("expected non-nil context")
	}
}

func TestShutdownContext_Custom(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{
		ShutdownTimeout: durationpb.New(5 * time.Second),
	}
	ctx, cancel := p.shutdownContext()
	defer cancel()
	dl, ok := ctx.Deadline()
	if !ok {
		t.Error("expected deadline in shutdown context")
	}
	if time.Until(dl) > 6*time.Second {
		t.Error("deadline too far")
	}
}

func TestWithOperationTimeout(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{
		Timeout: durationpb.New(1 * time.Second),
	}
	ctx, cancel := p.withOperationTimeout(context.Background())
	defer cancel()
	if ctx == nil {
		t.Error("expected non-nil context")
	}
}

// ---------------------------------------------------------------------------
// EtcdConfigSource
// ---------------------------------------------------------------------------

func TestNewEtcdConfigSource(t *testing.T) {
	s := NewEtcdConfigSource(nil, "prefix")
	if s == nil {
		t.Fatal("expected non-nil config source")
	}
}

func TestEtcdConfigSource_Load_NilClient(t *testing.T) {
	s := NewEtcdConfigSource(nil, "prefix")
	_, err := s.Load()
	if err == nil {
		t.Error("expected error with nil client")
	}
}

func TestEtcdConfigSource_Watch_NilClient(t *testing.T) {
	s := NewEtcdConfigSource(nil, "prefix")
	_, err := s.Watch()
	if err == nil {
		t.Error("expected error with nil client")
	}
}

// ---------------------------------------------------------------------------
// config.go: HTTPRateLimit, GRPCRateLimit, NewNodeRouter
// ---------------------------------------------------------------------------

func TestHTTPRateLimit(t *testing.T) {
	p := NewEtcdConfigCenter()
	if p.HTTPRateLimit() != nil {
		t.Error("expected nil HTTP rate limiter")
	}
}

func TestGRPCRateLimit(t *testing.T) {
	p := NewEtcdConfigCenter()
	if p.GRPCRateLimit() != nil {
		t.Error("expected nil gRPC rate limiter")
	}
}

func TestNewNodeRouter(t *testing.T) {
	p := NewEtcdConfigCenter()
	if p.NewNodeRouter("service") != nil {
		t.Error("expected nil router")
	}
}

// ---------------------------------------------------------------------------
// NewServiceRegistry / NewServiceDiscovery – not initialized
// ---------------------------------------------------------------------------

func TestNewServiceRegistry_NotInitialized(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{EnableRegister: true}
	r := p.NewServiceRegistry()
	if r != nil {
		t.Error("expected nil when not initialized")
	}
}

func TestNewServiceDiscovery_NotInitialized(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{EnableDiscovery: true}
	d := p.NewServiceDiscovery()
	if d != nil {
		t.Error("expected nil when not initialized")
	}
}

// ---------------------------------------------------------------------------
// GetConfig – not initialized
// ---------------------------------------------------------------------------

func TestGetConfig_NotInitialized(t *testing.T) {
	p := NewEtcdConfigCenter()
	_, err := p.GetConfig("file", "group")
	if err == nil {
		t.Error("expected error when not initialized")
	}
}

// ---------------------------------------------------------------------------
// GetConfigSources – not initialized
// ---------------------------------------------------------------------------

func TestGetConfigSources_NotInitialized(t *testing.T) {
	p := NewEtcdConfigCenter()
	_, err := p.GetConfigSources()
	if err == nil {
		t.Error("expected error when not initialized")
	}
}

// ---------------------------------------------------------------------------
// GetConfigWatchTargets – not initialized
// ---------------------------------------------------------------------------

func TestGetConfigWatchTargets_NotInitialized(t *testing.T) {
	p := NewEtcdConfigCenter()
	_, err := p.GetConfigWatchTargets("appname")
	if err == nil {
		t.Error("expected error when not initialized")
	}
}

// ---------------------------------------------------------------------------
// GetConfigValue – not initialized
// ---------------------------------------------------------------------------

func TestGetConfigValue_NotInitialized(t *testing.T) {
	p := NewEtcdConfigCenter()
	_, err := p.GetConfigValue("prefix", "key")
	if err == nil {
		t.Error("expected error when not initialized")
	}
}

// ---------------------------------------------------------------------------
// ControlPlaneCapabilities
// ---------------------------------------------------------------------------

func TestControlPlaneCapabilities(t *testing.T) {
	p := NewEtcdConfigCenter()
	p.conf = &conf.Etcd{
		EnableRegister:  true,
		EnableDiscovery: true,
	}
	caps := p.ControlPlaneCapabilities()
	if len(caps) < 2 {
		t.Errorf("expected at least 2 capabilities, got %d", len(caps))
	}
}

// ---------------------------------------------------------------------------
// checkConfigManagementHealth
// ---------------------------------------------------------------------------

func TestCheckConfigManagementHealth(t *testing.T) {
	p := NewEtcdConfigCenter()
	// configWatchers is initialized in constructor
	err := p.checkConfigManagementHealth()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// registry helpers
// ---------------------------------------------------------------------------

func TestBuildServiceKey(t *testing.T) {
	key := buildServiceKey("ns", "svc", "id1")
	if key == "" {
		t.Error("expected non-empty service key")
	}
}

func TestBuildServicePrefix(t *testing.T) {
	prefix := buildServicePrefix("ns", "svc")
	if prefix == "" {
		t.Error("expected non-empty service prefix")
	}
}

func TestParseServiceValue_Invalid(t *testing.T) {
	_, err := parseServiceValue([]byte("not-json"), "svc")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestBuildServiceValue_Valid(t *testing.T) {
	inst := &registry.ServiceInstance{
		ID:      "id1",
		Name:    "svc",
		Version: "v1",
	}
	s, err := buildServiceValue(inst, "localhost", 8080)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if s == "" {
		t.Error("expected non-empty service value")
	}
}

func TestParseServiceValue_Valid(t *testing.T) {
	inst := &registry.ServiceInstance{
		ID:      "id1",
		Name:    "svc",
		Version: "v1",
	}
	s, err := buildServiceValue(inst, "localhost", 8080)
	if err != nil {
		t.Fatalf("buildServiceValue: %v", err)
	}
	parsed, err := parseServiceValue([]byte(s), "svc")
	if err != nil {
		t.Fatalf("parseServiceValue: %v", err)
	}
	if parsed.ID != "id1" {
		t.Errorf("expected ID=id1, got %q", parsed.ID)
	}
}

// ---------------------------------------------------------------------------
// NewEtcdRegistrar
// ---------------------------------------------------------------------------

func TestNewEtcdRegistrar(t *testing.T) {
	r := NewEtcdRegistrar(nil, "ns", 30*time.Second)
	if r == nil {
		t.Fatal("expected non-nil registrar")
	}
	// Close the lease context
	r.leaseCancel()
}

// ---------------------------------------------------------------------------
// NewEtcdDiscovery
// ---------------------------------------------------------------------------

func TestNewEtcdDiscovery(t *testing.T) {
	d := NewEtcdDiscovery(nil, "ns")
	if d == nil {
		t.Fatal("expected non-nil discovery")
	}
}
