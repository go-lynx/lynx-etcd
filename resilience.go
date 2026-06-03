package etcd

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/go-lynx/lynx/log"
)

// RetryManager retry manager
// Provides exponential backoff retry mechanism
type RetryManager struct {
	maxRetries    int
	retryInterval time.Duration
	backoffFactor float64
}

// NewRetryManager creates new retry manager
func NewRetryManager(maxRetries int, retryInterval time.Duration) *RetryManager {
	return &RetryManager{
		maxRetries:    maxRetries,
		retryInterval: retryInterval,
		backoffFactor: 2.0, // Exponential backoff factor
	}
}

// DoWithRetry executes operation with retry
func (r *RetryManager) DoWithRetry(operation func() error) error {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		if err := operation(); err == nil {
			if attempt > 0 {
				log.Infof("Operation succeeded after %d retries", attempt)
			}
			return nil
		} else {
			lastErr = err
			if attempt < r.maxRetries {
				// Calculate backoff time
				backoffTime := r.calculateBackoff(attempt)
				log.Warnf("Operation failed (attempt %d/%d): %v, retrying in %v",
					attempt+1, r.maxRetries+1, err, backoffTime)
				time.Sleep(backoffTime)
			}
		}
	}

	return fmt.Errorf("operation failed after %d attempts, last error: %w", r.maxRetries+1, lastErr)
}

// DoWithRetryContext executes operation with retry (supports context)
func (r *RetryManager) DoWithRetryContext(ctx context.Context, operation func() error) error {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation cancelled: %w", ctx.Err())
		default:
		}

		if err := operation(); err == nil {
			if attempt > 0 {
				log.Infof("Operation succeeded after %d retries", attempt)
			}
			return nil
		} else {
			lastErr = err
			if attempt < r.maxRetries {
				backoffTime := r.calculateBackoff(attempt)
				log.Warnf("Operation failed (attempt %d/%d): %v, retrying in %v",
					attempt+1, r.maxRetries+1, err, backoffTime)

				select {
				case <-time.After(backoffTime):
				case <-ctx.Done():
					return fmt.Errorf("operation cancelled during retry: %w", ctx.Err())
				}
			}
		}
	}

	return fmt.Errorf("operation failed after %d attempts, last error: %w", r.maxRetries+1, lastErr)
}

// calculateBackoff calculates backoff time
func (r *RetryManager) calculateBackoff(attempt int) time.Duration {
	// Exponential backoff: base * factor^attempt
	backoffSeconds := float64(r.retryInterval) * math.Pow(r.backoffFactor, float64(attempt))

	// Limit maximum backoff time to 30 seconds
	maxBackoff := 30 * time.Second
	if time.Duration(backoffSeconds) > maxBackoff {
		return maxBackoff
	}

	return time.Duration(backoffSeconds)
}

// CircuitBreaker protects downstream operations using a simple closed/open/half-open
// state machine. It is safe for concurrent use.
//
// State transitions:
//
//	Closed  → Open      : failure rate crosses the configured threshold
//	Open    → HalfOpen  : openDuration has elapsed since last failure
//	HalfOpen → Closed   : the probe call succeeds (counters reset)
//	HalfOpen → Open     : the probe call fails
type CircuitBreaker struct {
	mu           sync.Mutex
	threshold    float64
	failureCount int
	successCount int
	lastFailure  time.Time
	state        CircuitState
	openDuration time.Duration // how long to stay Open before probing
}

// CircuitState enumerates the three possible circuit-breaker states.
type CircuitState int

const (
	// CircuitStateClosed is the normal operating state: all calls are forwarded.
	CircuitStateClosed CircuitState = iota
	// CircuitStateOpen means the circuit is tripped: calls are rejected immediately.
	CircuitStateOpen
	// CircuitStateHalfOpen means one probe call is allowed to test recovery.
	CircuitStateHalfOpen
)

// NewCircuitBreaker creates a new CircuitBreaker with the given failure-rate
// threshold (0–1). A threshold of 0.5 means the circuit opens when more than
// 50 % of all calls have failed. The circuit remains open for 30 s before
// transitioning to half-open for a probe attempt.
func NewCircuitBreaker(threshold float64) *CircuitBreaker {
	return &CircuitBreaker{
		threshold:    threshold,
		state:        CircuitStateClosed,
		openDuration: 30 * time.Second,
	}
}

// Do executes operation under circuit-breaker protection.
// The lock is released before the operation runs to avoid holding it during
// potentially slow I/O, then re-acquired to update the state.
func (cb *CircuitBreaker) Do(operation func() error) error {
	if err := cb.checkState(); err != nil {
		return err
	}

	err := operation()

	cb.mu.Lock()
	if err != nil {
		cb.recordFailureLocked()
	} else {
		cb.recordSuccessLocked()
	}
	cb.mu.Unlock()

	return err
}

// checkState decides whether the call should proceed and advances Open→HalfOpen
// when the cool-down window has elapsed.
func (cb *CircuitBreaker) checkState() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitStateClosed:
		return nil
	case CircuitStateOpen:
		if time.Since(cb.lastFailure) >= cb.openDuration {
			cb.state = CircuitStateHalfOpen
			log.Infof("Circuit breaker transitioning to half-open state")
			return nil // allow the probe call through
		}
		return fmt.Errorf("circuit breaker is open")
	case CircuitStateHalfOpen:
		log.Infof("Circuit breaker in half-open state, allowing one attempt")
		return nil
	default:
		return fmt.Errorf("invalid circuit breaker state: %v", cb.state)
	}
}

// recordFailureLocked records a failure. Must be called with mu held.
func (cb *CircuitBreaker) recordFailureLocked() {
	cb.failureCount++
	cb.lastFailure = time.Now()

	total := cb.failureCount + cb.successCount
	var failureRate float64
	if total > 0 {
		failureRate = float64(cb.failureCount) / float64(total)
	}

	switch cb.state {
	case CircuitStateClosed:
		if failureRate >= cb.threshold {
			cb.state = CircuitStateOpen
			log.Warnf("Circuit breaker opened: failure rate %.2f >= threshold %.2f",
				failureRate, cb.threshold)
		}
	case CircuitStateHalfOpen:
		cb.state = CircuitStateOpen
		log.Warnf("Circuit breaker reopened after failed probe attempt")
	}
}

// recordSuccessLocked records a success. Must be called with mu held.
func (cb *CircuitBreaker) recordSuccessLocked() {
	cb.successCount++

	if cb.state == CircuitStateHalfOpen {
		cb.state = CircuitStateClosed
		cb.failureCount = 0
		cb.successCount = 0
		log.Infof("Circuit breaker closed after successful probe attempt")
	}
}
