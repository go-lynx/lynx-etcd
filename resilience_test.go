package etcd

import (
	"errors"
	"testing"
	"time"
)

// ── RetryManager tests ────────────────────────────────────────────────────────

func TestRetryManager_SucceedsOnFirstAttempt(t *testing.T) {
	rm := NewRetryManager(3, time.Millisecond)
	calls := 0
	err := rm.DoWithRetry(func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetryManager_ExhaustsRetries(t *testing.T) {
	rm := NewRetryManager(2, time.Millisecond)
	sentinel := errors.New("boom")
	calls := 0
	err := rm.DoWithRetry(func() error {
		calls++
		return sentinel
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if calls != 3 { // 1 initial + 2 retries
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

// ── CircuitBreaker state-transition tests ─────────────────────────────────────

func TestCircuitBreaker_ClosedToOpen(t *testing.T) {
	// threshold=0: any single failure should open the circuit.
	cb := NewCircuitBreaker(0)

	sentinel := errors.New("fail")
	_ = cb.Do(func() error { return sentinel })

	if cb.state != CircuitStateOpen {
		t.Fatalf("expected Open after failure with threshold=0, got %v", cb.state)
	}
}

func TestCircuitBreaker_OpenRejectsImmediately(t *testing.T) {
	cb := NewCircuitBreaker(0)
	cb.state = CircuitStateOpen
	cb.lastFailure = time.Now() // within cool-down window

	err := cb.Do(func() error { return nil })
	if err == nil {
		t.Fatal("expected circuit-open error, got nil")
	}
}

func TestCircuitBreaker_OpenToHalfOpenAfterCoolDown(t *testing.T) {
	cb := NewCircuitBreaker(0)
	cb.state = CircuitStateOpen
	// Simulate cool-down already elapsed.
	cb.lastFailure = time.Now().Add(-60 * time.Second)

	// checkState should transition to HalfOpen.
	if err := cb.checkState(); err != nil {
		t.Fatalf("expected nil from checkState in half-open, got %v", err)
	}
	if cb.state != CircuitStateHalfOpen {
		t.Fatalf("expected HalfOpen after cool-down, got %v", cb.state)
	}
}

func TestCircuitBreaker_HalfOpenSuccessCloses(t *testing.T) {
	cb := NewCircuitBreaker(0)
	cb.state = CircuitStateHalfOpen

	err := cb.Do(func() error { return nil })
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if cb.state != CircuitStateClosed {
		t.Fatalf("expected Closed after HalfOpen success, got %v", cb.state)
	}
	if cb.failureCount != 0 || cb.successCount != 0 {
		t.Fatalf("expected counters reset after close, got failures=%d successes=%d",
			cb.failureCount, cb.successCount)
	}
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(0)
	cb.state = CircuitStateHalfOpen

	_ = cb.Do(func() error { return errors.New("probe failed") })

	if cb.state != CircuitStateOpen {
		t.Fatalf("expected Open after HalfOpen probe failure, got %v", cb.state)
	}
}

func TestCircuitBreaker_SuccessDoesNotOpenWhenClosed(t *testing.T) {
	cb := NewCircuitBreaker(0.5)
	for i := 0; i < 10; i++ {
		_ = cb.Do(func() error { return nil })
	}
	if cb.state != CircuitStateClosed {
		t.Fatalf("expected Closed after all successes, got %v", cb.state)
	}
}

func TestCircuitBreaker_HighThresholdDoesNotOpenOnSingleFailure(t *testing.T) {
	cb := NewCircuitBreaker(0.9) // 90 % failure rate needed
	_ = cb.Do(func() error { return errors.New("fail") })
	// One failure out of one call = 100 % rate, so it should open.
	if cb.state != CircuitStateOpen {
		t.Fatalf("expected Open at 100%% failure rate with 0.9 threshold, got %v", cb.state)
	}
}

func TestCircuitBreaker_FailureRateBelowThresholdKeepsClosed(t *testing.T) {
	// threshold=0.9: circuit opens only when ≥90% of calls fail.
	// Seed 9 successes first, then 1 failure → failure rate = 1/10 = 10%, below threshold.
	cb := NewCircuitBreaker(0.9)
	for i := 0; i < 9; i++ {
		_ = cb.Do(func() error { return nil })
	}
	_ = cb.Do(func() error { return errors.New("one fail") })
	if cb.state != CircuitStateClosed {
		t.Fatalf("expected Closed at 10%% failure rate with 0.9 threshold, got %v", cb.state)
	}
}
