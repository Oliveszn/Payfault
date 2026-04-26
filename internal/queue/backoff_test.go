package queue

// Pure unit test no database needed.
// backoffDuration is an unexported function in the queue package,

import (
	"testing"
	"time"
)

// TestBackoffDuration_Increases verifies that each subsequent attempt
// waits longer than the previous one (before the cap kicks in)
// If this were wrong say, backoff stayed flat you'd hammer a struggling
// downstream service at full speed, which is exactly what backoff prevents.
func TestBackoffDuration_Increases(t *testing.T) {
	prev := time.Duration(0)

	// attempts 0–3 are below the 30s cap so each should be strictly longer
	for attempt := 0; attempt < 4; attempt++ {
		got := backoffDuration(attempt)
		if got <= prev {
			t.Errorf("attempt %d: backoff %v is not greater than previous %v", attempt, got, prev)
		}
		prev = got
	}
}

// TestBackoffDuration_Cap verifies the wait never exceeds 30s.
// Without a cap, a transaction failing on attempt 10 would wait 2^10 = 1024 seconds.
func TestBackoffDuration_Cap(t *testing.T) {
	cap := 30 * time.Second

	// test well beyond where the cap should kick in
	for attempt := 5; attempt <= 20; attempt++ {
		got := backoffDuration(attempt)
		if got > cap+cap/5 { // allow for the +20% jitter
			t.Errorf("attempt %d: backoff %v exceeds cap %v", attempt, got, cap)
		}
	}
}

// TestBackoffDuration_FirstAttempt checks the base case.
// Attempt 0 should be around 2s (base), adjusted by jitter.
// We don't assert an exact value because jitter is intentionally variable we just verify it's in a reasonable range.
func TestBackoffDuration_FirstAttempt(t *testing.T) {
	got := backoffDuration(0)
	min := 1 * time.Second
	max := 3 * time.Second

	if got < min || got > max {
		t.Errorf("attempt 0: backoff %v, want between %v and %v", got, min, max)
	}
}
