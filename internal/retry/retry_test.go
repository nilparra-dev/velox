package retry

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestBackoffBoundsAndJitter(t *testing.T) {
	p := Policy{MaxAttempts: 6, Base: 500 * time.Millisecond, Max: 30 * time.Second}
	if got := p.Backoff(0, 1.0); got != 500*time.Millisecond {
		t.Errorf("Backoff(0,1.0) = %v, want 500ms", got)
	}
	if got := p.Backoff(0, 0.0); got != 0 {
		t.Errorf("Backoff(0,0.0) = %v, want 0", got)
	}
	if got := p.Backoff(2, 1.0); got != 2*time.Second {
		t.Errorf("Backoff(2,1.0) = %v, want 2s", got)
	}
	if got := p.Backoff(100, 1.0); got != 30*time.Second {
		t.Errorf("Backoff(overflow,1.0) = %v, want cap 30s", got)
	}
	if got := p.Backoff(0, 2.0); got != 500*time.Millisecond {
		t.Errorf("jitter clamps >1 to 1: got %v", got)
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		err    error
		status int
		want   Decision
	}{
		{errors.New("conn reset"), 0, Retryable},
		{nil, http.StatusTooManyRequests, Retryable},
		{nil, http.StatusServiceUnavailable, Retryable},
		{nil, http.StatusInternalServerError, Retryable},
		{nil, http.StatusRequestTimeout, Retryable},
		{nil, http.StatusNotFound, Fatal},
		{nil, http.StatusForbidden, Fatal},
		{nil, http.StatusRequestedRangeNotSatisfiable, Fatal},
	}
	for _, c := range cases {
		if got := Classify(c.err, c.status); got != c.want {
			t.Errorf("Classify(%v,%d) = %v, want %v", c.err, c.status, got, c.want)
		}
	}
}

func TestRetryAfter(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	h := http.Header{}
	h.Set("Retry-After", "5")
	if d, ok := RetryAfter(h, now); !ok || d != 5*time.Second {
		t.Errorf("seconds: got %v ok=%v, want 5s true", d, ok)
	}

	h = http.Header{}
	h.Set("Retry-After", now.Add(10*time.Second).UTC().Format(http.TimeFormat))
	if d, ok := RetryAfter(h, now); !ok || d != 10*time.Second {
		t.Errorf("http-date: got %v ok=%v, want 10s true", d, ok)
	}

	if _, ok := RetryAfter(http.Header{}, now); ok {
		t.Error("missing header should return ok=false")
	}
	h = http.Header{}
	h.Set("Retry-After", "garbage")
	if _, ok := RetryAfter(h, now); ok {
		t.Error("garbage header should return ok=false")
	}
}
