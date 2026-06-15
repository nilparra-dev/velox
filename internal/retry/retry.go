package retry

import (
	"net/http"
	"strconv"
	"time"
)

// Decision is the outcome of classifying a failed attempt.
type Decision int

const (
	Fatal Decision = iota
	Retryable
)

// Policy controls retry attempts and backoff.
type Policy struct {
	MaxAttempts int
	Base        time.Duration
	Max         time.Duration
}

// Default returns the standard policy: up to 6 attempts, 500ms base, 30s cap.
func Default() Policy {
	return Policy{MaxAttempts: 6, Base: 500 * time.Millisecond, Max: 30 * time.Second}
}

// Backoff returns the delay before a 0-based attempt index using exponential
// growth (Base * 2^attempt) capped at Max, scaled by full jitter in [0,1].
// Callers pass rand.Float64() in production; tests pass fixed values.
func (p Policy) Backoff(attempt int, jitter float64) time.Duration {
	d := p.Base << uint(attempt)
	if d <= 0 || d > p.Max { // overflow or above cap
		d = p.Max
	}
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	return time.Duration(jitter * float64(d))
}

// Classify decides whether an attempt that produced err and/or statusCode is
// retryable. statusCode is 0 when the request failed before a response. Any
// transport-level error is treated as transient; the caller must check the
// parent context first (a cancelled parent means stop, not retry).
func Classify(err error, statusCode int) Decision {
	if err != nil {
		return Retryable
	}
	switch statusCode {
	case http.StatusRequestTimeout, // 408
		http.StatusTooManyRequests,     // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return Retryable
	}
	return Fatal
}

// RetryAfter parses a Retry-After header (delta-seconds or HTTP-date) relative
// to now, returning the delay and whether a valid value was present.
func RetryAfter(h http.Header, now time.Time) (time.Duration, bool) {
	v := h.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			secs = 0
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
