// Package httpretry provides bounded retry with jittered backoff for
// idempotent HTTP requests shared by the Schema Registry and MDS Confluent
// clients (internal/schemaregistry/confluent, internal/mds/confluent).
//
// Only reads should ever set Idempotent: true on the Request passed to Do.
// Writes (schema registration, compatibility changes, role-binding
// mutations) must never be blindly retried, since a request that reached the
// server but failed to return a response could otherwise be applied twice.
package httpretry

import (
	"context"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

// maxAttempts is the total number of attempts (1 initial + retries) made for
// an idempotent request.
const maxAttempts = 3

// baseDelay is the base of the exponential backoff (doubled each retry) before
// jitter is applied.
const baseDelay = 200 * time.Millisecond

// maxDelay caps the computed backoff (including Retry-After) so a
// misbehaving/malicious server cannot stall a caller indefinitely.
const maxDelay = 30 * time.Second

// Sleep is the delay function used between attempts; a package-level var so
// tests can inject a fast, deterministic stand-in instead of sleeping in real
// time. Production code must not modify it. Sleep must return promptly (and
// report cancellation) when ctx is done.
var Sleep = ctxSleep

// ctxSleep is the default Sleep implementation: it waits for d or until ctx
// is done, whichever comes first.
func ctxSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// jitter, when non-nil, is called to add randomness to the computed backoff.
// Overridable in tests for determinism; production leaves it as rand.Int63n-based.
var jitter = func(n time.Duration) time.Duration {
	if n <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(n)))
}

// RequestFactory builds a fresh *http.Request for one attempt. It must return
// a request with a body that has not been consumed by a previous attempt
// (e.g. by re-wrapping the original bytes in a new io.Reader each call) so
// that a retry after a transport-level failure can resend the payload.
type RequestFactory func(ctx context.Context) (*http.Request, error)

// Do executes newReq via client, retrying on transient failures when
// idempotent is true. It returns the final *http.Response (2xx or terminal
// non-2xx; callers are responsible for closing its Body) or an error if every
// attempt failed at the transport level or ctx was canceled.
//
// Retry conditions (idempotent only): HTTP 429; HTTP 502/503/504; and
// transport-level errors (connection refused/reset, EOF, etc — anything
// http.Client.Do itself returns an error for). Not retried: any other 4xx,
// and context cancellation/deadline (surfaced immediately).
//
// Non-idempotent requests (idempotent=false) are always single-shot: newReq
// is invoked once and the result (response or error) is returned as-is.
func Do(ctx context.Context, client *http.Client, newReq RequestFactory, idempotent bool) (*http.Response, error) {
	attempts := 1
	if idempotent {
		attempts = maxAttempts
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		req, err := newReq(ctx)
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err == nil && !isRetryableStatus(resp.StatusCode) {
			return resp, nil
		}

		if err != nil {
			// Never retry a context cancellation/deadline: the caller is done
			// waiting, so burning another attempt (and sleeping) would only
			// delay surfacing that fact.
			if ctx.Err() != nil {
				return nil, err
			}
			lastErr = err
		} else {
			lastErr = nil // resp is the terminal error carrier below
		}

		if attempt == attempts {
			if resp != nil {
				return resp, nil // let the caller interpret the final non-2xx status
			}
			return nil, lastErr
		}

		// Decide the backoff for this retry.
		delay := backoff(attempt)
		if resp != nil {
			if ra, ok := retryAfter(resp); ok {
				delay = ra
			}
			// Drain and close the body before retrying so the underlying
			// connection can be reused/released.
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}

		if sleepErr := Sleep(ctx, delay); sleepErr != nil {
			return nil, sleepErr
		}
	}
	// Unreachable: the loop always returns on its final iteration.
	return nil, lastErr
}

// isRetryableStatus reports whether status is one we retry for idempotent
// requests: 429 (rate limited) and 502/503/504 (upstream/gateway trouble).
// Any other status (including other 4xx, and 2xx) is terminal.
func isRetryableStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// backoff computes the jittered exponential delay before the given attempt
// number's *next* attempt (attempt is 1-based: backoff(1) is the delay after
// the first try). Capped at maxDelay.
func backoff(attempt int) time.Duration {
	d := baseDelay << uint(attempt-1) // 200ms, 400ms, 800ms, ...
	if d > maxDelay {
		d = maxDelay
	}
	return d/2 + jitter(d/2) // half fixed, half jittered
}

// retryAfter parses a Retry-After header in the trivially-parseable seconds
// form (a non-negative integer). The HTTP-date form is not supported; on any
// parse failure or absence, ok is false and the caller falls back to the
// computed exponential backoff.
func retryAfter(resp *http.Response) (time.Duration, bool) {
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	secs, err := strconv.Atoi(v)
	if err != nil || secs < 0 {
		return 0, false
	}
	d := time.Duration(secs) * time.Second
	if d > maxDelay {
		d = maxDelay
	}
	return d, true
}
