package httpretry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noSleep replaces Sleep for tests: it returns immediately (unless ctx is
// already done) so retry tests run fast and deterministically, without
// depending on the real clock.
func noSleep(ctx context.Context, _ time.Duration) error {
	return ctx.Err()
}

// useNoSleep installs noSleep for the duration of the test.
func useNoSleep(t *testing.T) {
	t.Helper()
	orig := Sleep
	Sleep = noSleep
	t.Cleanup(func() { Sleep = orig })
}

func newGetReq(url string) RequestFactory {
	return func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	}
}

func TestDo_RetriesOn503ThenSucceeds(t *testing.T) {
	useNoSleep(t)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	resp, err := Do(context.Background(), srv.Client(), newGetReq(srv.URL), true)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(2), hits.Load(), "expected exactly 2 attempts (503 then 200)")
}

func TestDo_PersistentFailureFailsAfterMaxAttempts(t *testing.T) {
	useNoSleep(t)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	resp, err := Do(context.Background(), srv.Client(), newGetReq(srv.URL), true)
	require.NoError(t, err) // Do returns the terminal response, not an error, for a final non-2xx status
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, int32(maxAttempts), hits.Load(), "expected exactly %d attempts", maxAttempts)
}

func TestDo_401NoRetry(t *testing.T) {
	useNoSleep(t)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)

	resp, err := Do(context.Background(), srv.Client(), newGetReq(srv.URL), true)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, int32(1), hits.Load(), "401 must not be retried")
}

func TestDo_NonIdempotentNeverRetries(t *testing.T) {
	useNoSleep(t)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	newReq := func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, nil)
	}

	resp, err := Do(context.Background(), srv.Client(), newReq, false)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, int32(1), hits.Load(), "a write (idempotent=false) must never be retried, even on 503")
}

func TestDo_CtxCancelDuringBackoffReturnsPromptly(t *testing.T) {
	// Deliberately do NOT install noSleep here: we want the real ctxSleep to
	// prove it honors ctx.Done() promptly rather than blocking for the full
	// computed backoff duration.
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := Do(ctx, srv.Client(), newGetReq(srv.URL), true)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled, got %v", err)
	// The first backoff alone (attempt 1) is baseDelay/2 .. baseDelay = 100-200ms;
	// cancellation fires at 20ms, so returning within a few hundred ms proves we
	// didn't wait out the full sleep (let alone two of them).
	assert.Less(t, elapsed, 2*time.Second, "ctx cancellation during backoff must return promptly")
}

func TestDo_CtxAlreadyCanceledNotRetried(t *testing.T) {
	useNoSleep(t)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := Do(ctx, srv.Client(), newGetReq(srv.URL), true)
	require.Error(t, err)
}

func TestDo_TransportErrorRetried(t *testing.T) {
	useNoSleep(t)
	// A server that accepts the connection and then immediately hangs up
	// without writing a response simulates a connection-reset/EOF transport
	// error on the first attempt(s).
	var hits atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("ResponseWriter does not support hijacking")
		}
		conn, _, err := hj.Hijack()
		require.NoError(t, err)
		if n < maxAttempts {
			_ = conn.Close() // abrupt close: connection reset / EOF for the client
			return
		}
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"))
		_ = conn.Close()
	}))
	srv.Start()
	t.Cleanup(srv.Close)

	resp, err := Do(context.Background(), srv.Client(), newGetReq(srv.URL), true)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(maxAttempts), hits.Load())
}

func TestDo_RetryAfterHonored(t *testing.T) {
	// Install a Sleep spy that records the requested delay without actually
	// sleeping, so we can assert Retry-After was parsed and used instead of
	// the exponential backoff.
	var gotDelay time.Duration
	orig := Sleep
	Sleep = func(ctx context.Context, d time.Duration) error {
		gotDelay = d
		return ctx.Err()
	}
	t.Cleanup(func() { Sleep = orig })

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n == 1 {
			w.Header().Set("Retry-After", "7")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	resp, err := Do(context.Background(), srv.Client(), newGetReq(srv.URL), true)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 7*time.Second, gotDelay, "Retry-After: 7 must be honored verbatim")
}

func TestDo_RetryAfterUnparseableFallsBackToBackoff(t *testing.T) {
	var gotDelay time.Duration
	var calls int
	orig := Sleep
	Sleep = func(ctx context.Context, d time.Duration) error {
		calls++
		if calls == 1 {
			gotDelay = d // only the first attempt's backoff matters for this assertion
		}
		return ctx.Err()
	}
	t.Cleanup(func() { Sleep = orig })

	origJitter := jitter
	jitter = func(time.Duration) time.Duration { return 0 } // deterministic
	t.Cleanup(func() { jitter = origJitter })

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "Wed, 21 Oct 2099 07:28:00 GMT") // HTTP-date form: not supported
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)

	_, err := Do(context.Background(), srv.Client(), newGetReq(srv.URL), true)
	require.NoError(t, err)
	assert.Equal(t, baseDelay/2, gotDelay, "unparseable Retry-After must fall back to computed backoff")
}

func TestDo_429Retried(t *testing.T) {
	useNoSleep(t)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	resp, err := Do(context.Background(), srv.Client(), newGetReq(srv.URL), true)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(2), hits.Load())
}

func TestDo_RequestFactoryCalledFreshEachAttempt(t *testing.T) {
	useNoSleep(t)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, 5)
		n, _ := r.Body.Read(body)
		if string(body[:n]) != "hello" {
			t.Errorf("body = %q, want %q", body[:n], "hello")
		}
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	newReq := func(ctx context.Context) (*http.Request, error) {
		return http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, strings.NewReader("hello"))
	}

	resp, err := Do(context.Background(), srv.Client(), newReq, true)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(2), hits.Load())
}
