package confluent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monedula-dev/monedula-gitops/internal/httpretry"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noRetrySleep replaces httpretry.Sleep for the duration of a test: it
// returns immediately, so retry tests here run fast and deterministically
// instead of depending on the real clock (per house style: no real-clock
// sleeps in tests).
func noRetrySleep(t *testing.T) {
	t.Helper()
	orig := httpretry.Sleep
	httpretry.Sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	t.Cleanup(func() { httpretry.Sleep = orig })
}

// newClient spins up an httptest server with the given handler and returns a
// client pointed at it, registering cleanup.
func newClient(t *testing.T, auth *Auth, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, auth, nil)
	require.NoError(t, err)
	return c
}

func TestNew_TrimsTrailingSlash(t *testing.T) {
	c, err := New("http://example.com/", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "http://example.com", c.endpoint)
	assert.NotNil(t, c.http)
}

func TestNew_CustomHTTPClient(t *testing.T) {
	custom := &http.Client{}
	c, err := New("http://example.com", nil, custom)
	require.NoError(t, err)
	assert.Same(t, custom, c.http)
}

func TestListSubjects(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/subjects", r.URL.Path)
		_ = json.NewEncoder(w).Encode([]string{"a-value", "b-value"})
	})
	subjects, err := c.ListSubjects(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"a-value", "b-value"}, subjects)
}

func TestGetSubject_HappyPath(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/subjects/a.b-value/versions/latest", r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":         42,
			"version":    7,
			"schema":     `{"type":"record"}`,
			"schemaType": "JSON",
		})
	})
	st, err := c.GetSubject(context.Background(), "a.b-value")
	require.NoError(t, err)
	require.NotNil(t, st)
	assert.Equal(t, "a.b-value", st.Subject)
	assert.Equal(t, 42, st.ID)
	assert.Equal(t, 7, st.Version)
	assert.Equal(t, `{"type":"record"}`, st.Schema.Definition)
	assert.Equal(t, schemaregistry.JSON, st.Schema.Type)
	assert.Equal(t, "", st.Compatibility)
}

func TestGetSubject_DefaultsAvroWhenTypeAbsent(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":      1,
			"version": 1,
			"schema":  `{"type":"record"}`,
		})
	})
	st, err := c.GetSubject(context.Background(), "s")
	require.NoError(t, err)
	require.NotNil(t, st)
	assert.Equal(t, schemaregistry.AVRO, st.Schema.Type)
}

func TestGetSubject_NotFound(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error_code": 40401,
			"message":    "Subject 'x' not found.",
		})
	})
	st, err := c.GetSubject(context.Background(), "x")
	require.NoError(t, err)
	assert.Nil(t, st)
}

func TestCheckCompatibility_True(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/compatibility/subjects/s/versions/latest", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, `{"x":1}`, body["schema"])
		assert.Equal(t, "AVRO", body["schemaType"])
		_ = json.NewEncoder(w).Encode(map[string]any{"is_compatible": true})
	})
	ok, err := c.CheckCompatibility(context.Background(), "s", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"x":1}`})
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestCheckCompatibility_False(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"is_compatible": false})
	})
	ok, err := c.CheckCompatibility(context.Background(), "s", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{}`})
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCheckCompatibility_NotFoundIsCompatible(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error_code": 40401,
			"message":    "Subject not found.",
		})
	})
	ok, err := c.CheckCompatibility(context.Background(), "s", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{}`})
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestRegisterSchema(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/subjects/a.b-value/versions", r.URL.Path)
		assert.Equal(t, "application/vnd.schemaregistry.v1+json", r.Header.Get("Content-Type"))
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, `{"type":"record"}`, body["schema"])
		assert.Equal(t, "PROTOBUF", body["schemaType"])
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 99})
	})
	id, err := c.RegisterSchema(context.Background(), "a.b-value", schemaregistry.Schema{Type: schemaregistry.PROTOBUF, Definition: `{"type":"record"}`})
	require.NoError(t, err)
	assert.Equal(t, 99, id)
}

func TestGetCompatibility(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/config/s", r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{"compatibilityLevel": "BACKWARD"})
	})
	level, err := c.GetCompatibility(context.Background(), "s")
	require.NoError(t, err)
	assert.Equal(t, "BACKWARD", level)
}

func TestGetCompatibility_NotFound(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error_code": 40401,
			"message":    "Subject not found.",
		})
	})
	level, err := c.GetCompatibility(context.Background(), "s")
	require.NoError(t, err)
	assert.Equal(t, "", level)
}

// Governance mode (spec §12.2) sets subject-level config before any version is
// registered. Confluent serves config independently of versions: 40408 ("Subject
// compatibility not configured") must read back as "" with a nil error.
func TestGetCompatibility_ConfigNotConfigured(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error_code": 40408,
			"message":    "Subject 'fresh-value' does not have subject-level compatibility configured",
		})
	})
	level, err := c.GetCompatibility(context.Background(), "fresh-value")
	require.NoError(t, err)
	assert.Equal(t, "", level)
}

func TestGetGlobalCompatibility(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/config", r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{"compatibilityLevel": "BACKWARD"})
	})
	level, err := c.GetGlobalCompatibility(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "BACKWARD", level)
}

// An older/unconfigured registry answering 404 on GET /config reads back as ""
// (unknown) with a nil error — callers then fall back to legacy first-set risk
// classification rather than failing the run.
func TestGetGlobalCompatibility_NotFound(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error_code": 40401,
			"message":    "Not found.",
		})
	})
	level, err := c.GetGlobalCompatibility(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "", level)
}

// SetCompatibility must succeed against a subject that has no registered
// versions yet (governance mode): the PUT /config/{subject} endpoint is
// independent of /subjects/{subject}/versions.
func TestSetCompatibility_FreshSubjectNoVersions(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/config/fresh-value", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "BACKWARD", body["compatibility"])
		_ = json.NewEncoder(w).Encode(map[string]any{"compatibility": "BACKWARD"})
	})
	require.NoError(t, c.SetCompatibility(context.Background(), "fresh-value", "BACKWARD"))
}

func TestSetCompatibility(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPut, r.Method)
		assert.Equal(t, "/config/s", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, "FULL", body["compatibility"])
		_ = json.NewEncoder(w).Encode(map[string]any{"compatibility": "FULL"})
	})
	err := c.SetCompatibility(context.Background(), "s", "FULL")
	require.NoError(t, err)
}

func TestDeleteSubject(t *testing.T) {
	called := false
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		called = true
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/subjects/a.b-value", r.URL.Path)
		_ = json.NewEncoder(w).Encode([]int{1, 2, 3})
	})
	err := c.DeleteSubject(context.Background(), "a.b-value")
	require.NoError(t, err)
	assert.True(t, called)
}

func TestDeleteSubject_NotFoundIsIdempotent(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error_code": 40401,
			"message":    "Subject not found.",
		})
	})
	err := c.DeleteSubject(context.Background(), "x")
	require.NoError(t, err)
}

func TestServerError_SurfacesMessageAndStatus(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error_code": 50001,
			"message":    "boom",
		})
	})
	_, err := c.ListSubjects(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Contains(t, err.Error(), "500")
}

func TestBasicAuth_SentWhenConfigured(t *testing.T) {
	c := newClient(t, &Auth{Username: "user", Password: "pass"}, func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "user", u)
		assert.Equal(t, "pass", p)
		_ = json.NewEncoder(w).Encode([]string{})
	})
	_, err := c.ListSubjects(context.Background())
	require.NoError(t, err)
}

func TestBasicAuth_AbsentWhenNil(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Authorization"))
		_ = json.NewEncoder(w).Encode([]string{})
	})
	_, err := c.ListSubjects(context.Background())
	require.NoError(t, err)
}

func TestSubjectPathEscaped(t *testing.T) {
	var rawPath string
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		rawPath = r.URL.EscapedPath()
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 1, "version": 1, "schema": "{}"})
	})
	_, err := c.GetSubject(context.Background(), "a/b:c value")
	require.NoError(t, err)
	assert.Equal(t, "/subjects/a%2Fb:c%20value/versions/latest", rawPath)
}

// ensure interface satisfied at runtime usage
var _ = io.Discard
var _ schemaregistry.Client = (*Client)(nil)

func TestLookupSchema_Found(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/subjects/a.b-value", r.URL.Path)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, `{"type":"record"}`, body["schema"])
		assert.Equal(t, "AVRO", body["schemaType"])
		_ = json.NewEncoder(w).Encode(map[string]any{
			"subject": "a.b-value", "id": 7, "version": 2, "schema": `{"type":"record"}`,
		})
	})
	v, err := c.LookupSchema(context.Background(), "a.b-value", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{"type":"record"}`})
	require.NoError(t, err)
	assert.Equal(t, 2, v)
}

func TestLookupSchema_NotRegistered(t *testing.T) {
	// 404 means the schema is not registered under the subject: (0, nil).
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error_code":40403,"message":"Schema not found"}`)
	})
	v, err := c.LookupSchema(context.Background(), "a.b-value", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{}`})
	require.NoError(t, err)
	assert.Equal(t, 0, v)
}

func TestLookupSchema_ServerError(t *testing.T) {
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error_code":50001,"message":"backend down"}`)
	})
	_, err := c.LookupSchema(context.Background(), "s", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{}`})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backend down")
}

// ---- Retry wiring (internal/httpretry) ----

func TestListSubjects_RetriesOn503ThenSucceeds(t *testing.T) {
	noRetrySleep(t)
	var hits atomic.Int32
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode([]string{"a-value"})
	})
	subjects, err := c.ListSubjects(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"a-value"}, subjects)
	assert.Equal(t, int32(2), hits.Load(), "expected exactly 2 attempts (503 then 200)")
}

func TestListSubjects_PersistentServiceUnavailable_FailsAfterThreeAttempts(t *testing.T) {
	noRetrySleep(t)
	var hits atomic.Int32
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	_, err := c.ListSubjects(context.Background())
	require.Error(t, err)
	assert.Equal(t, int32(3), hits.Load(), "expected exactly 3 attempts total")
}

func TestListSubjects_401NotRetried(t *testing.T) {
	noRetrySleep(t)
	var hits atomic.Int32
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	})
	_, err := c.ListSubjects(context.Background())
	require.Error(t, err)
	assert.Equal(t, int32(1), hits.Load(), "401 must not be retried")
}

func TestRegisterSchema_WriteNeverRetriedOn503(t *testing.T) {
	noRetrySleep(t)
	var hits atomic.Int32
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	_, err := c.RegisterSchema(context.Background(), "s", schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: `{}`})
	require.Error(t, err)
	assert.Equal(t, int32(1), hits.Load(), "a write (RegisterSchema) must never be retried, even on 503")
}

func TestListSubjects_CtxCancelDuringBackoffReturnsPromptly(t *testing.T) {
	// Deliberately real backoff here (no noRetrySleep) to prove cancellation
	// during the sleep itself returns promptly rather than waiting it out.
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := c.ListSubjects(ctx)
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.Less(t, elapsed, 2*time.Second, "ctx cancellation during backoff must return promptly")
}

func TestGetCompatibility_RetryAfterHonored(t *testing.T) {
	var gotDelay time.Duration
	var calls int
	orig := httpretry.Sleep
	httpretry.Sleep = func(ctx context.Context, d time.Duration) error {
		calls++
		if calls == 1 {
			gotDelay = d
		}
		return ctx.Err()
	}
	t.Cleanup(func() { httpretry.Sleep = orig })

	var hits atomic.Int32
	c := newClient(t, nil, func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"compatibilityLevel": "BACKWARD"})
	})
	level, err := c.GetCompatibility(context.Background(), "s")
	require.NoError(t, err)
	assert.Equal(t, "BACKWARD", level)
	assert.Equal(t, 3*time.Second, gotDelay, "Retry-After: 3 must be honored")
}
