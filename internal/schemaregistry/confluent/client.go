// Package confluent implements schemaregistry.Client against the Confluent
// Schema Registry REST API using only the standard library.
package confluent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/monedula-dev/monedula-gitops/internal/httpretry"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

// acceptJSON is the Accept header value preferred by the Confluent SR API.
const acceptJSON = "application/vnd.schemaregistry.v1+json"

// registerContentType is the Content-Type required for schema registration.
const registerContentType = "application/vnd.schemaregistry.v1+json"

// defaultTimeout bounds each HTTP request to the registry.
const defaultTimeout = 30 * time.Second

// Auth holds resolved plaintext basic-auth credentials. The caller resolves
// any secret references (via internal/secrets) before constructing the client.
type Auth struct {
	Username string
	Password string
}

// Client talks to a Confluent Schema Registry over HTTP.
type Client struct {
	endpoint string
	auth     *Auth
	http     *http.Client
}

// compile-time assertion that Client implements the seam.
var _ schemaregistry.Client = (*Client)(nil)

// New returns a Client for the registry at endpoint. A trailing "/" is
// trimmed. If auth is non-nil, basic auth is sent on every request.
// httpClient, when non-nil, is used as-is (caller is responsible for TLS
// config, e.g. a custom CA); when nil, a default http.Client with a 30 s
// timeout is used. Mirrors mds/confluent.New.
func New(endpoint string, auth *Auth, httpClient *http.Client) (*Client, error) {
	endpoint = strings.TrimRight(endpoint, "/")
	if endpoint == "" {
		return nil, fmt.Errorf("schema registry endpoint is empty")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	return &Client{
		endpoint: endpoint,
		auth:     auth,
		http:     httpClient,
	}, nil
}

// srError is the standard Confluent SR error envelope.
type srError struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

// doJSON builds and executes a request against path (relative to endpoint),
// encoding body as JSON when non-nil and decoding the response into out when
// non-nil. The Content-Type for the request body is "application/json"; use
// do for a custom Content-Type. It returns the HTTP status code. On a non-2xx
// status it returns an error including the status and, when present, the SR
// error message. Callers that special-case 404 should inspect the returned
// status rather than relying on the error.
//
// idempotent selects retry behavior (see do): pass true only for read paths
// (GET, and the POST lookup endpoints that are read-only per the Confluent SR
// API — see the call sites' comments). Writes must pass false.
func (c *Client) doJSON(ctx context.Context, method, path string, body, out any, idempotent bool) (int, error) {
	return c.do(ctx, method, path, "application/json", body, out, idempotent)
}

// do is the shared request/response machinery. contentType is applied only
// when body is non-nil.
//
// idempotent, when true, allows internal/httpretry to retry the request up to
// a bounded number of times on 429/502/503/504 and transport-level errors
// (connection refused/reset, EOF, ...), with jittered backoff honoring
// Retry-After when present. It must be false for any request that mutates
// registry state (schema registration, compatibility updates, deletes),
// since those are not safe to blindly resend after an ambiguous failure.
func (c *Client) do(ctx context.Context, method, path, contentType string, body, out any, idempotent bool) (int, error) {
	var reqBytes []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("encode request body: %w", err)
		}
		reqBytes = b
	}

	newReq := func(ctx context.Context) (*http.Request, error) {
		var reqBody io.Reader
		if reqBytes != nil {
			// Fresh reader each attempt: a retried request must be able to
			// resend the full body from the start.
			reqBody = bytes.NewReader(reqBytes)
		}
		req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, reqBody)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Accept", acceptJSON)
		if body != nil {
			req.Header.Set("Content-Type", contentType)
		}
		if c.auth != nil {
			req.SetBasicAuth(c.auth.Username, c.auth.Password)
		}
		return req, nil
	}

	resp, err := httpretry.Do(ctx, c.http, newReq, idempotent)
	if err != nil {
		// NOTE: never wrap with the URL or credentials.
		return 0, fmt.Errorf("schema registry request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, c.statusError(resp.StatusCode, respBody)
	}

	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return resp.StatusCode, fmt.Errorf("decode response: %w", err)
		}
	}
	return resp.StatusCode, nil
}

// statusError builds an error from a non-2xx response, surfacing the SR error
// message when the body parses as the standard envelope. The message does not
// contain credentials, so it is safe to surface.
func (c *Client) statusError(status int, body []byte) error {
	var se srError
	if err := json.Unmarshal(body, &se); err == nil && se.Message != "" {
		return fmt.Errorf("schema registry returned status %d: %s", status, se.Message)
	}
	return fmt.Errorf("schema registry returned status %d", status)
}

// esc URL-escapes a subject name for use in a path segment. Subjects can
// contain dots and colons (e.g. "topic-value", "ns:type").
func esc(subject string) string {
	return url.PathEscape(subject)
}

// ListSubjects returns all registered subject names.
func (c *Client) ListSubjects(ctx context.Context) ([]string, error) {
	var out []string
	if _, err := c.doJSON(ctx, http.MethodGet, "/subjects", nil, &out, true); err != nil {
		return nil, err
	}
	return out, nil
}

// subjectVersion is the response shape of the version endpoints.
type subjectVersion struct {
	ID         int    `json:"id"`
	Version    int    `json:"version"`
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
}

// GetSubject returns the latest version of subject, or (nil, nil) if absent.
func (c *Client) GetSubject(ctx context.Context, subject string) (*schemaregistry.SubjectState, error) {
	path := fmt.Sprintf("/subjects/%s/versions/latest", esc(subject))
	var v subjectVersion
	status, err := c.doJSON(ctx, http.MethodGet, path, nil, &v, true)
	if status == http.StatusNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	st := schemaregistry.SchemaType(v.SchemaType)
	if st == "" {
		st = schemaregistry.AVRO
	}
	return &schemaregistry.SubjectState{
		Subject: subject,
		ID:      v.ID,
		Version: v.Version,
		Schema: schemaregistry.Schema{
			Type:       st,
			Definition: v.Schema,
		},
		Compatibility: "",
	}, nil
}

// schemaPayload is the request body shape for compatibility/registration.
type schemaPayload struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType"`
}

// CheckCompatibility reports whether s is compatible with subject's latest
// version. If the subject/version does not yet exist (404), it is treated as
// compatible.
//
// This is a POST, but it is read-only (the Confluent SR API evaluates
// compatibility without persisting anything), so it is retried like any other
// idempotent lookup.
func (c *Client) CheckCompatibility(ctx context.Context, subject string, s schemaregistry.Schema) (bool, error) {
	path := fmt.Sprintf("/compatibility/subjects/%s/versions/latest", esc(subject))
	body := schemaPayload{Schema: s.Definition, SchemaType: string(s.Type)}
	var out struct {
		IsCompatible bool `json:"is_compatible"`
	}
	status, err := c.doJSON(ctx, http.MethodPost, path, body, &out, true)
	if status == http.StatusNotFound {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return out.IsCompatible, nil
}

// RegisterSchema registers s under subject and returns the schema id.
//
// This is a write (it creates a new schema version) and is never retried: a
// request that reached the server but whose response was lost must not be
// blindly resent, since that could register a duplicate version.
func (c *Client) RegisterSchema(ctx context.Context, subject string, s schemaregistry.Schema) (int, error) {
	path := fmt.Sprintf("/subjects/%s/versions", esc(subject))
	body := schemaPayload{Schema: s.Definition, SchemaType: string(s.Type)}
	var out struct {
		ID int `json:"id"`
	}
	if _, err := c.do(ctx, http.MethodPost, path, registerContentType, body, &out, false); err != nil {
		return 0, err
	}
	return out.ID, nil
}

// LookupSchema checks whether s is already registered under subject and
// returns its registered version, or 0 (nil error) when it is not registered
// (HTTP 404 from POST /subjects/{subject}).
//
// This is a POST, but it is read-only (it looks up an existing schema by
// content; it registers nothing), so it is retried like any other idempotent
// lookup.
func (c *Client) LookupSchema(ctx context.Context, subject string, s schemaregistry.Schema) (int, error) {
	path := fmt.Sprintf("/subjects/%s", esc(subject))
	body := schemaPayload{Schema: s.Definition, SchemaType: string(s.Type)}
	var out struct {
		Version int `json:"version"`
	}
	status, err := c.doJSON(ctx, http.MethodPost, path, body, &out, true)
	if status == http.StatusNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return out.Version, nil
}

// GetCompatibility returns the subject-level compatibility level, or "" if
// unset (404, inherits global).
func (c *Client) GetCompatibility(ctx context.Context, subject string) (string, error) {
	path := fmt.Sprintf("/config/%s", esc(subject))
	var out struct {
		CompatibilityLevel string `json:"compatibilityLevel"`
	}
	status, err := c.doJSON(ctx, http.MethodGet, path, nil, &out, true)
	if status == http.StatusNotFound {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return out.CompatibilityLevel, nil
}

// GetGlobalCompatibility returns the registry's global compatibility level
// (GET /config), or "" if the registry reports none (404 — very old or
// unconfigured registries). Callers treat "" and errors alike as "global level
// unknown" and fall back to legacy risk classification (spec §17.1); they must
// NOT fail the run on an error here.
func (c *Client) GetGlobalCompatibility(ctx context.Context) (string, error) {
	var out struct {
		CompatibilityLevel string `json:"compatibilityLevel"`
	}
	status, err := c.doJSON(ctx, http.MethodGet, "/config", nil, &out, true)
	if status == http.StatusNotFound {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return out.CompatibilityLevel, nil
}

// SetCompatibility sets the subject-level compatibility level. A write: never
// retried.
func (c *Client) SetCompatibility(ctx context.Context, subject, level string) error {
	path := fmt.Sprintf("/config/%s", esc(subject))
	body := map[string]string{"compatibility": level}
	_, err := c.doJSON(ctx, http.MethodPut, path, body, nil, false)
	return err
}

// DeleteSubject removes subject and all its versions. A 404 is treated as
// success so the operation is idempotent at the API-semantics level, but the
// request itself is still a write and is never blindly retried by this
// client: an ambiguous transport failure must surface rather than risk
// resending a DELETE against a registry whose HA/version-handling deviates
// from the documented 404-is-success contract.
func (c *Client) DeleteSubject(ctx context.Context, subject string) error {
	path := fmt.Sprintf("/subjects/%s", esc(subject))
	status, err := c.doJSON(ctx, http.MethodDelete, path, nil, nil, false)
	if status == http.StatusNotFound {
		return nil
	}
	return err
}
