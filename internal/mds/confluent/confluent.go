// Package confluent implements mds.Client against the Confluent Metadata
// Service (MDS) Security API v1 using only the standard library. Structure
// mirrors internal/schemaregistry/confluent precisely.
package confluent

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/monedula-dev/monedula-gitops/internal/httpretry"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
)

// defaultTimeout bounds each HTTP request to the MDS.
const defaultTimeout = 30 * time.Second

// authType selects how the client authenticates.
type authType int

const (
	authNone   authType = iota
	authBasic           // Authorization: Basic base64(user:pass)
	authBearer          // Authorization: Bearer <token>
	// authMTLS: no header — the client cert on the http.Client.TLSClientConfig is the credential.
)

// Auth holds resolved plaintext credentials. The caller resolves any secret
// references (via internal/secrets) before constructing the client.
type Auth struct {
	// Type selects the auth method.
	Type     authType
	Username string // basic only
	Password string // basic only
	Token    string // bearer only
}

// BasicAuth returns an Auth for HTTP Basic authentication.
func BasicAuth(username, password string) *Auth {
	return &Auth{Type: authBasic, Username: username, Password: password}
}

// BearerAuth returns an Auth for Bearer token authentication.
func BearerAuth(token string) *Auth {
	return &Auth{Type: authBearer, Token: token}
}

// MTLSAuth returns an Auth that signals mTLS: no header is added; the TLS
// client certificate must already be configured on the *http.Client's transport.
func MTLSAuth() *Auth {
	return &Auth{Type: authNone}
}

// Client talks to the Confluent MDS Security API over HTTP.
type Client struct {
	baseURL string
	auth    *Auth
	http    *http.Client
}

// compile-time assertion that Client implements the seam.
var _ mds.Client = (*Client)(nil)

// New returns a Client for the MDS instance at endpoint. A trailing "/" is
// trimmed. httpClient, when non-nil, is used as-is (caller is responsible for
// TLS config, e.g. for mTLS); when nil, a default http.Client with a 30 s
// timeout is used (basic/bearer auth).
func New(endpoint string, auth *Auth, httpClient *http.Client) (*Client, error) {
	endpoint = strings.TrimRight(endpoint, "/")
	if endpoint == "" {
		return nil, fmt.Errorf("mds: endpoint is empty")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}
	return &Client{
		baseURL: endpoint,
		auth:    auth,
		http:    httpClient,
	}, nil
}

// mdsError is the generic MDS error envelope (non-standard; MDS returns plain
// status codes with optional JSON body).
type mdsError struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

// do is the shared request/response machinery. body is JSON-encoded when
// non-nil. out is JSON-decoded from the response when non-nil and the response
// is 2xx. Returns the HTTP status code.
//
// On a non-2xx status an error is returned including the status and, when
// present, the MDS error message. The error is safe to surface (no credentials).
//
// idempotent, when true, allows internal/httpretry to retry the request up to
// a bounded number of times on 429/502/503/504 and transport-level errors
// (connection refused/reset, EOF, ...), with jittered backoff honoring
// Retry-After when present. It must be false for any request that mutates MDS
// state (role-binding add/remove), since those are not safe to blindly resend
// after an ambiguous failure. The MDS lookup endpoints
// (/security/1.0/lookup/...) are POSTs but read-only (they look up existing
// role/principal assignments; they create nothing), so callers pass true for
// them.
func (c *Client) do(ctx context.Context, method, path string, body, out any, idempotent bool) (int, error) {
	var reqBytes []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("mds: encode request body: %w", err)
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
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
		if err != nil {
			return nil, fmt.Errorf("mds: build request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		c.applyAuth(req)
		return req, nil
	}

	resp, err := httpretry.Do(ctx, c.http, newReq, idempotent)
	if err != nil {
		// NOTE: never wrap with the URL or credentials.
		return 0, fmt.Errorf("mds: request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, fmt.Errorf("mds: read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, c.statusError(resp.StatusCode, respBody)
	}

	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return resp.StatusCode, fmt.Errorf("mds: decode response: %w", err)
		}
	}
	return resp.StatusCode, nil
}

// applyAuth adds the appropriate Authorization header, or nothing for mTLS.
func (c *Client) applyAuth(req *http.Request) {
	if c.auth == nil {
		return
	}
	switch c.auth.Type {
	case authBasic:
		raw := c.auth.Username + ":" + c.auth.Password
		req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(raw)))
	case authBearer:
		req.Header.Set("Authorization", "Bearer "+c.auth.Token)
	case authNone:
		// mTLS: client cert in transport config, no header.
	}
}

// statusError builds an error from a non-2xx response, surfacing the MDS error
// message when the body parses as the standard envelope. Safe to surface.
func (c *Client) statusError(status int, body []byte) error {
	var me mdsError
	if err := json.Unmarshal(body, &me); err == nil && me.Message != "" {
		return fmt.Errorf("mds: returned status %d: %s", status, me.Message)
	}
	// Surface first 200 bytes of raw body for debugging (no credentials expected).
	snippet := string(body)
	if len(snippet) > 200 {
		snippet = snippet[:200] + "..."
	}
	if snippet != "" {
		return fmt.Errorf("mds: returned status %d: %s", status, snippet)
	}
	return fmt.Errorf("mds: returned status %d", status)
}

// esc URL-escapes a path segment component (principal name, role name).
func esc(s string) string {
	return url.PathEscape(s)
}

// ---- Scope JSON helpers ----

// MDS API: https://docs.confluent.io/platform/current/security/rbac/mds-api.html
//
// Scope JSON shape sent in request bodies:
//
//	{
//	  "clusters": {
//	    "kafka-cluster": "<kafka-cluster-id>",
//	    "<sub>-cluster":  "<sub-cluster-id>"   // omitted for kafka scope
//	  }
//	}
//
// Sub-cluster key per scope type:
//   - schema-registry → "schema-registry-cluster"
//   - connect         → "connect-cluster"
//   - ksql            → "ksql-cluster"

// scopeClusters builds the "clusters" map for the given scope.
func scopeClusters(s mds.Scope) map[string]string {
	m := map[string]string{"kafka-cluster": s.KafkaCluster}
	switch s.Type {
	case "schema-registry":
		m["schema-registry-cluster"] = s.SubCluster
	case "connect":
		m["connect-cluster"] = s.SubCluster
	case "ksql":
		m["ksql-cluster"] = s.SubCluster
	}
	// "kafka" scope: no sub-cluster key.
	return m
}

// ---- Resource pattern JSON ----

// resourcePatternJSON is the wire shape for a resource pattern in MDS requests.
type resourcePatternJSON struct {
	ResourceType string `json:"resourceType"`
	Name         string `json:"name"`
	PatternType  string `json:"patternType"`
}

// ---- ListRoleBindings ----

// MDS API strategy: enumerate per-role then per-principal.
//
// The scope-wide single-call endpoint (POST /security/1.0/lookup/rolebindings)
// does not exist in self-hosted Confluent Platform (cp-server 7.x and 8.x
// both return 404). We use the two-step approach documented by Confluent:
//
//  1. GET /security/1.0/roles -> list of all role names.
//  2. POST /security/1.0/lookup/role/{role}
//     Body: { "clusters": { "kafka-cluster": "..." } }
//     Response: [ "User:alice", "User:bob", ... ]
//     Collects distinct principals that hold each role in the scope.
//  3. POST /security/1.0/lookup/rolebindings/principal/{principal}
//     Body: { "clusters": { "kafka-cluster": "..." } }
//     Response: { "scope": {...}, "rolebindings": { "<principal>": { "<role>": [patterns] } } }
//     Fetches all bindings for each discovered principal.
//
// Patterns for cluster-scoped roles have an empty slice []; resource-scoped
// roles have one or more { resourceType, name, patternType } entries.
// patternType is returned upper-case by MDS (e.g. "PREFIXED", "LITERAL").

// roleListResponse is the response shape of GET /security/1.0/roles.
type roleListResponse []struct {
	Name string `json:"name"`
}

// principalsResponse is the response shape of POST /security/1.0/lookup/role/{role}.
// It is a plain JSON array of principal strings.
type principalsResponse []string

// perPrincipalResponse is the response shape of
// POST /security/1.0/lookup/rolebindings/principal/{principal}.
// The rolebindings field maps principal -> role -> resource-pattern list.
type perPrincipalResponse struct {
	Rolebindings map[string]map[string][]resourcePatternJSON `json:"rolebindings"`
}

// clustersBody is the body sent to the per-role and per-principal lookup endpoints.
// Note: these endpoints accept { "clusters": {...} } (without the "scope" wrapper),
// unlike the other scope-body endpoints.
type clustersBody struct {
	Clusters map[string]string `json:"clusters"`
}

func newClustersBody(s mds.Scope) clustersBody {
	return clustersBody{Clusters: scopeClusters(s)}
}

// ListRoleBindings returns all role bindings visible within scope using the
// two-step per-role -> per-principal enumeration (compatible with self-hosted
// Confluent Platform cp-server 7.x and 8.x, which do not expose the
// scope-wide POST /security/1.0/lookup/rolebindings endpoint).
func (c *Client) ListRoleBindings(ctx context.Context, scope mds.Scope) ([]mds.RoleBinding, error) {
	// Step 1: discover all role names.
	var roles roleListResponse
	if _, err := c.do(ctx, http.MethodGet, "/security/1.0/roles", nil, &roles, true); err != nil {
		return nil, fmt.Errorf("mds.ListRoleBindings: list roles: %w", err)
	}

	// Step 2: for each role, collect principals that hold it in this scope.
	body := newClustersBody(scope)
	principals := map[string]struct{}{}
	for _, r := range roles {
		var plist principalsResponse
		// POST, but read-only: looks up which principals hold r.Name in scope
		// without mutating anything. Retried like any other idempotent lookup.
		status, err := c.do(ctx, http.MethodPost,
			"/security/1.0/lookup/role/"+esc(r.Name), body, &plist, true)
		if err != nil {
			// Skip only 404 -- the role exists but no one holds it in this
			// scope. Any other error (401/403 bad credentials, 5xx/network
			// outage, status 0 transport failure, ...) must surface: silently
			// skipping it would collect an incomplete principal set and make
			// ListRoleBindings return a fictional (too-small) live set as
			// success, so the caller could never tell an auth failure or
			// outage apart from an empty live set.
			if status == http.StatusNotFound {
				continue
			}
			return nil, fmt.Errorf("mds.ListRoleBindings: lookup principals for role %q: %w", r.Name, err)
		}
		for _, p := range plist {
			principals[p] = struct{}{}
		}
	}

	if len(principals) == 0 {
		return nil, nil
	}

	// Step 3: for each discovered principal, fetch all their role bindings.
	var out []mds.RoleBinding
	for principal := range principals {
		var resp perPrincipalResponse
		// POST, but read-only: looks up principal's existing bindings without
		// mutating anything. Retried like any other idempotent lookup.
		if _, err := c.do(ctx, http.MethodPost,
			"/security/1.0/lookup/rolebindings/principal/"+esc(principal), body, &resp, true); err != nil {
			return nil, fmt.Errorf("mds.ListRoleBindings: principal %s: %w", principal, err)
		}
		for p, roleMap := range resp.Rolebindings {
			for role, patterns := range roleMap {
				if len(patterns) == 0 {
					// Cluster-scoped binding: no resource patterns.
					out = append(out, mds.RoleBinding{
						Principal: p,
						Role:      role,
						Scope:     scope,
						Resource:  nil,
					})
				} else {
					for _, pat := range patterns {
						out = append(out, mds.RoleBinding{
							Principal: p,
							Role:      role,
							Scope:     scope,
							Resource: &mds.ResourcePattern{
								Type:        pat.ResourceType,
								Name:        pat.Name,
								PatternType: strings.ToLower(pat.PatternType),
							},
						})
					}
				}
			}
		}
	}
	return out, nil
}

// ---- AddRoleBinding ----

// AddRoleBinding adds a role binding via the MDS Security API.
//
// Resource-scoped (rb.Resource != nil):
//
//	POST /security/1.0/principals/{principal}/roles/{role}/bindings
//	Body: { "scope": { "clusters": {...} }, "resourcePatterns": [ {...} ] }
//
// Cluster-scoped (rb.Resource == nil):
//
//	POST /security/1.0/principals/{principal}/roles/{role}
//	Body: { "clusters": {...} }
//
// MDS API: https://docs.confluent.io/platform/current/security/rbac/mds-api.html
func (c *Client) AddRoleBinding(ctx context.Context, rb mds.RoleBinding) error {
	if rb.Resource != nil {
		return c.bindingsRequest(ctx, http.MethodPost, rb)
	}
	return c.clusterRoleRequest(ctx, http.MethodPost, rb)
}

// ---- RemoveRoleBinding ----

// RemoveRoleBinding removes a role binding via the MDS Security API.
//
// Resource-scoped (rb.Resource != nil):
//
//	DELETE /security/1.0/principals/{principal}/roles/{role}/bindings
//	Body: { "scope": { "clusters": {...} }, "resourcePatterns": [ {...} ] }
//
// Cluster-scoped (rb.Resource == nil):
//
//	DELETE /security/1.0/principals/{principal}/roles/{role}
//	Body: { "clusters": {...} }
//
// MDS API: https://docs.confluent.io/platform/current/security/rbac/mds-api.html
func (c *Client) RemoveRoleBinding(ctx context.Context, rb mds.RoleBinding) error {
	if rb.Resource != nil {
		return c.bindingsRequest(ctx, http.MethodDelete, rb)
	}
	return c.clusterRoleRequest(ctx, http.MethodDelete, rb)
}

// bindingsRequest performs a POST or DELETE to .../roles/{role}/bindings for a
// resource-scoped role binding. A write (adds or removes a binding): never
// retried, since resending an ambiguously-failed add/remove could
// double-apply or race a subsequent legitimate change.
func (c *Client) bindingsRequest(ctx context.Context, method string, rb mds.RoleBinding) error {
	path := fmt.Sprintf("/security/1.0/principals/%s/roles/%s/bindings",
		esc(rb.Principal), esc(rb.Role))

	body := struct {
		Scope struct {
			Clusters map[string]string `json:"clusters"`
		} `json:"scope"`
		ResourcePatterns []resourcePatternJSON `json:"resourcePatterns"`
	}{}
	body.Scope.Clusters = scopeClusters(rb.Scope)
	body.ResourcePatterns = []resourcePatternJSON{{
		ResourceType: rb.Resource.Type,
		Name:         rb.Resource.Name,
		PatternType:  strings.ToUpper(rb.Resource.PatternType),
	}}

	if _, err := c.do(ctx, method, path, body, nil, false); err != nil {
		op := "AddRoleBinding"
		if method == http.MethodDelete {
			op = "RemoveRoleBinding"
		}
		return fmt.Errorf("mds.%s: %w", op, err)
	}
	return nil
}

// clusterRoleRequest performs a POST or DELETE to .../roles/{role} for a
// cluster-scoped role binding (no resource patterns). A write: never retried
// (see bindingsRequest).
func (c *Client) clusterRoleRequest(ctx context.Context, method string, rb mds.RoleBinding) error {
	path := fmt.Sprintf("/security/1.0/principals/%s/roles/%s",
		esc(rb.Principal), esc(rb.Role))

	// Body is the scope object directly (not wrapped in "scope" key) per MDS API.
	body := struct {
		Clusters map[string]string `json:"clusters"`
	}{
		Clusters: scopeClusters(rb.Scope),
	}

	if _, err := c.do(ctx, method, path, body, nil, false); err != nil {
		op := "AddRoleBinding"
		if method == http.MethodDelete {
			op = "RemoveRoleBinding"
		}
		return fmt.Errorf("mds.%s: %w", op, err)
	}
	return nil
}
