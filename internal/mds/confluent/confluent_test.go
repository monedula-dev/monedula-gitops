package confluent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/httpretry"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
)

// noRetrySleep replaces httpretry.Sleep for the duration of a test: it
// returns immediately, so retry tests here run fast and deterministically
// instead of depending on the real clock.
func noRetrySleep(t *testing.T) {
	t.Helper()
	orig := httpretry.Sleep
	httpretry.Sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	t.Cleanup(func() { httpretry.Sleep = orig })
}

// newTestClient spins up an httptest server with the given handler and returns
// a Client pointed at it (no auth), registering cleanup.
func newTestClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	return newTestClientAuth(t, nil, handler)
}

// newTestClientAuth is like newTestClient but accepts an auth config.
func newTestClientAuth(t *testing.T, auth *Auth, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, auth, nil)
	require.NoError(t, err)
	return c
}

// kafkaScope is a helper scope for tests.
var kafkaScope = mds.Scope{
	Type:         "kafka",
	KafkaCluster: "lkc-abc123",
}

// srScope is a schema-registry scope for tests.
var srScope = mds.Scope{
	Type:         "schema-registry",
	KafkaCluster: "lkc-abc123",
	SubCluster:   "lsrc-xyz",
}

// ---- Constructor tests ----

func TestNew_TrimsTrailingSlash(t *testing.T) {
	c, err := New("http://mds.example.com/", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "http://mds.example.com", c.baseURL)
	assert.NotNil(t, c.http)
}

func TestNew_EmptyEndpointErrors(t *testing.T) {
	_, err := New("", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "endpoint is empty")
}

func TestNew_CustomHTTPClient(t *testing.T) {
	custom := &http.Client{}
	c, err := New("http://example.com", nil, custom)
	require.NoError(t, err)
	assert.Same(t, custom, c.http)
}

// ---- Auth header tests ----

// emptyListMuxWithRolesFunc builds a fresh 3-endpoint mux for ListRoleBindings
// tests that want to inspect the GET /roles call. rolesHandler replaces the
// default empty-list handler for /security/1.0/roles.
func emptyListMuxWithRolesFunc(rolesHandler http.HandlerFunc) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", rolesHandler)
	mux.HandleFunc("/security/1.0/lookup/role/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	mux.HandleFunc("/security/1.0/lookup/rolebindings/principal/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})
	return mux
}

func TestBasicAuth_SentWhenConfigured(t *testing.T) {
	auth := BasicAuth("alice", "s3cr3t")
	var gotAuth bool
	mux := emptyListMuxWithRolesFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		assert.True(t, ok, "expected Basic auth header")
		assert.Equal(t, "alice", user)
		assert.Equal(t, "s3cr3t", pass)
		gotAuth = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, auth, nil)
	require.NoError(t, err)
	_, err = c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	assert.True(t, gotAuth)
}

func TestBearerAuth_SentWhenConfigured(t *testing.T) {
	auth := BearerAuth("tok-123")
	var gotAuth bool
	mux := emptyListMuxWithRolesFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer tok-123", r.Header.Get("Authorization"))
		gotAuth = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, auth, nil)
	require.NoError(t, err)
	_, err = c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	assert.True(t, gotAuth)
}

func TestMTLSAuth_NoAuthHeaderSent(t *testing.T) {
	auth := MTLSAuth()
	var gotCall bool
	mux := emptyListMuxWithRolesFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Authorization"), "mTLS must not send an auth header")
		gotCall = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, auth, nil)
	require.NoError(t, err)
	_, err = c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	assert.True(t, gotCall)
}

func TestNoAuth_NoAuthHeaderSent(t *testing.T) {
	var gotCall bool
	mux := emptyListMuxWithRolesFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Authorization"))
		gotCall = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, nil, nil)
	require.NoError(t, err)
	_, err = c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	assert.True(t, gotCall)
}

// ---- ListRoleBindings tests ----
//
// The new implementation makes 3 request types:
//   GET  /security/1.0/roles                                     -> role name list
//   POST /security/1.0/lookup/role/{role}                        -> principals per role
//   POST /security/1.0/lookup/rolebindings/principal/{principal} -> bindings per principal
//
// Tests use newMuxTestClient (mux-based handler) for assertions on multiple
// request types, and newTestClient (single handler) for error-path tests where
// only the first request (GET /roles) matters.

// newMuxTestClient creates a test client routed by an http.ServeMux.
func newMuxTestClient(t *testing.T, mux *http.ServeMux) *Client {
	t.Helper()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, nil, nil)
	require.NoError(t, err)
	return c
}

// muxForList builds a ServeMux that serves a well-formed 3-step response:
//   - GET /security/1.0/roles    -> rolesJSON (JSON array of {"name":"..."})
//   - POST /security/1.0/lookup/role/{role} -> principalsForRole(role)
//   - POST /security/1.0/lookup/rolebindings/principal/{principal} -> bindingsForPrincipal(principal)
func muxForList(
	t *testing.T,
	rolesJSON []byte,
	principalsForRole func(role string) []byte,
	bindingsForPrincipal func(principal string) []byte,
) *http.ServeMux {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(rolesJSON)
	})
	mux.HandleFunc("POST /security/1.0/lookup/role/", func(w http.ResponseWriter, r *http.Request) {
		role := r.PathValue("role")
		if role == "" {
			// Go 1.21 path value extraction; fallback for older routers.
			role = r.URL.Path[len("/security/1.0/lookup/role/"):]
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(principalsForRole(role))
	})
	mux.HandleFunc("POST /security/1.0/lookup/rolebindings/principal/", func(w http.ResponseWriter, r *http.Request) {
		principal := r.PathValue("principal")
		if principal == "" {
			principal = r.URL.Path[len("/security/1.0/lookup/rolebindings/principal/"):]
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bindingsForPrincipal(principal))
	})
	return mux
}

// simpleListMux builds a 3-step mux that simulates a cluster with the given
// bindings (keyed by principal). Roles list is derived from the binding map.
// This is the primary helper for most ListRoleBindings tests.
func simpleListMux(t *testing.T, bindings map[string]map[string][]resourcePatternJSON) *http.ServeMux {
	t.Helper()
	// Collect unique roles and map principal->bindings.
	roles := map[string]bool{}
	for _, rm := range bindings {
		for r := range rm {
			roles[r] = true
		}
	}
	rolesArr := []map[string]string{}
	for r := range roles {
		rolesArr = append(rolesArr, map[string]string{"name": r})
	}
	rolesJSON, _ := json.Marshal(rolesArr)

	// principalsForRole: return principals that have this role.
	principalsForRole := func(role string) []byte {
		var ps []string
		for p, rm := range bindings {
			if _, ok := rm[role]; ok {
				ps = append(ps, p)
			}
		}
		b, _ := json.Marshal(ps)
		return b
	}

	// bindingsForPrincipal: return perPrincipalResponse for this principal.
	bindingsForPrincipal := func(principal string) []byte {
		rm, ok := bindings[principal]
		if !ok {
			b, _ := json.Marshal(perPrincipalResponse{})
			return b
		}
		resp := perPrincipalResponse{
			Rolebindings: map[string]map[string][]resourcePatternJSON{
				principal: rm,
			},
		}
		b, _ := json.Marshal(resp)
		return b
	}

	return muxForList(t, rolesJSON, principalsForRole, bindingsForPrincipal)
}

func TestListRoleBindings_MethodAndPath(t *testing.T) {
	// Verify that the first request is GET /security/1.0/roles.
	gotRolesCall := false
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gotRolesCall = true
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	mux.HandleFunc("/security/1.0/lookup/role/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	mux.HandleFunc("/security/1.0/lookup/rolebindings/principal/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})
	c := newMuxTestClient(t, mux)
	_, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	assert.True(t, gotRolesCall, "expected GET /security/1.0/roles to be called")
}

func TestListRoleBindings_ScopeBody_KafkaScope(t *testing.T) {
	// Verify that per-role and per-principal requests include kafka-cluster in body.
	var gotClusters map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"name":"DeveloperRead"}]`)
	})
	mux.HandleFunc("/security/1.0/lookup/role/", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotClusters, _ = body["clusters"].(map[string]any)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	mux.HandleFunc("/security/1.0/lookup/rolebindings/principal/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})
	c := newMuxTestClient(t, mux)
	_, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	require.NotNil(t, gotClusters)
	assert.Equal(t, "lkc-abc123", gotClusters["kafka-cluster"])
	assert.NotContains(t, gotClusters, "schema-registry-cluster")
}

func TestListRoleBindings_ScopeBody_SRScope(t *testing.T) {
	var gotClusters map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"name":"ResourceOwner"}]`)
	})
	mux.HandleFunc("/security/1.0/lookup/role/", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotClusters, _ = body["clusters"].(map[string]any)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	mux.HandleFunc("/security/1.0/lookup/rolebindings/principal/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})
	c := newMuxTestClient(t, mux)
	_, err := c.ListRoleBindings(context.Background(), srScope)
	require.NoError(t, err)
	require.NotNil(t, gotClusters)
	assert.Equal(t, "lkc-abc123", gotClusters["kafka-cluster"])
	assert.Equal(t, "lsrc-xyz", gotClusters["schema-registry-cluster"])
}

func TestListRoleBindings_ScopeBody_ConnectScope(t *testing.T) {
	scope := mds.Scope{Type: "connect", KafkaCluster: "lkc-1", SubCluster: "connect-2"}
	var gotClusters map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"name":"DeveloperRead"}]`)
	})
	mux.HandleFunc("/security/1.0/lookup/role/", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotClusters, _ = body["clusters"].(map[string]any)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	mux.HandleFunc("/security/1.0/lookup/rolebindings/principal/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})
	c := newMuxTestClient(t, mux)
	_, err := c.ListRoleBindings(context.Background(), scope)
	require.NoError(t, err)
	require.NotNil(t, gotClusters)
	assert.Equal(t, "connect-2", gotClusters["connect-cluster"])
}

func TestListRoleBindings_ScopeBody_KsqlScope(t *testing.T) {
	scope := mds.Scope{Type: "ksql", KafkaCluster: "lkc-1", SubCluster: "ksql-3"}
	var gotClusters map[string]any
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"name":"DeveloperRead"}]`)
	})
	mux.HandleFunc("/security/1.0/lookup/role/", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		gotClusters, _ = body["clusters"].(map[string]any)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	mux.HandleFunc("/security/1.0/lookup/rolebindings/principal/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})
	c := newMuxTestClient(t, mux)
	_, err := c.ListRoleBindings(context.Background(), scope)
	require.NoError(t, err)
	require.NotNil(t, gotClusters)
	assert.Equal(t, "ksql-3", gotClusters["ksql-cluster"])
}

func TestListRoleBindings_MapsResourceScopedBinding(t *testing.T) {
	// Mock MDS returns uppercase "LITERAL" (as real MDS does); client must
	// normalise to lowercase before returning.
	bindings := map[string]map[string][]resourcePatternJSON{
		"User:alice": {
			"DeveloperRead": {{ResourceType: "Topic", Name: "payments.orders", PatternType: "LITERAL"}},
		},
	}
	c := newMuxTestClient(t, simpleListMux(t, bindings))
	rbs, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	require.Len(t, rbs, 1)
	rb := rbs[0]
	assert.Equal(t, "User:alice", rb.Principal)
	assert.Equal(t, "DeveloperRead", rb.Role)
	assert.Equal(t, kafkaScope, rb.Scope)
	require.NotNil(t, rb.Resource)
	assert.Equal(t, "Topic", rb.Resource.Type)
	assert.Equal(t, "payments.orders", rb.Resource.Name)
	// MDS returns uppercase; client must lower-case it.
	assert.Equal(t, "literal", rb.Resource.PatternType)
}

func TestListRoleBindings_MapsClusterScopedBinding(t *testing.T) {
	// Cluster-scoped: role maps to nil/empty patterns slice.
	bindings := map[string]map[string][]resourcePatternJSON{
		"User:bob": {
			"ClusterAdmin": nil,
		},
	}
	c := newMuxTestClient(t, simpleListMux(t, bindings))
	rbs, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	require.Len(t, rbs, 1)
	rb := rbs[0]
	assert.Equal(t, "User:bob", rb.Principal)
	assert.Equal(t, "ClusterAdmin", rb.Role)
	assert.Nil(t, rb.Resource)
}

func TestListRoleBindings_MultipleBindings(t *testing.T) {
	bindings := map[string]map[string][]resourcePatternJSON{
		"User:alice": {
			"DeveloperRead":  {{ResourceType: "Topic", Name: "payments.*", PatternType: "prefixed"}},
			"DeveloperWrite": {{ResourceType: "Topic", Name: "payments.orders", PatternType: "literal"}},
		},
		"User:bob": {
			"SecurityAdmin": nil,
		},
	}
	c := newMuxTestClient(t, simpleListMux(t, bindings))
	rbs, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	assert.Len(t, rbs, 3)
	sort.Slice(rbs, func(i, j int) bool { return rbs[i].Key() < rbs[j].Key() })
	principals := make(map[string]bool)
	for _, rb := range rbs {
		principals[rb.Principal] = true
	}
	assert.True(t, principals["User:alice"])
	assert.True(t, principals["User:bob"])
}

func TestListRoleBindings_EmptyResponse(t *testing.T) {
	// No bindings -> roles list is empty -> no per-role calls -> nil result.
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	mux.HandleFunc("/security/1.0/lookup/role/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	mux.HandleFunc("/security/1.0/lookup/rolebindings/principal/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})
	c := newMuxTestClient(t, mux)
	rbs, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	assert.Empty(t, rbs)
}

func TestListRoleBindings_NonTwoXX_ReturnsError(t *testing.T) {
	// 401 on GET /roles -> error propagated.
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error_code":401,"message":"Unauthorized"}`)
	})
	_, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
	assert.Contains(t, err.Error(), "Unauthorized")
}

func TestListRoleBindings_ServerError_IncludesStatusAndSnippet(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error_code":500,"message":"internal server error"}`)
	})
	_, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

func TestListRoleBindings_PerRole404Skip(t *testing.T) {
	// When one role's POST /lookup/role/{role} returns 404, enumeration must
	// continue and still collect bindings from roles that do resolve.
	//
	// Setup:
	//   GET  /security/1.0/roles                      -> [DeveloperRead, ResourceOwner]
	//   POST /security/1.0/lookup/role/DeveloperRead  -> 404 (no principal holds it)
	//   POST /security/1.0/lookup/role/ResourceOwner  -> ["User:svc-x"]
	//   POST /lookup/rolebindings/principal/User:svc-x -> one binding
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"name":"DeveloperRead"},{"name":"ResourceOwner"}]`)
	})
	mux.HandleFunc("/security/1.0/lookup/role/", func(w http.ResponseWriter, r *http.Request) {
		role := r.URL.Path[len("/security/1.0/lookup/role/"):]
		switch role {
		case "DeveloperRead":
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error_code":404,"message":"role not found"}`)
		case "ResourceOwner":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `["User:svc-x"]`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	mux.HandleFunc("/security/1.0/lookup/rolebindings/principal/", func(w http.ResponseWriter, r *http.Request) {
		principal := r.URL.Path[len("/security/1.0/lookup/rolebindings/principal/"):]
		if principal == "User:svc-x" {
			resp := perPrincipalResponse{
				Rolebindings: map[string]map[string][]resourcePatternJSON{
					"User:svc-x": {
						"ResourceOwner": {{ResourceType: "Topic", Name: "orders.*", PatternType: "PREFIXED"}},
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})

	c := newMuxTestClient(t, mux)
	rbs, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err, "404 on one role must not abort enumeration")
	require.Len(t, rbs, 1, "expected one binding from the surviving role")
	rb := rbs[0]
	assert.Equal(t, "User:svc-x", rb.Principal)
	assert.Equal(t, "ResourceOwner", rb.Role)
	require.NotNil(t, rb.Resource)
	assert.Equal(t, "Topic", rb.Resource.Type)
	assert.Equal(t, "orders.*", rb.Resource.Name)
	assert.Equal(t, "prefixed", rb.Resource.PatternType)
}

func TestListRoleBindings_PerRoleErrorPropagates(t *testing.T) {
	// A 403 on POST /lookup/role/{role} must abort enumeration with an error
	// naming the role -- NOT be silently skipped like a 404. Skipping it would
	// make ListRoleBindings return an incomplete (or empty) live set as
	// success, which is the worst failure mode for a GitOps diff.
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"name":"DeveloperRead"},{"name":"ResourceOwner"}]`)
	})
	mux.HandleFunc("/security/1.0/lookup/role/", func(w http.ResponseWriter, r *http.Request) {
		role := r.URL.Path[len("/security/1.0/lookup/role/"):]
		switch role {
		case "DeveloperRead":
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error_code":403,"message":"Forbidden"}`)
		case "ResourceOwner":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `["User:svc-x"]`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	mux.HandleFunc("/security/1.0/lookup/rolebindings/principal/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})

	c := newMuxTestClient(t, mux)
	rbs, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.Error(t, err, "403 on one role must abort enumeration, not be skipped like a 404")
	assert.Contains(t, err.Error(), "DeveloperRead")
	assert.Contains(t, err.Error(), "403")
	assert.Nil(t, rbs)
}

func TestListRoleBindings_PerRoleServerErrorPropagates(t *testing.T) {
	// A 500 on POST /lookup/role/{role} must also abort enumeration with an
	// error naming the role, not be treated as a benign "no one holds it".
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"name":"DeveloperRead"}]`)
	})
	mux.HandleFunc("/security/1.0/lookup/role/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"error_code":500,"message":"internal server error"}`)
	})
	mux.HandleFunc("/security/1.0/lookup/rolebindings/principal/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	})

	c := newMuxTestClient(t, mux)
	rbs, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DeveloperRead")
	assert.Contains(t, err.Error(), "500")
	assert.Nil(t, rbs)
}

// ---- AddRoleBinding tests ----

func TestAddRoleBinding_ResourceScoped_MethodAndPath(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     kafkaScope,
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "payments.orders", PatternType: "literal"},
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		// Colon is a valid path-segment character (RFC 3986 §3.3); Go's
		// url.PathEscape preserves it, so the server sees "User:alice" not
		// "User%3Aalice". MDS accepts the unencoded form.
		assert.Equal(t, "/security/1.0/principals/User:alice/roles/DeveloperRead/bindings", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	})
	require.NoError(t, c.AddRoleBinding(context.Background(), rb))
}

func TestAddRoleBinding_ResourceScoped_Body(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     kafkaScope,
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "payments.orders", PatternType: "literal"},
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		// Check scope
		clusters := body["scope"].(map[string]any)["clusters"].(map[string]any)
		assert.Equal(t, "lkc-abc123", clusters["kafka-cluster"])
		// Check resourcePatterns
		patterns := body["resourcePatterns"].([]any)
		require.Len(t, patterns, 1)
		p := patterns[0].(map[string]any)
		assert.Equal(t, "Topic", p["resourceType"])
		assert.Equal(t, "payments.orders", p["name"])
		assert.Equal(t, "LITERAL", p["patternType"]) // MDS requires uppercase patternType
		w.WriteHeader(http.StatusOK)
	})
	require.NoError(t, c.AddRoleBinding(context.Background(), rb))
}

func TestAddRoleBinding_ClusterScoped_MethodAndPath(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:bob",
		Role:      "ClusterAdmin",
		Scope:     kafkaScope,
		Resource:  nil,
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/security/1.0/principals/User:bob/roles/ClusterAdmin", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	})
	require.NoError(t, c.AddRoleBinding(context.Background(), rb))
}

func TestAddRoleBinding_ClusterScoped_Body(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:bob",
		Role:      "ClusterAdmin",
		Scope:     kafkaScope,
		Resource:  nil,
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		// Cluster-scoped: body is { "clusters": {...} } (no "scope" wrapper).
		clusters := body["clusters"].(map[string]any)
		assert.Equal(t, "lkc-abc123", clusters["kafka-cluster"])
		w.WriteHeader(http.StatusOK)
	})
	require.NoError(t, c.AddRoleBinding(context.Background(), rb))
}

func TestAddRoleBinding_ClusterScoped_SRScope_Body(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:svc",
		Role:      "ResourceOwner",
		Scope:     srScope,
		Resource:  nil,
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		clusters := body["clusters"].(map[string]any)
		assert.Equal(t, "lkc-abc123", clusters["kafka-cluster"])
		assert.Equal(t, "lsrc-xyz", clusters["schema-registry-cluster"])
		w.WriteHeader(http.StatusOK)
	})
	require.NoError(t, c.AddRoleBinding(context.Background(), rb))
}

func TestAddRoleBinding_NonTwoXX_ReturnsError(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:alice", Role: "DeveloperRead", Scope: kafkaScope,
		Resource: &mds.ResourcePattern{Type: "Topic", Name: "t", PatternType: "literal"},
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error_code":403,"message":"Forbidden"}`)
	})
	err := c.AddRoleBinding(context.Background(), rb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "403")
}

// ---- RemoveRoleBinding tests ----

func TestRemoveRoleBinding_ResourceScoped_MethodAndPath(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     kafkaScope,
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "payments.orders", PatternType: "literal"},
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/security/1.0/principals/User:alice/roles/DeveloperRead/bindings", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	})
	require.NoError(t, c.RemoveRoleBinding(context.Background(), rb))
}

func TestRemoveRoleBinding_ClusterScoped_MethodAndPath(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:bob",
		Role:      "ClusterAdmin",
		Scope:     kafkaScope,
		Resource:  nil,
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodDelete, r.Method)
		assert.Equal(t, "/security/1.0/principals/User:bob/roles/ClusterAdmin", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	})
	require.NoError(t, c.RemoveRoleBinding(context.Background(), rb))
}

func TestRemoveRoleBinding_ClusterScoped_Body(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:bob",
		Role:      "ClusterAdmin",
		Scope:     kafkaScope,
		Resource:  nil,
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		// Cluster-scoped DELETE: body is { "clusters": {...} } (no "scope" wrapper,
		// no resourcePatterns) — mirrors AddRoleBinding cluster-scoped body format.
		clusters := body["clusters"].(map[string]any)
		assert.Equal(t, "lkc-abc123", clusters["kafka-cluster"])
		assert.NotContains(t, body, "scope")
		assert.NotContains(t, body, "resourcePatterns")
		w.WriteHeader(http.StatusOK)
	})
	require.NoError(t, c.RemoveRoleBinding(context.Background(), rb))
}

func TestRemoveRoleBinding_ResourceScoped_Body(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperWrite",
		Scope:     kafkaScope,
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "orders.*", PatternType: "prefixed"},
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		patterns := body["resourcePatterns"].([]any)
		require.Len(t, patterns, 1)
		p := patterns[0].(map[string]any)
		assert.Equal(t, "Topic", p["resourceType"])
		assert.Equal(t, "orders.*", p["name"])
		assert.Equal(t, "PREFIXED", p["patternType"]) // MDS requires uppercase patternType
		w.WriteHeader(http.StatusOK)
	})
	require.NoError(t, c.RemoveRoleBinding(context.Background(), rb))
}

func TestRemoveRoleBinding_NonTwoXX_ReturnsError(t *testing.T) {
	rb := mds.RoleBinding{
		Principal: "User:alice", Role: "DeveloperRead", Scope: kafkaScope,
		Resource: &mds.ResourcePattern{Type: "Topic", Name: "t", PatternType: "literal"},
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"message":"not authenticated"}`)
	})
	err := c.RemoveRoleBinding(context.Background(), rb)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

// ---- Principal / role path escaping ----

func TestPathEscaping_PrincipalWithColon(t *testing.T) {
	// Colon is a valid path-segment character (RFC 3986 §3.3) and is NOT
	// percent-encoded by url.PathEscape when embedded in a path segment. MDS
	// uses "User:alice" format natively — the colon is preserved as-is on the
	// wire, which is correct and expected by the MDS server.
	rb := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     kafkaScope,
		Resource:  nil,
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.URL.Path, "User:alice")
		w.WriteHeader(http.StatusOK)
	})
	require.NoError(t, c.AddRoleBinding(context.Background(), rb))
}

func TestPathEscaping_PrincipalWithSlash(t *testing.T) {
	// Slashes in principal names MUST be percent-encoded so they are not
	// mistaken for path separators.
	rb := mds.RoleBinding{
		Principal: "Group:team/payments",
		Role:      "DeveloperRead",
		Scope:     kafkaScope,
		Resource:  nil,
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		// The slash in "team/payments" must be encoded as %2F.
		assert.Contains(t, r.URL.EscapedPath(), "team%2Fpayments")
		w.WriteHeader(http.StatusOK)
	})
	require.NoError(t, c.AddRoleBinding(context.Background(), rb))
}

// ---- statusError snippet truncation ----

func TestStatusError_LongBodyTruncated(t *testing.T) {
	noRetrySleep(t) // 502 is retryable; keep the test fast/deterministic.
	longBody := make([]byte, 500)
	for i := range longBody {
		longBody[i] = 'x'
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write(longBody)
	})
	_, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "502")
	// Error message must not be longer than body snippet + overhead.
	assert.Less(t, len(err.Error()), 300, "error message should truncate long bodies")
}

// ---- Retry wiring (internal/httpretry) ----

func TestListRoleBindings_RetriesOn503ThenSucceeds(t *testing.T) {
	noRetrySleep(t)
	var hits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	c := newMuxTestClient(t, mux)
	rbs, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	assert.Empty(t, rbs)
	assert.Equal(t, int32(2), hits.Load(), "expected exactly 2 attempts (503 then 200)")
}

func TestListRoleBindings_PersistentServiceUnavailable_FailsAfterThreeAttempts(t *testing.T) {
	noRetrySleep(t)
	var hits atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	_, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.Error(t, err)
	assert.Equal(t, int32(3), hits.Load(), "expected exactly 3 attempts total")
}

func TestListRoleBindings_401NotRetried(t *testing.T) {
	noRetrySleep(t)
	var hits atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	})
	_, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.Error(t, err)
	assert.Equal(t, int32(1), hits.Load(), "401 must not be retried")
}

func TestLookupRolePrincipals_RetriedAsIdempotentPOST(t *testing.T) {
	// POST /security/1.0/lookup/role/{role} is semantically a read (looks up
	// principals holding a role); it must be retried like any GET.
	noRetrySleep(t)
	var roleHits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[{"name":"DeveloperRead"}]`)
	})
	mux.HandleFunc("/security/1.0/lookup/role/DeveloperRead", func(w http.ResponseWriter, r *http.Request) {
		if roleHits.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	c := newMuxTestClient(t, mux)
	rbs, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	assert.Empty(t, rbs)
	assert.Equal(t, int32(2), roleHits.Load(), "expected the lookup/role POST to be retried once after a 503")
}

func TestAddRoleBinding_WriteNeverRetriedOn503(t *testing.T) {
	noRetrySleep(t)
	var hits atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	rb := mds.RoleBinding{Principal: "User:alice", Role: "ClusterAdmin", Scope: kafkaScope}
	err := c.AddRoleBinding(context.Background(), rb)
	require.Error(t, err)
	assert.Equal(t, int32(1), hits.Load(), "a write (AddRoleBinding) must never be retried, even on 503")
}

func TestRemoveRoleBinding_WriteNeverRetriedOn503(t *testing.T) {
	noRetrySleep(t)
	var hits atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	rb := mds.RoleBinding{Principal: "User:alice", Role: "ClusterAdmin", Scope: kafkaScope}
	err := c.RemoveRoleBinding(context.Background(), rb)
	require.Error(t, err)
	assert.Equal(t, int32(1), hits.Load(), "a write (RemoveRoleBinding) must never be retried, even on 503")
}

func TestListRoleBindings_CtxCancelDuringBackoffReturnsPromptly(t *testing.T) {
	// Deliberately real backoff here (no noRetrySleep) to prove cancellation
	// during the sleep itself returns promptly rather than waiting it out.
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := c.ListRoleBindings(ctx, kafkaScope)
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.Less(t, elapsed, 2*time.Second, "ctx cancellation during backoff must return promptly")
}

func TestListRoleBindings_RetryAfterHonored(t *testing.T) {
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
	mux := http.NewServeMux()
	mux.HandleFunc("/security/1.0/roles", func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "5")
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	})
	c := newMuxTestClient(t, mux)
	_, err := c.ListRoleBindings(context.Background(), kafkaScope)
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, gotDelay, "Retry-After: 5 must be honored")
}

// ---- Compile-time interface assertion ----
var _ mds.Client = (*Client)(nil)

// ---- helpers ----
