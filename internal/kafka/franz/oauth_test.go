package franz

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// tokenServer returns a test HTTP server acting as a token endpoint.
// When statusCode == http.StatusOK it returns a well-formed token response;
// otherwise it returns {"error":"unauthorized_client"} with the given status.
// The returned *atomic.Int64 counts how many requests the server has received.
func tokenServer(t *testing.T, accessToken string, expiresIn int, statusCode int) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if statusCode != http.StatusOK {
			w.WriteHeader(statusCode)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized_client"})
			return
		}
		resp := map[string]any{
			"access_token": accessToken,
			"token_type":   "Bearer",
		}
		if expiresIn > 0 {
			resp["expires_in"] = expiresIn
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

// oauthAuth builds an AuthConfig for OAUTHBEARER pointing at the given token endpoint.
func oauthAuth(tokenEndpoint, clientIDEnv, clientSecretEnv string) *v1alpha1.AuthConfig {
	return &v1alpha1.AuthConfig{
		Mechanism: "OAUTHBEARER",
		OAuth: &v1alpha1.OAuthConfig{
			TokenEndpoint: tokenEndpoint,
			ClientID:      v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: clientIDEnv}},
			ClientSecret:  v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: clientSecretEnv}},
		},
	}
}

// oauthAuthWithScope is like oauthAuth but also sets a scope.
func oauthAuthWithScope(tokenEndpoint, clientIDEnv, clientSecretEnv, scope string) *v1alpha1.AuthConfig {
	a := oauthAuth(tokenEndpoint, clientIDEnv, clientSecretEnv)
	a.OAuth.Scope = scope
	return a
}

// extractCredentials parses Basic auth header or form params from the request
// and returns clientID, clientSecret. oauth2 tries HTTP Basic first
// (AuthStyleInHeader); this helper accepts either form to avoid fragility.
func extractCredentials(r *http.Request) (clientID, clientSecret string, err error) {
	if id, secret, ok := r.BasicAuth(); ok {
		// oauth2 URL-encodes the values when using HTTP Basic auth.
		id, _ = url.QueryUnescape(id)
		secret, _ = url.QueryUnescape(secret)
		return id, secret, nil
	}
	if parseErr := r.ParseForm(); parseErr != nil {
		return "", "", fmt.Errorf("parsing form: %w", parseErr)
	}
	return r.FormValue("client_id"), r.FormValue("client_secret"), nil
}

// TestBuildOAuthBearer_HappyPath verifies that:
//   - buildConnConfig succeeds and produces a non-nil SASL mechanism,
//   - the token endpoint receives grant_type=client_credentials,
//   - the configured clientID/clientSecret are sent (via Basic or form params),
//   - the SASL init bytes contain the access token served by the endpoint.
func TestBuildOAuthBearer_HappyPath(t *testing.T) {
	const (
		wantClientID     = "my-client"
		wantClientSecret = "my-secret"
		wantAccessToken  = "tok-abc123"
	)

	var (
		receivedGrantType    string
		receivedClientID     string
		receivedClientSecret string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		receivedGrantType = r.FormValue("grant_type")
		var err error
		receivedClientID, receivedClientSecret, err = extractCredentials(r)
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": wantAccessToken,
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	}))
	t.Cleanup(srv.Close)

	t.Setenv("OAUTH_CLIENT_ID", wantClientID)
	t.Setenv("OAUTH_CLIENT_SECRET", wantClientSecret)

	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		Auth:             oauthAuth(srv.URL+"/token", "OAUTH_CLIENT_ID", "OAUTH_CLIENT_SECRET"),
	}}

	cc, err := buildConnConfig(c, envResolver)
	if err != nil {
		t.Fatalf("buildConnConfig: %v", err)
	}
	if cc.sasl == nil {
		t.Fatal("sasl = nil, want non-nil OAUTHBEARER mechanism")
	}

	// Drive the SASL mechanism; this triggers the oauth2 token fetch.
	_, initBytes, err := cc.sasl.Authenticate(t.Context(), "broker:9092")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}

	if receivedGrantType != "client_credentials" {
		t.Errorf("grant_type = %q, want %q", receivedGrantType, "client_credentials")
	}
	if receivedClientID != wantClientID {
		t.Errorf("clientID = %q, want %q", receivedClientID, wantClientID)
	}
	if receivedClientSecret != wantClientSecret {
		t.Errorf("clientSecret = %q, want %q", receivedClientSecret, wantClientSecret)
	}

	// The SASL init message (RFC 7628 §3.1) must embed the access token.
	if !strings.Contains(string(initBytes), wantAccessToken) {
		t.Errorf("SASL init bytes do not contain access token %q: %q", wantAccessToken, initBytes)
	}
}

// TestBuildOAuthBearer_ScopePassedThrough verifies that auth.oauth.scope is
// sent in the token request.
func TestBuildOAuthBearer_ScopePassedThrough(t *testing.T) {
	const wantScope = "kafka"
	var receivedScope string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		receivedScope = r.FormValue("scope")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "tok-scope",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	}))
	t.Cleanup(srv.Close)

	t.Setenv("OAUTH_CID_SCOPE", "c1")
	t.Setenv("OAUTH_CSEC_SCOPE", "s1")

	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		Auth:             oauthAuthWithScope(srv.URL+"/token", "OAUTH_CID_SCOPE", "OAUTH_CSEC_SCOPE", wantScope),
	}}

	cc, err := buildConnConfig(c, envResolver)
	if err != nil {
		t.Fatalf("buildConnConfig: %v", err)
	}
	if _, _, err := cc.sasl.Authenticate(t.Context(), "broker:9092"); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if receivedScope != wantScope {
		t.Errorf("scope = %q, want %q", receivedScope, wantScope)
	}
}

// TestBuildOAuthBearer_TokenExpiry verifies that after a token has expired the
// auth fn triggers a second request to the token endpoint
// (oauth2.ReuseTokenSource refresh).
//
// We issue a token with expires_in=1 (1 second). golang.org/x/oauth2 considers
// a token expired when now > expiry - defaultExpiryDelta (10s). With
// expires_in=1 the expiry is time.Now()+1s, so expiry-10s = time.Now()-9s,
// which is already in the past — meaning every call triggers a fresh fetch.
func TestBuildOAuthBearer_TokenExpiry(t *testing.T) {
	// expiresIn=1: expires immediately because oauth2's defaultExpiryDelta (10s)
	// exceeds the 1-second lifetime, so the token is stale on arrival.
	srv, hits := tokenServer(t, "tok-refresh", 1 /* expires_in=1 → stale immediately */, http.StatusOK)

	t.Setenv("OAUTH_CID_EXP", "c2")
	t.Setenv("OAUTH_CSEC_EXP", "s2")

	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		Auth:             oauthAuth(srv.URL+"/token", "OAUTH_CID_EXP", "OAUTH_CSEC_EXP"),
	}}

	cc, err := buildConnConfig(c, envResolver)
	if err != nil {
		t.Fatalf("buildConnConfig: %v", err)
	}

	// First Authenticate: fetches a token.
	if _, _, err := cc.sasl.Authenticate(t.Context(), "broker:9092"); err != nil {
		t.Fatalf("first Authenticate: %v", err)
	}
	// Second Authenticate: zero-expiry token is stale → must fetch again.
	if _, _, err := cc.sasl.Authenticate(t.Context(), "broker:9092"); err != nil {
		t.Fatalf("second Authenticate: %v", err)
	}

	if n := hits.Load(); n < 2 {
		t.Errorf("token endpoint hit %d times, want >= 2 (zero-expiry token re-fetched)", n)
	}
}

// TestBuildOAuthBearer_EndpointReturns401_ErrorDoesNotLeakSecret verifies that
// when the token endpoint returns HTTP 401 the error message does NOT contain
// the client secret value.
func TestBuildOAuthBearer_EndpointReturns401_ErrorDoesNotLeakSecret(t *testing.T) {
	const clientSecret = "ultra-secret-must-not-appear-in-error"
	srv, _ := tokenServer(t, "", 0, http.StatusUnauthorized)

	t.Setenv("OAUTH_CID_401", "c3")
	t.Setenv("OAUTH_CSEC_401", clientSecret)

	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		Auth:             oauthAuth(srv.URL+"/token", "OAUTH_CID_401", "OAUTH_CSEC_401"),
	}}

	cc, err := buildConnConfig(c, envResolver)
	if err != nil {
		t.Fatalf("buildConnConfig: %v", err)
	}

	_, _, authErr := cc.sasl.Authenticate(t.Context(), "broker:9092")
	if authErr == nil {
		t.Fatal("expected error from 401 endpoint, got nil")
	}

	errMsg := authErr.Error()
	if strings.Contains(errMsg, clientSecret) {
		t.Errorf("error message leaks client secret; error: %q", errMsg)
	}
	t.Logf("error message (no secret): %v", authErr)
}

// TestBuildOAuthBearer_TokenFetchIsTimeoutBounded verifies that the HTTP
// client used to fetch the OAUTHBEARER token enforces oauthTokenTimeout: a
// token endpoint that hangs well past the timeout must cause Authenticate to
// fail with a deadline/timeout error in roughly oauthTokenTimeout, not hang
// indefinitely (regression guard for the missing-deadline bug, where the
// token source used http.DefaultClient with no timeout at all).
//
// oauthTokenTimeout is a package-level var specifically so this test can
// shorten it; the production value (set in production code, unmodified here)
// is 10s.
func TestBuildOAuthBearer_TokenFetchIsTimeoutBounded(t *testing.T) {
	const testTimeout = 200 * time.Millisecond

	orig := oauthTokenTimeout
	oauthTokenTimeout = testTimeout
	t.Cleanup(func() { oauthTokenTimeout = orig })

	// The token endpoint sleeps far longer than the test timeout, simulating a
	// hung/unreachable IdP. sync.Once guards the close: the handler may run
	// more than once (e.g. retried connections) and each invocation's
	// r.Context() is independently cancelable, so without it a second
	// unblocked handler would double-close the channel.
	var closeOnce sync.Once
	blockUntilClientGone := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			// Client (our *http.Client with Timeout) gave up; unblock the handler
			// so httptest.Server can shut down cleanly.
		case <-time.After(10 * testTimeout):
		}
		closeOnce.Do(func() { close(blockUntilClientGone) })
	}))
	t.Cleanup(srv.Close)

	t.Setenv("OAUTH_CID_TIMEOUT", "c-timeout")
	t.Setenv("OAUTH_CSEC_TIMEOUT", "s-timeout")

	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		Auth:             oauthAuth(srv.URL+"/token", "OAUTH_CID_TIMEOUT", "OAUTH_CSEC_TIMEOUT"),
	}}

	cc, err := buildConnConfig(c, envResolver)
	if err != nil {
		t.Fatalf("buildConnConfig: %v", err)
	}

	start := time.Now()
	_, _, authErr := cc.sasl.Authenticate(t.Context(), "broker:9092")
	elapsed := time.Since(start)

	if authErr == nil {
		t.Fatal("expected a timeout error from a hanging token endpoint, got nil")
	}
	if !isDeadlineErr(authErr) {
		t.Errorf("error is not a deadline/timeout error: %v", authErr)
	}
	// Generous upper bound so this stays robust under CI load: it must not hang
	// anywhere close to the server's 10x-testTimeout sleep.
	if elapsed > 5*testTimeout {
		t.Errorf("Authenticate took %s, want roughly <= %s (testTimeout=%s)", elapsed, 2*testTimeout, testTimeout)
	}
	t.Logf("Authenticate failed after %s (testTimeout=%s): %v", elapsed, testTimeout, authErr)

	<-blockUntilClientGone // avoid leaking the handler goroutine past test end
}

// isDeadlineErr reports whether err (or something it wraps) is a timeout /
// deadline-exceeded error, covering both the net.Error.Timeout() form used by
// *http.Client's Timeout and a wrapped context.DeadlineExceeded.
func isDeadlineErr(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	// Fall back to substring matching: (*url.Error) from net/http wraps the
	// underlying timeout in a way that isn't always unwrapped cleanly, and its
	// Error() string reliably contains "Client.Timeout" for client-side
	// timeouts or "context deadline exceeded" for context-based ones.
	msg := err.Error()
	return strings.Contains(msg, "Client.Timeout") || strings.Contains(msg, "context deadline exceeded") || strings.Contains(msg, "timeout")
}

// ---- tokenEndpointCA (private-CA IdP trust) ----

// tlsTokenServer is like tokenServer but serves over TLS with a self-signed
// certificate (httptest.NewTLSServer), simulating a private-CA IdP.
func tlsTokenServer(t *testing.T, accessToken string) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": accessToken,
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// writeCAFile writes srv's own (self-signed) certificate as a PEM CA file
// under dir and returns its basename, for use as a file-sourced ValueFrom.
func writeCAFile(t *testing.T, dir string, srv *httptest.Server) string {
	t.Helper()
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	const name = "idp-ca.crt"
	require := func(err error) {
		if err != nil {
			t.Fatalf("writing CA file: %v", err)
		}
	}
	require(os.WriteFile(filepath.Join(dir, name), caPEM, 0o600))
	return name
}

// oauthAuthWithCA is like oauthAuth but also sets tokenEndpointCA from a
// file-sourced ValueFrom (relative to the CLI resolver's BaseDir).
func oauthAuthWithCA(tokenEndpoint, clientIDEnv, clientSecretEnv, caFile string) *v1alpha1.AuthConfig {
	a := oauthAuth(tokenEndpoint, clientIDEnv, clientSecretEnv)
	a.OAuth.TokenEndpointCA = &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: caFile}}
	return a
}

// TestBuildOAuthBearer_TokenEndpointCA_TrustsPrivateCA verifies that setting
// auth.oauth.tokenEndpointCA lets the token client complete a TLS handshake
// with a private-CA IdP that the system trust store would otherwise reject.
func TestBuildOAuthBearer_TokenEndpointCA_TrustsPrivateCA(t *testing.T) {
	const wantAccessToken = "tok-private-ca"
	srv := tlsTokenServer(t, wantAccessToken)

	dir := t.TempDir()
	caFile := writeCAFile(t, dir, srv)

	t.Setenv("OAUTH_CID_CA", "c-ca")
	t.Setenv("OAUTH_CSEC_CA", "s-ca")

	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		Auth:             oauthAuthWithCA(srv.URL+"/token", "OAUTH_CID_CA", "OAUTH_CSEC_CA", caFile),
	}}

	resolver := secrets.FileEnvResolver{BaseDir: dir}
	cc, err := buildConnConfig(c, resolver)
	if err != nil {
		t.Fatalf("buildConnConfig: %v", err)
	}

	_, initBytes, err := cc.sasl.Authenticate(t.Context(), "broker:9092")
	if err != nil {
		t.Fatalf("Authenticate: %v (tokenEndpointCA should have made the private-CA IdP trusted)", err)
	}
	if !strings.Contains(string(initBytes), wantAccessToken) {
		t.Errorf("SASL init bytes do not contain access token %q: %q", wantAccessToken, initBytes)
	}
}

// TestBuildOAuthBearer_NoTokenEndpointCA_RejectsPrivateCA is the negative
// control: without tokenEndpointCA, the same private-CA IdP is rejected by
// the default (system trust store) transport, proving the field is load
// bearing and that spec.tls.caCert is never silently reused for the IdP.
func TestBuildOAuthBearer_NoTokenEndpointCA_RejectsPrivateCA(t *testing.T) {
	srv := tlsTokenServer(t, "tok-should-not-be-used")

	t.Setenv("OAUTH_CID_NOCA", "c-noca")
	t.Setenv("OAUTH_CSEC_NOCA", "s-noca")

	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		Auth:             oauthAuth(srv.URL+"/token", "OAUTH_CID_NOCA", "OAUTH_CSEC_NOCA"),
	}}

	cc, err := buildConnConfig(c, envResolver)
	if err != nil {
		t.Fatalf("buildConnConfig: %v", err)
	}

	_, _, authErr := cc.sasl.Authenticate(t.Context(), "broker:9092")
	if authErr == nil {
		t.Fatal("expected a TLS trust error against a private-CA IdP without tokenEndpointCA, got nil")
	}
}

// TestBuildOAuthBearer_TokenEndpointCA_InvalidPEM verifies that an
// unparseable tokenEndpointCA PEM fails buildConnConfig immediately (before
// any network I/O), with an error that does not leak the (garbage) PEM
// content.
func TestBuildOAuthBearer_TokenEndpointCA_InvalidPEM(t *testing.T) {
	dir := t.TempDir()
	const badPEM = "not a valid PEM certificate, definitely-not-secret-marker"
	require := func(err error) {
		if err != nil {
			t.Fatalf("writing bad CA file: %v", err)
		}
	}
	require(os.WriteFile(filepath.Join(dir, "bad-ca.crt"), []byte(badPEM), 0o600))

	t.Setenv("OAUTH_CID_BADCA", "c-badca")
	t.Setenv("OAUTH_CSEC_BADCA", "s-badca")

	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		Auth:             oauthAuthWithCA("https://example.invalid/token", "OAUTH_CID_BADCA", "OAUTH_CSEC_BADCA", "bad-ca.crt"),
	}}

	resolver := secrets.FileEnvResolver{BaseDir: dir}
	_, err := buildConnConfig(c, resolver)
	if err == nil {
		t.Fatal("expected an error for an invalid tokenEndpointCA PEM, got nil")
	}
	if strings.Contains(err.Error(), badPEM) {
		t.Errorf("error message leaks the invalid PEM content: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "tokenEndpointCA") {
		t.Errorf("error message should name the field (tokenEndpointCA): %q", err.Error())
	}
}
