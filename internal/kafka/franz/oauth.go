package franz

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	fwoauth "github.com/twmb/franz-go/pkg/sasl/oauth"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
	"github.com/twmb/franz-go/pkg/sasl"
)

// oauthTokenTimeout bounds every HTTP request the OAUTHBEARER token source
// makes to the IdP's token endpoint. It is a package-level var (not a const)
// so tests can shorten it to keep a deadline test fast. Production code must
// not modify it.
var oauthTokenTimeout = 10 * time.Second

// buildOAuthBearer constructs an OAUTHBEARER SASL mechanism for the OIDC
// client-credentials flow (spec §4.5). The ClientID and ClientSecret are
// resolved once at build time; tokens are fetched (and cached/refreshed) by
// the oauth2 ReuseTokenSource that is embedded in the returned mechanism.
//
// SECURITY: resolved secret values are never embedded in error messages or
// logged. If token retrieval fails, the error from the oauth2 library is
// returned as-is; it describes the HTTP failure without echoing secrets.
func buildOAuthBearer(c *v1alpha1.KafkaCluster, r secrets.Resolver) (sasl.Mechanism, error) {
	cfg := c.Spec.Auth.OAuth
	if cfg == nil {
		return nil, fmt.Errorf("auth mechanism OAUTHBEARER requires auth.oauth to be configured")
	}

	clientID, err := r.Resolve(cfg.ClientID)
	if err != nil {
		return nil, fmt.Errorf("resolving auth.oauth.clientId: %w", err)
	}
	clientSecret, err := r.Resolve(cfg.ClientSecret)
	if err != nil {
		return nil, fmt.Errorf("resolving auth.oauth.clientSecret: %w", err)
	}

	ccCfg := clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     cfg.TokenEndpoint,
	}
	if cfg.Scope != "" {
		ccCfg.Scopes = []string{cfg.Scope}
	}

	// Build the token source once for the lifetime of this client. We use
	// context.Background() so the long-lived token source is not tied to any
	// individual request context. oauth2.ReuseTokenSource (returned by
	// ccCfg.TokenSource) caches valid tokens and refreshes them on expiry.
	//
	// The background context carries an oauth2.HTTPClient value bounding every
	// token-fetch HTTP request to oauthTokenTimeout. Without this, the token
	// source would fall back to http.DefaultClient, which has no timeout: a
	// hung/unreachable IdP would block the token fetch (and therefore the SASL
	// handshake) indefinitely, since kgo's dial timeout does not cover time
	// spent inside a SASL mechanism callback.
	transport, err := oauthTransport(cfg, r)
	if err != nil {
		return nil, err
	}
	httpCli := &http.Client{Timeout: oauthTokenTimeout, Transport: transport}
	tokCtx := context.WithValue(context.Background(), oauth2.HTTPClient, httpCli)
	ts := ccCfg.TokenSource(tokCtx)

	return fwoauth.Oauth(func(ctx context.Context) (fwoauth.Auth, error) {
		// ctx is not forwarded to ts.Token(): the oauth2.TokenSource interface takes
		// no context. The underlying HTTP client uses the context.Background() (bounded
		// by oauthTokenTimeout via the injected *http.Client, see above) that was
		// captured when ccCfg.TokenSource() was called above.
		tok, err := ts.Token()
		if err != nil {
			// The oauth2 library describes HTTP-level failures without echoing
			// the client secret, so returning err as-is is safe.
			return fwoauth.Auth{}, fmt.Errorf("fetching OAUTHBEARER token: %w", err)
		}
		return fwoauth.Auth{Token: tok.AccessToken}, nil
	}), nil
}

// oauthTransport builds the http.RoundTripper used for the IdP token-endpoint
// client, trusting cfg.TokenEndpointCA when set. The IdP is a DIFFERENT trust
// domain than the Kafka brokers (spec.tls.caCert): a private-CA broker
// cluster and a private-CA IdP are not necessarily signed by the same
// authority, so this never falls back to or reuses spec.tls.caCert. Nil
// TokenEndpointCA (the common case) returns nil, meaning the oauth2 http
// client falls back to its own default transport (system trust store),
// exactly as before this field existed.
func oauthTransport(cfg *v1alpha1.OAuthConfig, r secrets.Resolver) (http.RoundTripper, error) {
	if cfg.TokenEndpointCA == nil {
		return nil, nil
	}
	pem, err := r.Resolve(*cfg.TokenEndpointCA)
	if err != nil {
		return nil, fmt.Errorf("resolving oauth.tokenEndpointCA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(pem)) {
		// SECURITY: never include the PEM content in the error.
		return nil, fmt.Errorf("oauth.tokenEndpointCA does not contain a valid PEM certificate")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool}
	return transport, nil
}
