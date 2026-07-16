// Package cluster builds the live Kafka and Schema Registry clients for a
// KafkaCluster spec, resolving any secret references via a secrets.Resolver.
// It is the single seam through which both the CLI (FileEnvResolver) and the
// operator (a Kubernetes-Secret resolver) construct clients.
package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/franz"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	mdsconfluent "github.com/monedula-dev/monedula-gitops/internal/mds/confluent"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry/confluent"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// KafkaLogWriter, when non-nil, makes BuildKafkaClient attach franz-go's
// BasicLogger writing to it. It is the CLI's debug-logging hook (--log-level
// debug); the operator leaves it nil. The kgo logger is attached at
// kgo.LogLevelInfo deliberately, even though our flag level is "debug":
// kgo's info level covers connection/metadata lifecycle without the
// per-request payload dumps of kgo's own debug level, and neither level logs
// SASL credentials — keeping secrets out of stderr by construction.
var KafkaLogWriter io.Writer

// BuildKafkaClient constructs a live kafka.AdminClient for the cluster, resolving
// credentials and any TLS CA cert via r. It returns the client and a cleanup
// func the caller must defer (it closes the client). No network I/O is performed
// during construction.
func BuildKafkaClient(c *v1alpha1.KafkaCluster, r secrets.Resolver) (kafka.AdminClient, func(), error) {
	noop := func() {}
	var extra []kgo.Opt
	if KafkaLogWriter != nil {
		extra = append(extra, kgo.WithLogger(kgo.BasicLogger(KafkaLogWriter, kgo.LogLevelInfo, nil)))
	}
	client, err := franz.New(c, r, extra...)
	if err != nil {
		return nil, noop, err
	}
	return client, client.Close, nil
}

// BuildSchemaClient constructs a live schemaregistry.Client for the cluster's
// Schema Registry, or (nil, nil) when the cluster has no schemaRegistry
// configured. Basic-auth credentials and TLS material (custom CA, optional
// client cert), if configured, are resolved via r.
func BuildSchemaClient(c *v1alpha1.KafkaCluster, r secrets.Resolver) (schemaregistry.Client, error) {
	if c == nil || c.Spec.SchemaRegistry == nil {
		return nil, nil
	}
	sr := c.Spec.SchemaRegistry

	httpClient, err := buildHTTPClientTLS(sr.TLS, r, "schemaRegistry")
	if err != nil {
		return nil, fmt.Errorf("building schema registry http client: %w", err)
	}

	var auth *confluent.Auth
	if sr.Auth != nil && sr.Auth.Type == "basic" {
		user, err := r.Resolve(sr.Auth.Username)
		if err != nil {
			return nil, err
		}
		pass, err := r.Resolve(sr.Auth.Password)
		if err != nil {
			return nil, err
		}
		auth = &confluent.Auth{Username: user, Password: pass}
	}
	return confluent.New(sr.Endpoint, auth, httpClient)
}

// httpClientTimeout is the per-request timeout applied to the http.Clients
// built for the HTTP-based backends (MDS, Schema Registry). Matches the
// defaults in mds/confluent and schemaregistry/confluent (30 s) and is set
// here explicitly when we construct the http.Client so the factory controls
// the deadline.
const httpClientTimeout = 30 * time.Second

// BuildMDSClient constructs a live mds.Client for the cluster's Confluent
// Metadata Service (RBAC), or (nil, nil) when the cluster has no MDS
// configured. Credentials and TLS material are resolved via r.
//
// Mirrors BuildSchemaClient's nil-handling and error-wrapping conventions.
// mTLS cert loading mirrors the pattern in internal/kafka/franz/config.go
// (buildConnConfig).
func BuildMDSClient(c *v1alpha1.KafkaCluster, r secrets.Resolver) (mds.Client, error) {
	if c == nil || c.Spec.Authorization == nil || c.Spec.Authorization.MDS == nil {
		return nil, nil
	}
	mdsConf := c.Spec.Authorization.MDS

	// Build a TLS-aware http.Client from MDSConfig.TLS (CA + optional client
	// cert), mirroring buildConnConfig in internal/kafka/franz/config.go.
	httpClient, err := buildHTTPClientTLS(mdsConf.TLS, r, "mds")
	if err != nil {
		return nil, fmt.Errorf("building mds http client: %w", err)
	}

	// Resolve auth credentials.
	var auth *mdsconfluent.Auth
	if mdsConf.Auth != nil {
		switch mdsConf.Auth.Type {
		case "basic":
			if mdsConf.Auth.Username == nil || mdsConf.Auth.Password == nil {
				return nil, fmt.Errorf("mds: basic auth requires username and password")
			}
			user, err := r.Resolve(*mdsConf.Auth.Username)
			if err != nil {
				return nil, fmt.Errorf("mds: resolving auth.username: %w", err)
			}
			pass, err := r.Resolve(*mdsConf.Auth.Password)
			if err != nil {
				return nil, fmt.Errorf("mds: resolving auth.password: %w", err)
			}
			auth = mdsconfluent.BasicAuth(user, pass)
		case "bearer":
			if mdsConf.Auth.Token == nil {
				return nil, fmt.Errorf("mds: bearer auth requires token")
			}
			tok, err := r.Resolve(*mdsConf.Auth.Token)
			if err != nil {
				return nil, fmt.Errorf("mds: resolving auth.token: %w", err)
			}
			auth = mdsconfluent.BearerAuth(tok)
		case "mtls":
			// mTLS: the client cert is already on httpClient's TLS config.
			// No auth header is sent (authNone / MTLSAuth).
			auth = mdsconfluent.MTLSAuth()
		default:
			return nil, fmt.Errorf("mds: unsupported auth type %q (supported: basic, bearer, mtls)", mdsConf.Auth.Type)
		}
	}

	return mdsconfluent.New(mdsConf.Endpoint, auth, httpClient)
}

// buildHTTPClientTLS constructs an *http.Client with an optional TLS config
// derived from tlsSpec (CA cert + optional client cert/key). When tlsSpec is
// nil or disabled, a plain http.Client with a timeout is returned. fieldLabel
// names the owning spec block in errors (e.g. "mds" -> "mds tls.caCert",
// "schemaRegistry" -> "schemaRegistry tls.caCert").
//
// Shared by the MDS and Schema Registry client factories. Mirrors
// buildConnConfig in internal/kafka/franz/config.go for TLS material
// resolution (Resolve for CA/cert/key PEM strings).
//
// Shape validation (cert+key must be paired, and require tls.enabled: true)
// is the validation package's job, not this function's: buildHTTPClientTLS
// assumes tlsSpec already has a valid shape and silently no-ops on a partial
// clientCert/clientKey pair (see the `tlsSpec.ClientCert != nil &&
// tlsSpec.ClientKey != nil` guard below).
func buildHTTPClientTLS(tlsSpec *v1alpha1.TLSConfig, r secrets.Resolver, fieldLabel string) (*http.Client, error) {
	if tlsSpec == nil || !tlsSpec.Enabled {
		return &http.Client{Timeout: httpClientTimeout}, nil
	}

	tlsCfg := &tls.Config{InsecureSkipVerify: tlsSpec.InsecureSkipVerify} //nolint:gosec // insecure is opt-in via spec

	// Custom CA cert.
	if tlsSpec.CACert != nil {
		pem, err := r.Resolve(*tlsSpec.CACert)
		if err != nil {
			return nil, fmt.Errorf("resolving %s tls.caCert: %w", fieldLabel, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(pem)) {
			return nil, fmt.Errorf("%s tls.caCert does not contain a valid PEM certificate", fieldLabel)
		}
		tlsCfg.RootCAs = pool
	}
	// CACert nil => system root pool (RootCAs left nil).

	// mTLS client certificate.
	if tlsSpec.ClientCert != nil && tlsSpec.ClientKey != nil {
		certPEM, err := r.Resolve(*tlsSpec.ClientCert)
		if err != nil {
			return nil, fmt.Errorf("resolving %s tls.clientCert: %w", fieldLabel, err)
		}
		keyPEM, err := r.Resolve(*tlsSpec.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("resolving %s tls.clientKey: %w", fieldLabel, err)
		}
		cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		if err != nil {
			// SECURITY: never include PEM/key material in errors.
			return nil, fmt.Errorf("loading %s tls.clientCert/tls.clientKey keypair: %w", fieldLabel, err)
		}
		tlsCfg.Certificates = append(tlsCfg.Certificates, cert)
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsCfg
	return &http.Client{
		Timeout:   httpClientTimeout,
		Transport: transport,
	}, nil
}
