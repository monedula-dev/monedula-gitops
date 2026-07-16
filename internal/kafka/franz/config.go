// Package franz implements the kafka.AdminClient seam on top of the real
// franz-go (kgo) client and its kadm admin helper.
package franz

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// connConfig is an inspectable, broker-free intermediate produced from a
// KafkaCluster spec. It exists so the connection wiring can be unit tested
// without dialing a broker; opts() turns it into kgo options.
type connConfig struct {
	seeds []string
	tls   *tls.Config    // nil if TLS disabled
	sasl  sasl.Mechanism // nil if no SASL
}

// buildConnConfig translates a KafkaCluster spec into a connConfig. The Resolver
// resolves secret references (SASL credentials, TLS CA cert). It performs no
// network I/O.
func buildConnConfig(c *v1alpha1.KafkaCluster, r secrets.Resolver) (connConfig, error) {
	var cc connConfig

	// Seeds: split on comma, trim, drop empties.
	for _, s := range strings.Split(c.Spec.BootstrapServers, ",") {
		if s = strings.TrimSpace(s); s != "" {
			cc.seeds = append(cc.seeds, s)
		}
	}
	if len(cc.seeds) == 0 {
		return connConfig{}, fmt.Errorf("bootstrapServers is empty: at least one broker is required")
	}

	// TLS.
	if c.Spec.TLS != nil && c.Spec.TLS.Enabled {
		tlsCfg := &tls.Config{InsecureSkipVerify: c.Spec.TLS.InsecureSkipVerify} //nolint:gosec // insecure is opt-in via spec
		if c.Spec.TLS.CACert != nil {
			// Custom CA: resolve the PEM (file/env/inline in CLI mode,
			// secretKeyRef/configMapKeyRef in operator mode) and build a
			// dedicated root pool.
			pem, err := r.Resolve(*c.Spec.TLS.CACert)
			if err != nil {
				return connConfig{}, fmt.Errorf("resolving tls.caCert: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(pem)) {
				return connConfig{}, fmt.Errorf("tls.caCert does not contain a valid PEM certificate")
			}
			tlsCfg.RootCAs = pool
		}
		// CACert nil => system root pool (RootCAs left nil) honoring
		// insecureSkipVerify, as before.

		// Client certificate (mTLS or mutual TLS alongside SASL).
		// Validation guarantees both ClientCert and ClientKey are set together.
		if c.Spec.TLS.ClientCert != nil && c.Spec.TLS.ClientKey != nil {
			certPEM, err := r.Resolve(*c.Spec.TLS.ClientCert)
			if err != nil {
				return connConfig{}, fmt.Errorf("resolving tls.clientCert: %w", err)
			}
			keyPEM, err := r.Resolve(*c.Spec.TLS.ClientKey)
			if err != nil {
				return connConfig{}, fmt.Errorf("resolving tls.clientKey: %w", err)
			}
			cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
			if err != nil {
				// SECURITY: never include PEM/key material in errors.
				return connConfig{}, fmt.Errorf("loading tls.clientCert/tls.clientKey keypair: %w", err)
			}
			tlsCfg.Certificates = append(tlsCfg.Certificates, cert)
		}
		cc.tls = tlsCfg
	}

	// SASL.
	if c.Spec.Auth != nil {
		mech, err := buildSASL(c, r)
		if err != nil {
			return connConfig{}, err
		}
		cc.sasl = mech
	}

	return cc, nil
}

// buildSASL constructs the SASL mechanism for the configured auth.Mechanism.
//
// NOTE: SCRAM-SHA-256, SCRAM-SHA-512, and PLAIN share the auth.scram credential block.
// OAUTHBEARER uses the separate auth.oauth block (see buildOAuthBearer); mTLS carries no
// SASL at all (the TLS client cert is the authentication).
func buildSASL(c *v1alpha1.KafkaCluster, r secrets.Resolver) (sasl.Mechanism, error) {
	switch c.Spec.Auth.Mechanism {
	case "", "None":
		return nil, nil
	case "SCRAM-SHA-256":
		user, pass, err := scramCreds(c, r)
		if err != nil {
			return nil, err
		}
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		user, pass, err := scramCreds(c, r)
		if err != nil {
			return nil, err
		}
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism(), nil
	case "PLAIN":
		user, pass, err := scramCreds(c, r)
		if err != nil {
			return nil, err
		}
		return plain.Auth{User: user, Pass: pass}.AsMechanism(), nil
	case "OAUTHBEARER":
		return buildOAuthBearer(c, r)
	case "mTLS":
		// mTLS = TLS client-certificate authentication (spec §4.5).
		// No SASL mechanism is used; the client cert is the credential.
		// Validation normally guarantees tls.enabled + clientCert/clientKey are set,
		// but we fail closed here so that a bypassed validation can never silently
		// yield a plaintext connection with no authentication.
		tlsSpec := c.Spec.TLS
		if tlsSpec == nil || !tlsSpec.Enabled || tlsSpec.ClientCert == nil || tlsSpec.ClientKey == nil {
			return nil, fmt.Errorf("auth mechanism %q requires tls.enabled with tls.clientCert and tls.clientKey", "mTLS")
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("auth mechanism %q is not supported", c.Spec.Auth.Mechanism)
	}
}

// scramCreds resolves the username/password from the auth.scram block.
func scramCreds(c *v1alpha1.KafkaCluster, r secrets.Resolver) (user, pass string, err error) {
	if c.Spec.Auth.SCRAM == nil {
		return "", "", fmt.Errorf("auth mechanism %q requires auth.scram credentials, but none were provided", c.Spec.Auth.Mechanism)
	}
	user, err = r.Resolve(c.Spec.Auth.SCRAM.Username)
	if err != nil {
		return "", "", fmt.Errorf("resolving SASL username: %w", err)
	}
	pass, err = r.Resolve(c.Spec.Auth.SCRAM.Password)
	if err != nil {
		return "", "", fmt.Errorf("resolving SASL password: %w", err)
	}
	return user, pass, nil
}

// opts converts a connConfig into the kgo options needed to dial the cluster.
func (cc connConfig) opts() []kgo.Opt {
	// RetryTimeout pins the worst-case request lifetime explicitly (it matches
	// franz-go's current default). The operator holds per-(cluster, substrate)
	// locks across broker I/O, so this bound is what caps how long a hung
	// broker can stall a substrate's other writers — it must not drift with an
	// upstream default change. MDS/SR clients pin the same 30s explicitly.
	opts := []kgo.Opt{kgo.SeedBrokers(cc.seeds...), kgo.RetryTimeout(30 * time.Second)}
	if cc.tls != nil {
		opts = append(opts, kgo.DialTLSConfig(cc.tls))
	}
	if cc.sasl != nil {
		opts = append(opts, kgo.SASL(cc.sasl))
	}
	return opts
}
