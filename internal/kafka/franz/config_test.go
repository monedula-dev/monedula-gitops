package franz

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// envResolver is the CLI resolver rooted at "." for tests reading env-based creds.
var envResolver = secrets.FileEnvResolver{BaseDir: "."}

// certResolver is a stub Resolver returning fixed PEM bytes (or an error) for
// any reference, used to test caCert wiring without a real Secret backend. A
// zero certResolver falls through to the CLI FileEnvResolver.
type certResolver struct {
	pem []byte
	err error
}

func (s certResolver) Resolve(vf v1alpha1.ValueFrom) (string, error) {
	if s.pem != nil || s.err != nil {
		return string(s.pem), s.err
	}
	return secrets.FileEnvResolver{BaseDir: "."}.Resolve(vf)
}

// selfSignedPEM generates a throwaway self-signed cert PEM for the cert-pool test.
func selfSignedPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating cert: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// scramAuth builds an AuthConfig whose scram block reads creds from env vars.
func scramAuth(mech, userEnv, passEnv string) *v1alpha1.AuthConfig {
	a := &v1alpha1.AuthConfig{Mechanism: mech}
	a.SCRAM = &v1alpha1.SCRAMAuth{
		Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: userEnv}},
		Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: passEnv}},
	}
	return a
}

func TestBuildConnConfig_Plaintext(t *testing.T) {
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092, b:9092",
	}}
	cc, err := buildConnConfig(c, envResolver)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cc.seeds) != 2 {
		t.Fatalf("seeds = %v, want 2", cc.seeds)
	}
	if cc.tls != nil {
		t.Errorf("tls = %v, want nil", cc.tls)
	}
	if cc.sasl != nil {
		t.Errorf("sasl = %v, want nil", cc.sasl)
	}
}

func TestBuildConnConfig_EmptyBootstrap(t *testing.T) {
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{BootstrapServers: "  ,  "}}
	if _, err := buildConnConfig(c, envResolver); err == nil {
		t.Fatal("expected error for empty bootstrapServers")
	}
}

func TestBuildConnConfig_TLSInsecure(t *testing.T) {
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9093",
		TLS:              &v1alpha1.TLSConfig{Enabled: true, InsecureSkipVerify: true},
	}}
	cc, err := buildConnConfig(c, envResolver)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cc.tls == nil {
		t.Fatal("tls = nil, want non-nil")
	}
	if !cc.tls.InsecureSkipVerify {
		t.Error("tls.InsecureSkipVerify = false, want true")
	}
	if cc.tls.RootCAs != nil {
		t.Error("tls.RootCAs = non-nil, want nil (system roots) when no caCert")
	}
}

// caClusterSpec builds a TLS-enabled cluster spec whose CA resolves from src.
func caClusterSpec(src v1alpha1.ValueSource) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9093",
		TLS: &v1alpha1.TLSConfig{
			Enabled: true,
			CACert:  &v1alpha1.ValueFrom{ValueFrom: src},
		},
	}}
}

// TestBuildConnConfig_TLSCACertFromFileCLI confirms the CLI FileEnvResolver
// resolves tls.caCert from a file reference and populates tls.RootCAs with
// exactly the CA cert from that file (not just "some non-nil pool").
func TestBuildConnConfig_TLSCACertFromFileCLI(t *testing.T) {
	dir := t.TempDir()
	caPEM := selfSignedPEM(t)
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), caPEM, 0o600); err != nil {
		t.Fatalf("writing ca.crt: %v", err)
	}
	c := caClusterSpec(v1alpha1.ValueSource{File: "ca.crt"})
	cc, err := buildConnConfig(c, secrets.FileEnvResolver{BaseDir: dir})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cc.tls == nil || cc.tls.RootCAs == nil {
		t.Fatal("tls.RootCAs = nil, want a populated pool from tls.caCert file ref")
	}
	want := x509.NewCertPool()
	if !want.AppendCertsFromPEM(caPEM) {
		t.Fatal("test bug: failed to parse the self-signed PEM used as expectation")
	}
	if !cc.tls.RootCAs.Equal(want) {
		t.Error("tls.RootCAs does not contain (only) the CA cert from tls.caCert file ref")
	}
	// Also confirm it differs from an empty pool, i.e. something real landed in it.
	if cc.tls.RootCAs.Equal(x509.NewCertPool()) {
		t.Error("tls.RootCAs equals an empty pool, want the injected CA")
	}
}

// TestBuildConnConfig_TLSCACertFromEnvCLI confirms the CLI FileEnvResolver
// resolves tls.caCert from an environment variable.
func TestBuildConnConfig_TLSCACertFromEnvCLI(t *testing.T) {
	t.Setenv("KAFKA_CA_PEM", string(selfSignedPEM(t)))
	c := caClusterSpec(v1alpha1.ValueSource{Env: "KAFKA_CA_PEM"})
	cc, err := buildConnConfig(c, envResolver)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cc.tls == nil || cc.tls.RootCAs == nil {
		t.Fatal("tls.RootCAs = nil, want a populated pool from tls.caCert env ref")
	}
}

// TestBuildConnConfig_TLSCACertSecretKeyRefRejectedCLI confirms that with the
// CLI FileEnvResolver, a caCert via secretKeyRef errors clearly (k8s-only).
func TestBuildConnConfig_TLSCACertSecretKeyRefRejectedCLI(t *testing.T) {
	c := caClusterSpec(v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "ca", Key: "ca.crt"}})
	_, err := buildConnConfig(c, envResolver)
	if err == nil {
		t.Fatal("expected error for tls.caCert secretKeyRef with FileEnvResolver")
	}
	if !strings.Contains(err.Error(), "tls.caCert") {
		t.Errorf("error %q does not mention tls.caCert", err)
	}
	if !strings.Contains(err.Error(), "secretKeyRef") {
		t.Errorf("error %q does not mention secretKeyRef", err)
	}
}

// TestBuildConnConfig_TLSCACertPopulatesPool confirms that a resolver
// returning a valid PEM populates tls.RootCAs (the operator plumbing).
func TestBuildConnConfig_TLSCACertPopulatesPool(t *testing.T) {
	c := caClusterSpec(v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "ca", Key: "ca.crt"}})
	cc, err := buildConnConfig(c, certResolver{pem: selfSignedPEM(t)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cc.tls == nil {
		t.Fatal("tls = nil, want non-nil")
	}
	if cc.tls.RootCAs == nil {
		t.Fatal("tls.RootCAs = nil, want a populated pool from caCert")
	}
}

// TestBuildConnConfig_TLSCACertInvalidPEM confirms a non-PEM payload errors,
// and that the error does not leak the (bogus) PEM payload.
func TestBuildConnConfig_TLSCACertInvalidPEM(t *testing.T) {
	c := caClusterSpec(v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "ca", Key: "ca.crt"}})
	_, err := buildConnConfig(c, certResolver{pem: []byte("not a pem")})
	if err == nil {
		t.Fatal("expected error for invalid PEM")
	}
	// SECURITY: must not echo the (rejected) payload material.
	if strings.Contains(err.Error(), "not a pem") {
		t.Errorf("error leaks PEM payload: %q", err)
	}
}

func TestBuildConnConfig_ScramSha512(t *testing.T) {
	t.Setenv("KAFKA_USER", "alice")
	t.Setenv("KAFKA_PASS", "s3cret")
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9093",
		Auth:             scramAuth("SCRAM-SHA-512", "KAFKA_USER", "KAFKA_PASS"),
	}}
	cc, err := buildConnConfig(c, envResolver)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cc.sasl == nil {
		t.Fatal("sasl = nil, want non-nil")
	}
}

func TestBuildConnConfig_ScramSha256(t *testing.T) {
	t.Setenv("KAFKA_USER", "alice")
	t.Setenv("KAFKA_PASS", "s3cret")
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9093",
		Auth:             scramAuth("SCRAM-SHA-256", "KAFKA_USER", "KAFKA_PASS"),
	}}
	cc, err := buildConnConfig(c, envResolver)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cc.sasl == nil {
		t.Fatal("sasl = nil, want non-nil")
	}
}

func TestBuildConnConfig_Plain(t *testing.T) {
	t.Setenv("KAFKA_USER", "alice")
	t.Setenv("KAFKA_PASS", "s3cret")
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		Auth:             scramAuth("PLAIN", "KAFKA_USER", "KAFKA_PASS"),
	}}
	cc, err := buildConnConfig(c, envResolver)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cc.sasl == nil {
		t.Fatal("sasl = nil, want non-nil")
	}
}

func TestBuildConnConfig_ScramMissingCreds(t *testing.T) {
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9093",
		Auth:             &v1alpha1.AuthConfig{Mechanism: "SCRAM-SHA-256"}, // SCRAM block nil
	}}
	if _, err := buildConnConfig(c, envResolver); err == nil {
		t.Fatal("expected error when auth.scram is nil")
	}
}

func TestBuildConnConfig_OAuthBearerMissingOAuthBlock(t *testing.T) {
	// OAUTHBEARER without an auth.oauth block must fail with a clear message.
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9093",
		Auth:             &v1alpha1.AuthConfig{Mechanism: "OAUTHBEARER"},
	}}
	_, err := buildConnConfig(c, envResolver)
	if err == nil {
		t.Fatal("expected error for OAUTHBEARER without oauth block")
	}
	if !strings.Contains(err.Error(), "auth.oauth") {
		t.Errorf("error %q does not mention auth.oauth", err)
	}
}

// selfSignedClientPEM generates a throwaway self-signed client cert + private key pair,
// returning (certPEM, keyPEM). Uses ECDSA P-256 for speed.
func selfSignedClientPEM(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating ecdsa key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(42),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating cert: %v", err)
	}
	certBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshalling ec key: %v", err)
	}
	keyBytes := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return string(certBytes), string(keyBytes)
}

// mapResolver is a Resolver backed by an in-memory map (key = ValueFrom.File path),
// used in tests to inject PEM material without touching the filesystem.
type mapResolver struct {
	files map[string]string // key: file path → resolved string value
	certResolver
}

func (m mapResolver) Resolve(vf v1alpha1.ValueFrom) (string, error) {
	if vf.ValueFrom.File != "" {
		if v, ok := m.files[vf.ValueFrom.File]; ok {
			return v, nil
		}
	}
	return m.certResolver.Resolve(vf)
}

// TestBuildConnConfig_MTLS_HappyPath verifies that mTLS wires a tls.Config with
// 1 certificate and returns nil SASL.
func TestBuildConnConfig_MTLS_HappyPath(t *testing.T) {
	certPEM, keyPEM := selfSignedClientPEM(t)
	r := mapResolver{files: map[string]string{
		"client.crt": certPEM,
		"client.key": keyPEM,
	}}
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9093",
		TLS: &v1alpha1.TLSConfig{
			Enabled:    true,
			ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
			ClientKey:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
		},
		Auth: &v1alpha1.AuthConfig{Mechanism: "mTLS"},
	}}
	cc, err := buildConnConfig(c, r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cc.tls == nil {
		t.Fatal("tls = nil, want non-nil for mTLS")
	}
	if len(cc.tls.Certificates) != 1 {
		t.Errorf("tls.Certificates len = %d, want 1", len(cc.tls.Certificates))
	}
	if cc.sasl != nil {
		t.Errorf("sasl = %v, want nil for mTLS (no SASL)", cc.sasl)
	}
}

// TestBuildConnConfig_MTLS_BadCertPEM verifies that a garbage cert PEM returns
// an error that mentions the field path but NOT the bad material.
func TestBuildConnConfig_MTLS_BadCertPEM(t *testing.T) {
	_, keyPEM := selfSignedClientPEM(t)
	r := mapResolver{files: map[string]string{
		"client.crt": "not a valid pem",
		"client.key": keyPEM,
	}}
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9093",
		TLS: &v1alpha1.TLSConfig{
			Enabled:    true,
			ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
			ClientKey:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
		},
		Auth: &v1alpha1.AuthConfig{Mechanism: "mTLS"},
	}}
	_, err := buildConnConfig(c, r)
	if err == nil {
		t.Fatal("expected error for bad cert PEM, got nil")
	}
	if !strings.Contains(err.Error(), "tls.clientCert") {
		t.Errorf("error %q does not mention 'tls.clientCert'", err)
	}
	if !strings.Contains(err.Error(), "loading") {
		t.Errorf("error %q does not mention 'loading'", err)
	}
	// SECURITY: must not echo PEM material.
	if strings.Contains(err.Error(), "not a valid pem") {
		t.Errorf("error leaks PEM material: %q", err)
	}
}

// TestBuildConnConfig_MTLS_MissingKeyResolveFails verifies that a resolver error
// for the key path returns an error mentioning the field path.
func TestBuildConnConfig_MTLS_MissingKeyResolveFails(t *testing.T) {
	certPEM, _ := selfSignedClientPEM(t)
	r := mapResolver{files: map[string]string{
		"client.crt": certPEM,
		// client.key intentionally absent → falls back to FileEnvResolver which will fail
	}}
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9093",
		TLS: &v1alpha1.TLSConfig{
			Enabled:    true,
			ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
			ClientKey:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
		},
		Auth: &v1alpha1.AuthConfig{Mechanism: "mTLS"},
	}}
	_, err := buildConnConfig(c, r)
	if err == nil {
		t.Fatal("expected error when key cannot be resolved")
	}
	if !strings.Contains(err.Error(), "tls.clientKey") {
		t.Errorf("error %q does not mention 'tls.clientKey'", err)
	}
}

// TestBuildConnConfig_MTLS_TLSDisabledFailsClosed verifies that mTLS with TLS disabled
// returns an error (fail-closed guard) rather than silently yielding a plaintext connection.
func TestBuildConnConfig_MTLS_TLSDisabledFailsClosed(t *testing.T) {
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9093",
		TLS:              &v1alpha1.TLSConfig{Enabled: false}, // explicitly disabled
		Auth:             &v1alpha1.AuthConfig{Mechanism: "mTLS"},
	}}
	_, err := buildConnConfig(c, envResolver)
	if err == nil {
		t.Fatal("expected error for mTLS with tls.enabled=false, got nil")
	}
	if !strings.Contains(err.Error(), "mTLS") {
		t.Errorf("error %q does not mention mTLS", err)
	}
	if !strings.Contains(err.Error(), "tls.enabled") {
		t.Errorf("error %q does not mention tls.enabled", err)
	}
}

// TestBuildSASL_MTLSReturnsNilMechanism confirms buildSASL returns (nil, nil) for mTLS
// when TLS is properly configured (the guard passes).
func TestBuildSASL_MTLSReturnsNilMechanism(t *testing.T) {
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9093",
		TLS: &v1alpha1.TLSConfig{
			Enabled:    true,
			ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
			ClientKey:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
		},
		Auth: &v1alpha1.AuthConfig{Mechanism: "mTLS"},
	}}
	mech, err := buildSASL(c, envResolver)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mech != nil {
		t.Errorf("sasl mechanism = %v, want nil for mTLS", mech)
	}
}

// TestBuildSASL_UnknownMechanismError confirms the error for unknown mechanisms
// does not contain a version pin.
func TestBuildSASL_UnknownMechanismError(t *testing.T) {
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9093",
		Auth:             &v1alpha1.AuthConfig{Mechanism: "GSSAPI"},
	}}
	_, err := buildSASL(c, envResolver)
	if err == nil {
		t.Fatal("expected error for unknown mechanism")
	}
	if !strings.Contains(err.Error(), "GSSAPI") {
		t.Errorf("error %q does not mention mechanism name", err)
	}
	if strings.Contains(err.Error(), "v0.") {
		t.Errorf("error %q contains version pin (should age well without it)", err)
	}
}

func TestConnConfig_Opts(t *testing.T) {
	// Plaintext: SeedBrokers plus the always-on RetryTimeout pin — no
	// TLS/SASL options.
	cc := connConfig{seeds: []string{"a:9092"}}
	if got := len(cc.opts()); got != 2 {
		t.Errorf("plaintext opts len = %d, want 2 (seeds + retry timeout)", got)
	}
}
