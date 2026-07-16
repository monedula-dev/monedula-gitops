package cluster

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// stubResolver is a Resolver returning a fixed string (or an error) for any
// reference, used to test TLS material wiring without a real Secret backend.
type stubResolver struct {
	val string
	err error
}

func (s stubResolver) Resolve(v1alpha1.ValueFrom) (string, error) {
	return s.val, s.err
}

func TestBuildKafkaClient_Plaintext(t *testing.T) {
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
	}}
	client, cleanup, err := BuildKafkaClient(c, secrets.FileEnvResolver{BaseDir: "."})
	require.NoError(t, err)
	require.NotNil(t, client)
	require.NotNil(t, cleanup)
	cleanup() // no dial occurred; just releases the lazy client
}

func TestBuildKafkaClient_DebugLogWriter(t *testing.T) {
	// With the debug hook armed the client builds with kgo's BasicLogger
	// attached (no dial happens at construction, so nothing is written yet).
	var buf bytes.Buffer
	KafkaLogWriter = &buf
	t.Cleanup(func() { KafkaLogWriter = nil })

	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{BootstrapServers: "a:9092"}}
	client, cleanup, err := BuildKafkaClient(c, secrets.FileEnvResolver{BaseDir: "."})
	require.NoError(t, err)
	require.NotNil(t, client)
	cleanup()
}

func TestBuildKafkaClient_BadSpec(t *testing.T) {
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{BootstrapServers: ""}}
	_, _, err := BuildKafkaClient(c, secrets.FileEnvResolver{BaseDir: "."})
	require.Error(t, err)
}

func TestBuildSchemaClient_NoRegistry(t *testing.T) {
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{BootstrapServers: "a:9092"}}
	sc, err := BuildSchemaClient(c, secrets.FileEnvResolver{BaseDir: "."})
	require.NoError(t, err)
	require.Nil(t, sc)
}

func TestBuildSchemaClient_NilCluster(t *testing.T) {
	sc, err := BuildSchemaClient(nil, secrets.FileEnvResolver{BaseDir: "."})
	require.NoError(t, err)
	require.Nil(t, sc)
}

func TestBuildSchemaClient_Endpoint(t *testing.T) {
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		SchemaRegistry:   &v1alpha1.SchemaRegistryConf{Endpoint: "http://sr:8081"},
	}}
	sc, err := BuildSchemaClient(c, secrets.FileEnvResolver{BaseDir: "."})
	require.NoError(t, err)
	require.NotNil(t, sc)
}

func TestBuildSchemaClient_BasicAuth(t *testing.T) {
	t.Setenv("SR_USER", "u")
	t.Setenv("SR_PASS", "p")
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		SchemaRegistry: &v1alpha1.SchemaRegistryConf{
			Endpoint: "http://sr:8081",
			Auth: &v1alpha1.SchemaRegistryAuth{
				Type:     "basic",
				Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "SR_USER"}},
				Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "SR_PASS"}},
			},
		},
	}}
	sc, err := BuildSchemaClient(c, secrets.FileEnvResolver{BaseDir: "."})
	require.NoError(t, err)
	require.NotNil(t, sc)
}

func TestBuildSchemaClient_BasicAuthResolveError(t *testing.T) {
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		SchemaRegistry: &v1alpha1.SchemaRegistryConf{
			Endpoint: "http://sr:8081",
			Auth: &v1alpha1.SchemaRegistryAuth{
				Type:     "basic",
				Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "SR_MISSING_XYZ"}},
				Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "SR_MISSING_XYZ2"}},
			},
		},
	}}
	_, err := BuildSchemaClient(c, secrets.FileEnvResolver{BaseDir: "."})
	require.Error(t, err)
}

// selfSignedCAPEM generates a throwaway self-signed CA cert PEM for the
// cert-pool tests. Mirrors selfSignedPEM in internal/kafka/franz.
func selfSignedCAPEM(t *testing.T) []byte {
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

// selfSignedClientPEM generates a throwaway client cert + key PEM pair for the
// mTLS keypair test. Mirrors the helper in internal/kafka/franz.
func selfSignedClientPEM(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating ec key: %v", err)
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

// srTLSCluster builds a cluster spec whose schemaRegistry block carries the
// given TLS config.
func srTLSCluster(endpoint string, tlsSpec *v1alpha1.TLSConfig) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		SchemaRegistry: &v1alpha1.SchemaRegistryConf{
			Endpoint: endpoint,
			TLS:      tlsSpec,
		},
	}}
}

// TestBuildSchemaClient_TLSTrustsPrivateCA is the end-to-end trust test: a
// Schema Registry client built with tls.caCert pointing (via the CLI
// FileEnvResolver) at the CA of a private test TLS server successfully calls
// it over HTTPS — the private-CA scenario this feature exists for.
func TestBuildSchemaClient_TLSTrustsPrivateCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]string{"a-value"})
	}))
	t.Cleanup(srv.Close)

	// Write the test server's (self-signed) cert as the CA file to trust.
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ca.crt"), caPEM, 0o600))

	c := srTLSCluster(srv.URL, &v1alpha1.TLSConfig{
		Enabled: true,
		CACert:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "ca.crt"}},
	})
	sc, err := BuildSchemaClient(c, secrets.FileEnvResolver{BaseDir: dir})
	require.NoError(t, err)
	require.NotNil(t, sc)

	subjects, err := sc.ListSubjects(context.Background())
	require.NoError(t, err, "SR client should trust the private CA from tls.caCert")
	require.Equal(t, []string{"a-value"}, subjects)
}

// TestBuildSchemaClient_TLSUntrustedWithoutCA is the negative control for the
// trust test above: without tls.caCert the same private-CA server is rejected.
func TestBuildSchemaClient_TLSUntrustedWithoutCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]string{})
	}))
	t.Cleanup(srv.Close)

	c := srTLSCluster(srv.URL, &v1alpha1.TLSConfig{Enabled: true})
	sc, err := BuildSchemaClient(c, secrets.FileEnvResolver{BaseDir: "."})
	require.NoError(t, err)
	_, err = sc.ListSubjects(context.Background())
	require.Error(t, err, "system trust store must not trust the test server's self-signed cert")
}

// TestBuildHTTPClientTLS_CACertPoolFromFileCLI confirms the shared helper
// resolves tls.caCert from a file reference (CLI resolver) and populates the
// Transport's RootCAs with exactly that CA. Mirrors
// TestBuildConnConfig_TLSCACertFromFileCLI in internal/kafka/franz.
func TestBuildHTTPClientTLS_CACertPoolFromFileCLI(t *testing.T) {
	dir := t.TempDir()
	caPEM := selfSignedCAPEM(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ca.crt"), caPEM, 0o600))

	tlsSpec := &v1alpha1.TLSConfig{
		Enabled: true,
		CACert:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "ca.crt"}},
	}
	hc, err := buildHTTPClientTLS(tlsSpec, secrets.FileEnvResolver{BaseDir: dir}, "schemaRegistry")
	require.NoError(t, err)

	transport, ok := hc.Transport.(*http.Transport)
	require.True(t, ok, "Transport should be *http.Transport")
	require.NotNil(t, transport.TLSClientConfig)
	require.NotNil(t, transport.TLSClientConfig.RootCAs)

	want := x509.NewCertPool()
	require.True(t, want.AppendCertsFromPEM(caPEM), "test bug: expectation PEM does not parse")
	require.True(t, transport.TLSClientConfig.RootCAs.Equal(want),
		"RootCAs does not contain (only) the CA cert from tls.caCert file ref")
	require.False(t, transport.TLSClientConfig.RootCAs.Equal(x509.NewCertPool()),
		"RootCAs equals an empty pool, want the injected CA")
}

// TestBuildHTTPClientTLS_CACertPopulatesPoolOperator confirms a resolver
// returning a valid PEM for a secretKeyRef source (the operator plumbing)
// populates RootCAs. Mirrors TestBuildConnConfig_TLSCACertPopulatesPool in
// internal/kafka/franz.
func TestBuildHTTPClientTLS_CACertPopulatesPoolOperator(t *testing.T) {
	caPEM := selfSignedCAPEM(t)
	tlsSpec := &v1alpha1.TLSConfig{
		Enabled: true,
		CACert:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "ca", Key: "ca.crt"}}},
	}
	hc, err := buildHTTPClientTLS(tlsSpec, stubResolver{val: string(caPEM)}, "schemaRegistry")
	require.NoError(t, err)
	transport := hc.Transport.(*http.Transport)
	require.NotNil(t, transport.TLSClientConfig.RootCAs)
	want := x509.NewCertPool()
	require.True(t, want.AppendCertsFromPEM(caPEM))
	require.True(t, transport.TLSClientConfig.RootCAs.Equal(want))
}

// TestBuildSchemaClient_TLSCACertSecretKeyRefRejectedCLI confirms that with
// the CLI FileEnvResolver a schemaRegistry tls.caCert via secretKeyRef errors
// clearly (k8s-only) and the error names the SR field path.
func TestBuildSchemaClient_TLSCACertSecretKeyRefRejectedCLI(t *testing.T) {
	c := srTLSCluster("https://sr:8081", &v1alpha1.TLSConfig{
		Enabled: true,
		CACert:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "ca", Key: "ca.crt"}}},
	})
	_, err := BuildSchemaClient(c, secrets.FileEnvResolver{BaseDir: "."})
	require.Error(t, err)
	require.Contains(t, err.Error(), "schemaRegistry tls.caCert")
	require.Contains(t, err.Error(), "secretKeyRef")
}

// TestBuildSchemaClient_TLSInvalidCACert confirms a non-PEM schemaRegistry
// tls.caCert payload errors clearly, mentions the field path, and does not
// leak the (bogus) payload. Mirrors TestBuildHTTPClientTLS_InvalidCACert_MDSLabel.
func TestBuildSchemaClient_TLSInvalidCACert(t *testing.T) {
	c := srTLSCluster("https://sr:8081", &v1alpha1.TLSConfig{
		Enabled: true,
		CACert:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "ca", Key: "ca.crt"}}},
	})
	_, err := BuildSchemaClient(c, stubResolver{val: "not a pem"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "schemaRegistry tls.caCert")
	// SECURITY: must not echo the (rejected) payload material.
	require.NotContains(t, err.Error(), "not a pem")
}

// TestBuildHTTPClientTLS_MTLSKeypair confirms an mTLS client cert/key pair
// loads onto the Transport's TLS config.
func TestBuildHTTPClientTLS_MTLSKeypair(t *testing.T) {
	certPEM, keyPEM := selfSignedClientPEM(t)
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "client.crt"), []byte(certPEM), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "client.key"), []byte(keyPEM), 0o600))

	tlsSpec := &v1alpha1.TLSConfig{
		Enabled:    true,
		ClientCert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.crt"}},
		ClientKey:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "client.key"}},
	}
	hc, err := buildHTTPClientTLS(tlsSpec, secrets.FileEnvResolver{BaseDir: dir}, "schemaRegistry")
	require.NoError(t, err)
	transport := hc.Transport.(*http.Transport)
	require.Len(t, transport.TLSClientConfig.Certificates, 1)
}

// TestBuildHTTPClientTLS_InvalidCACert_MDSLabel confirms a non-PEM mds
// tls.caCert payload errors clearly, mentions the field path, and does not
// leak the (bogus) payload in the error message. Mirrors
// TestBuildConnConfig_TLSCACertInvalidPEM in internal/kafka/franz.
func TestBuildHTTPClientTLS_InvalidCACert_MDSLabel(t *testing.T) {
	tlsSpec := &v1alpha1.TLSConfig{
		Enabled: true,
		CACert:  &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "ca", Key: "ca.crt"}}},
	}
	_, err := buildHTTPClientTLS(tlsSpec, stubResolver{val: "not a pem"}, "mds")
	require.Error(t, err)
	require.Contains(t, err.Error(), "mds tls.caCert")
	// SECURITY: must not echo the (rejected) payload material.
	require.NotContains(t, err.Error(), "not a pem")
}
