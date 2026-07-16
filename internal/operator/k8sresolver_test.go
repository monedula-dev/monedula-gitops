package operator

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/cluster"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

func newResolver(t *testing.T, objs ...interface{}) *K8sResolver {
	t.Helper()
	b := fake.NewClientBuilder()
	for _, o := range objs {
		switch obj := o.(type) {
		case *corev1.Secret:
			b = b.WithObjects(obj)
		case *corev1.ConfigMap:
			b = b.WithObjects(obj)
		}
	}
	return &K8sResolver{Client: b.Build(), Namespace: "ns1", Ctx: context.Background()}
}

func secretObj() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "ns1"},
		Data: map[string][]byte{
			"username": []byte("alice"),
			"ca.crt":   []byte("-----BEGIN CERTIFICATE-----\nPEM\n-----END CERTIFICATE-----"),
		},
	}
}

func TestK8sResolver_ImplementsResolver(t *testing.T) {
	var _ secrets.Resolver = (*K8sResolver)(nil)
}

func TestK8sResolver_ResolveSecretKeyRef(t *testing.T) {
	r := newResolver(t, secretObj())
	vf := v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
		SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "creds", Key: "username"},
	}}
	got, err := r.Resolve(vf)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != "alice" {
		t.Fatalf("got %q want %q", got, "alice")
	}
}

func TestK8sResolver_ResolveMissingSecret(t *testing.T) {
	r := newResolver(t) // no secret seeded
	vf := v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
		SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "creds", Key: "username"},
	}}
	if _, err := r.Resolve(vf); err == nil {
		t.Fatal("expected error for missing secret")
	}
}

func TestK8sResolver_ResolveMissingKey(t *testing.T) {
	r := newResolver(t, secretObj())
	vf := v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
		SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "creds", Key: "nope"},
	}}
	if _, err := r.Resolve(vf); err == nil {
		t.Fatal("expected error for missing key")
	}
}

func TestK8sResolver_ResolveEnvRejected(t *testing.T) {
	r := newResolver(t, secretObj())
	vf := v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "FOO"}}
	if _, err := r.Resolve(vf); err == nil {
		t.Fatal("expected error for env ref in operator mode")
	}
}

func TestK8sResolver_ResolveFileRejected(t *testing.T) {
	r := newResolver(t, secretObj())
	vf := v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "/etc/foo"}}
	if _, err := r.Resolve(vf); err == nil {
		t.Fatal("expected error for file ref in operator mode")
	}
}

func TestK8sResolver_ResolveNoSource(t *testing.T) {
	r := newResolver(t, secretObj())
	vf := v1alpha1.ValueFrom{}
	if _, err := r.Resolve(vf); err == nil {
		t.Fatal("expected error for empty source")
	}
}

// TestK8sResolver_ResolveCACertSecretKeyRef confirms TLS CA material (a
// tls.caCert secretKeyRef) resolves through the ordinary Resolve path.
func TestK8sResolver_ResolveCACertSecretKeyRef(t *testing.T) {
	r := newResolver(t, secretObj())
	vf := v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
		SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "creds", Key: "ca.crt"},
	}}
	got, err := r.Resolve(vf)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != "-----BEGIN CERTIFICATE-----\nPEM\n-----END CERTIFICATE-----" {
		t.Fatalf("unexpected cert bytes: %q", got)
	}
}

// ---- Inline source ----

func TestK8sResolver_ResolveInlineVerbatim(t *testing.T) {
	r := newResolver(t)
	body := `{"type":"record","name":"Order","fields":[]}`
	vf := v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: body}}
	got, err := r.Resolve(vf)
	if err != nil {
		t.Fatalf("Resolve inline: %v", err)
	}
	if got != body {
		t.Fatalf("got %q want %q", got, body)
	}
}

func TestK8sResolver_ResolveInlinePreservesWhitespace(t *testing.T) {
	r := newResolver(t)
	body := "  leading\ntrailing  \n"
	vf := v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: body}}
	got, err := r.Resolve(vf)
	if err != nil {
		t.Fatalf("Resolve inline: %v", err)
	}
	if got != body {
		t.Fatalf("inline must be verbatim, got %q want %q", got, body)
	}
}

// ---- ConfigMapKeyRef source ----

func configMapObj() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "schemas", Namespace: "ns1"},
		Data: map[string]string{
			"order.avsc": `{"type":"record","name":"Order","fields":[]}`,
		},
	}
}

func TestK8sResolver_ResolveConfigMapKeyRef(t *testing.T) {
	r := newResolver(t, configMapObj())
	vf := v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
		ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: "schemas", Key: "order.avsc"},
	}}
	got, err := r.Resolve(vf)
	if err != nil {
		t.Fatalf("Resolve configMapKeyRef: %v", err)
	}
	want := `{"type":"record","name":"Order","fields":[]}`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestK8sResolver_ResolveConfigMapMissing(t *testing.T) {
	r := newResolver(t) // no ConfigMap seeded
	vf := v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
		ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: "schemas", Key: "order.avsc"},
	}}
	if _, err := r.Resolve(vf); err == nil {
		t.Fatal("expected error for missing configmap")
	}
}

func TestK8sResolver_ResolveConfigMapMissingKey(t *testing.T) {
	r := newResolver(t, configMapObj())
	vf := v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
		ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: "schemas", Key: "nope"},
	}}
	if _, err := r.Resolve(vf); err == nil {
		t.Fatal("expected error for missing configmap key")
	}
}

// ---- Schema Registry TLS via K8sResolver (operator path) ----

// selfSignedCAPEMForTest generates a throwaway self-signed CA cert PEM.
// Mirrors selfSignedPEM in internal/kafka/franz.
func selfSignedCAPEMForTest(t *testing.T) []byte {
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

// TestK8sResolver_SchemaRegistryTLSCACertSecretKeyRef confirms the operator
// plumbing end-to-end: a Schema Registry client builds with
// schemaRegistry.tls.caCert resolved from a Kubernetes Secret via K8sResolver.
// Mirrors TestK8sResolver_ResolveCACertSecretKeyRef for the SR TLS path.
func TestK8sResolver_SchemaRegistryTLSCACertSecretKeyRef(t *testing.T) {
	caSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "sr-ca", Namespace: "ns1"},
		Data:       map[string][]byte{"ca.crt": selfSignedCAPEMForTest(t)},
	}
	r := newResolver(t, caSecret)
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		SchemaRegistry: &v1alpha1.SchemaRegistryConf{
			Endpoint: "https://sr:8081",
			TLS: &v1alpha1.TLSConfig{
				Enabled: true,
				CACert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
					SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "sr-ca", Key: "ca.crt"},
				}},
			},
		},
	}}
	sc, err := cluster.BuildSchemaClient(c, r)
	if err != nil {
		t.Fatalf("BuildSchemaClient with SR tls.caCert secretKeyRef: %v", err)
	}
	if sc == nil {
		t.Fatal("BuildSchemaClient returned nil client")
	}
}

// TestK8sResolver_SchemaRegistryTLSCACertMissingSecret confirms a missing CA
// Secret surfaces as a build error naming the SR field path.
func TestK8sResolver_SchemaRegistryTLSCACertMissingSecret(t *testing.T) {
	r := newResolver(t) // no secret seeded
	c := &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{
		BootstrapServers: "a:9092",
		SchemaRegistry: &v1alpha1.SchemaRegistryConf{
			Endpoint: "https://sr:8081",
			TLS: &v1alpha1.TLSConfig{
				Enabled: true,
				CACert: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
					SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "sr-ca", Key: "ca.crt"},
				}},
			},
		},
	}}
	_, err := cluster.BuildSchemaClient(c, r)
	if err == nil {
		t.Fatal("expected error for missing SR CA secret")
	}
	if !strings.Contains(err.Error(), "schemaRegistry tls.caCert") {
		t.Fatalf("error %q does not mention schemaRegistry tls.caCert", err)
	}
}
