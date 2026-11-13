package operator

import (
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// withPolicy installs p for the duration of the test.
func withPolicy(t *testing.T, p SecretAccessPolicy) {
	t.Helper()
	prev := CurrentSecretAccessPolicy()
	SetSecretAccessPolicy(p)
	t.Cleanup(func() { SetSecretAccessPolicy(prev) })
}

func credsRef() v1alpha1.ValueFrom {
	return v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
		SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "creds", Key: "username"},
	}}
}

// A CR that merely names a Secret must not make the operator read it unless
// the Secret was opted in with the credential-source label.
func TestSecretPolicy_UnlabelledSecretRejected(t *testing.T) {
	withPolicy(t, SecretAccessPolicy{RequireLabel: true})
	sec := secretObj()
	sec.Labels = nil
	_, err := newResolver(t, sec).Resolve(credsRef())
	if err == nil || !strings.Contains(err.Error(), CredentialSourceLabel) {
		t.Fatalf("want a not-labelled error naming %s, got %v", CredentialSourceLabel, err)
	}
	if strings.Contains(err.Error(), "alice") {
		t.Fatalf("error must not leak the Secret value: %v", err)
	}
}

func TestSecretPolicy_LabelNotRequired(t *testing.T) {
	withPolicy(t, SecretAccessPolicy{RequireLabel: false})
	sec := secretObj()
	sec.Labels = nil
	if got, err := newResolver(t, sec).Resolve(credsRef()); err != nil || got != "alice" {
		t.Fatalf("with --require-secret-label=false an unlabelled Secret is readable; got %q, %v", got, err)
	}
}

func TestSecretPolicy_NamespaceOutOfScope(t *testing.T) {
	withPolicy(t, SecretAccessPolicy{RequireLabel: true, Namespaces: []string{"kafka-system"}})
	_, err := newResolver(t, secretObj()).Resolve(credsRef()) // resolver namespace ns1
	if err == nil || !strings.Contains(err.Error(), "--secret-namespaces") {
		t.Fatalf("want an out-of-scope namespace error, got %v", err)
	}
	if err := CheckSecretNamespace("kafka-system"); err != nil {
		t.Fatalf("listed namespace must be allowed: %v", err)
	}
}

func TestSecretPolicy_DefaultRequiresLabel(t *testing.T) {
	if !(SecretAccessPolicy{RequireLabel: true}).RequireLabel || !CurrentSecretAccessPolicy().RequireLabel {
		t.Fatal("the process default must require the credential-source label")
	}
}
