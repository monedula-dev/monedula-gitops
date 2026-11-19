package operator

import (
	"fmt"
	"slices"
	"sync"

	corev1 "k8s.io/api/core/v1"
)

// CredentialSourceLabel marks a Secret as a credential source the operator may
// use. It serves two purposes:
//
//   - opt-in (security): when SecretAccessPolicy.RequireLabel is on (the
//     default), the operator only reads a REFERENCED Secret — KafkaCluster
//     credentials and TLS material, KafkaTopic schema bodies, KafkaUser
//     passwords — if it carries this label. Only a principal allowed to write
//     a Secret can label it, so being able to create a CR that references a
//     Secret is no longer enough to make the operator read it and send its
//     contents to an endpoint the CR author chose (a confused deputy);
//   - watch (§11.4): the operator caches/watches only Secrets with this label,
//     so a change reconciles the referencing resources promptly.
const CredentialSourceLabel = "gitops.monedula.dev/credential-source"

// CredentialSourceLabelValue is the value CredentialSourceLabel must carry.
const CredentialSourceLabelValue = "true"

// SecretAccessPolicy restricts which Secrets the operator reads on behalf of
// a custom resource. The zero value is permissive; production code installs
// the configured policy once at startup via SetSecretAccessPolicy.
type SecretAccessPolicy struct {
	// RequireLabel rejects referenced Secrets without
	// CredentialSourceLabel=CredentialSourceLabelValue.
	RequireLabel bool
	// Namespaces, when non-empty, is the only set of namespaces the operator
	// may read or write Secrets in (it then has RBAC there only).
	Namespaces []string
}

var (
	secretPolicyMu sync.RWMutex
	secretPolicy   = SecretAccessPolicy{RequireLabel: true}
)

// SetSecretAccessPolicy installs p as the process-wide Secret access policy.
// Called once by the operator manager before controllers start; tests may
// call it (and restore the previous value) to exercise either mode.
func SetSecretAccessPolicy(p SecretAccessPolicy) {
	secretPolicyMu.Lock()
	defer secretPolicyMu.Unlock()
	secretPolicy = SecretAccessPolicy{RequireLabel: p.RequireLabel, Namespaces: slices.Clone(p.Namespaces)}
}

// CurrentSecretAccessPolicy returns the installed Secret access policy.
func CurrentSecretAccessPolicy() SecretAccessPolicy {
	secretPolicyMu.RLock()
	defer secretPolicyMu.RUnlock()
	return SecretAccessPolicy{RequireLabel: secretPolicy.RequireLabel, Namespaces: slices.Clone(secretPolicy.Namespaces)}
}

// CheckSecretNamespace returns an error when the policy confines Secret
// access to a namespace list that does not include namespace.
func CheckSecretNamespace(namespace string) error {
	p := CurrentSecretAccessPolicy()
	if len(p.Namespaces) == 0 || slices.Contains(p.Namespaces, namespace) {
		return nil
	}
	return fmt.Errorf("secrets in namespace %q are outside the operator's --secret-namespaces %v", namespace, p.Namespaces)
}

// HasCredentialSourceLabel reports whether sec carries the opt-in label.
func HasCredentialSourceLabel(sec *corev1.Secret) bool {
	return sec.Labels[CredentialSourceLabel] == CredentialSourceLabelValue
}

// CheckReferencedSecret enforces the policy on a Secret the operator read
// because a custom resource referenced it. The error names the Secret and the
// missing label, never its contents.
func CheckReferencedSecret(sec *corev1.Secret) error {
	if !CurrentSecretAccessPolicy().RequireLabel || HasCredentialSourceLabel(sec) {
		return nil
	}
	return fmt.Errorf("secret %s/%s is not labelled %s=%s; the operator only uses Secrets opted in with this label "+
		"(add it with kubectl label secret, or start the operator with --require-secret-label=false)",
		sec.Namespace, sec.Name, CredentialSourceLabel, CredentialSourceLabelValue)
}
