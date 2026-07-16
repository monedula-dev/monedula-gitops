// Package operator contains the controller-runtime-facing glue for the Monedula
// GitOps operator: the Kubernetes-Secret backed secrets.Resolver used in
// operator mode, and shared helpers wiring the reconcile core to controllers.
package operator

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// K8sResolver resolves secret references from Kubernetes Secrets and ConfigMaps,
// for operator mode. It supports secretKeyRef, inline, and configMapKeyRef
// sources; env/file references (the CLI FileEnvResolver modes) are rejected,
// since the operator never reads the host environment or filesystem. Resolved
// values are never logged or embedded in returned errors.
type K8sResolver struct {
	// Client is a controller-runtime client used to read Secrets and ConfigMaps.
	Client client.Client
	// Namespace is the namespace to read Secrets/ConfigMaps from (the managed
	// resource's namespace; refs are namespace-local).
	Namespace string
	// Ctx is the context used for client reads. The secrets.Resolver interface
	// (Resolve) takes no ctx parameter, so the per-reconcile context is carried
	// on the struct; a fresh resolver is built each reconcile.
	Ctx context.Context //nolint:containedctx // required by secrets.Resolver interface (no ctx param); fresh per reconcile
}

// compile-time assertion that K8sResolver implements Resolver.
var _ secrets.Resolver = (*K8sResolver)(nil)

// Resolve returns the plaintext for a value reference. Supported sources:
//   - inline: returns the literal value verbatim.
//   - secretKeyRef: reads Secret <Namespace>/<ref.Name> and returns data[ref.Key].
//   - configMapKeyRef: reads ConfigMap <Namespace>/<ref.Name> and returns data[ref.Key].
//     ConfigMaps labelled gitops.monedula.dev/schema-source="true" are watched (§11.3)
//     and reconcile the referencing topics promptly; unlabelled ConfigMaps rely on the
//     periodic resync.
//   - env/file: rejected (use secretKeyRef or inline in operator mode).
func (r *K8sResolver) Resolve(vf v1alpha1.ValueFrom) (string, error) {
	src := vf.ValueFrom
	switch {
	case src.Inline != "":
		return src.Inline, nil
	case src.SecretKeyRef != nil:
		b, err := r.readKey(src.SecretKeyRef)
		if err != nil {
			return "", err
		}
		return string(b), nil
	case src.ConfigMapKeyRef != nil:
		v, err := r.readConfigMapKey(src.ConfigMapKeyRef)
		if err != nil {
			return "", err
		}
		return v, nil
	case src.Env != "" || src.File != "":
		return "", errors.New("env/file secret refs are not supported in operator mode (use secretKeyRef)")
	default:
		return "", errors.New("no secret source specified (set secretKeyRef)")
	}
}

// readKey fetches Secret <Namespace>/<ref.Name> and returns data[ref.Key],
// erroring if the Secret or the key is absent. The returned bytes are the raw
// Secret value and must never be logged.
func (r *K8sResolver) readKey(ref *v1alpha1.SecretKeyRef) ([]byte, error) {
	var sec corev1.Secret
	key := types.NamespacedName{Namespace: r.Namespace, Name: ref.Name}
	if err := r.Client.Get(r.Ctx, key, &sec); err != nil {
		return nil, fmt.Errorf("reading secret %s: %w", key, err)
	}
	v, ok := sec.Data[ref.Key]
	if !ok {
		return nil, fmt.Errorf("secret %s has no key %q", key, ref.Key)
	}
	return v, nil
}

// readConfigMapKey fetches ConfigMap <Namespace>/<ref.Name> and returns
// data[ref.Key], erroring if the ConfigMap or the key is absent. ConfigMap.Data
// is map[string]string (no base64), unlike Secret.Data.
func (r *K8sResolver) readConfigMapKey(ref *v1alpha1.SecretKeyRef) (string, error) {
	var cm corev1.ConfigMap
	key := types.NamespacedName{Namespace: r.Namespace, Name: ref.Name}
	if err := r.Client.Get(r.Ctx, key, &cm); err != nil {
		return "", fmt.Errorf("reading configmap %s: %w", key, err)
	}
	v, ok := cm.Data[ref.Key]
	if !ok {
		return "", fmt.Errorf("configmap %s has no key %q", key, ref.Key)
	}
	return v, nil
}
