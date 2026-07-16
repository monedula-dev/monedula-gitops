package controller

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// CredentialSourceLabel marks a Secret as a watched credential/TLS source. The
// operator caches/watches only Secrets carrying this label = "true" (§11.4); a
// change to such a Secret reconciles the referencing KafkaCluster AND the
// data-plane resources on it promptly instead of at the periodic resync.
const CredentialSourceLabel = "gitops.monedula.dev/credential-source"

// CredentialSourceLabelValue is the value CredentialSourceLabel must carry to
// opt a Secret into the watch.
const CredentialSourceLabelValue = "true"

// ClusterSecretNamesIndex is the KafkaCluster field index of referenced Secret
// names (across every secretKeyRef-bearing spec field). It lets the watch
// map-funcs List the clusters that reference a changed Secret.
const ClusterSecretNamesIndex = "spec.secretRefs"

// clusterSecretNames returns the de-duplicated Secret names a cluster references
// via secretKeyRef across every credential/TLS-bearing field: tls (caCert,
// clientCert, clientKey), auth.scram (username/password), auth.oauth
// (clientId/clientSecret/tokenEndpointCA — the IdP's own trust domain,
// distinct from tls.caCert), schemaRegistry (auth username/password +
// schemaRegistry.tls), and authorization.mds (auth username/password/token +
// mds.tls). Only secretKeyRef sources count — inline/env/configMapKeyRef
// reference no Secret. Empty when the cluster references no Secret. Safe on a
// nil cluster.
func clusterSecretNames(c *v1alpha1.KafkaCluster) []string {
	if c == nil {
		return nil
	}
	seen := map[string]struct{}{}
	var out []string
	addName := func(n string) {
		if n == "" {
			return
		}
		if _, ok := seen[n]; ok {
			return
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	addRef := func(ref *v1alpha1.SecretKeyRef) {
		if ref != nil {
			addName(ref.Name)
		}
	}
	// addVF reads the secretKeyRef from a *ValueFrom (pointer-typed fields).
	addVF := func(vf *v1alpha1.ValueFrom) {
		if vf != nil {
			addRef(vf.ValueFrom.SecretKeyRef)
		}
	}
	// addVal reads the secretKeyRef from a ValueFrom value (non-pointer fields).
	addVal := func(vf v1alpha1.ValueFrom) {
		addRef(vf.ValueFrom.SecretKeyRef)
	}

	// addTLS collects the secretKeyRefs from a TLS block (used for spec.tls,
	// schemaRegistry.tls, and authorization.mds.tls).
	addTLS := func(tls *v1alpha1.TLSConfig) {
		if tls == nil {
			return
		}
		addVF(tls.CACert)
		addVF(tls.ClientCert)
		addVF(tls.ClientKey)
	}

	addTLS(c.Spec.TLS)

	if c.Spec.Auth != nil {
		if c.Spec.Auth.SCRAM != nil {
			addVal(c.Spec.Auth.SCRAM.Username)
			addVal(c.Spec.Auth.SCRAM.Password)
		}
		if c.Spec.Auth.OAuth != nil {
			addVal(c.Spec.Auth.OAuth.ClientID)
			addVal(c.Spec.Auth.OAuth.ClientSecret)
			addVF(c.Spec.Auth.OAuth.TokenEndpointCA)
		}
	}

	if c.Spec.SchemaRegistry != nil {
		if c.Spec.SchemaRegistry.Auth != nil {
			addVal(c.Spec.SchemaRegistry.Auth.Username)
			addVal(c.Spec.SchemaRegistry.Auth.Password)
		}
		addTLS(c.Spec.SchemaRegistry.TLS)
	}

	if c.Spec.Authorization != nil && c.Spec.Authorization.MDS != nil {
		mds := c.Spec.Authorization.MDS
		if mds.Auth != nil {
			addVF(mds.Auth.Username)
			addVF(mds.Auth.Password)
			addVF(mds.Auth.Token)
		}
		addTLS(mds.TLS)
	}

	return out
}

// RegisterClusterSecretNamesIndex registers ClusterSecretNamesIndex on
// KafkaCluster. Call once before mgr.Start (alongside the other index
// registrations in manager.Run).
func RegisterClusterSecretNamesIndex(ctx context.Context, mgr ctrl.Manager) error {
	return mgr.GetFieldIndexer().IndexField(ctx, &v1alpha1.KafkaCluster{}, ClusterSecretNamesIndex,
		func(obj client.Object) []string {
			c, ok := obj.(*v1alpha1.KafkaCluster)
			if !ok {
				return nil
			}
			return clusterSecretNames(c)
		})
}

// UserPasswordSecretNamesIndex is the KafkaUser field index of the password
// Secret name (spec.password.valueFrom.secretKeyRef.name). It lets the
// KafkaUser Secret watch map a changed password Secret directly to the
// referencing users (the event-driven rotation trigger, §11.4) without
// listing every user. Generated Secrets are deliberately NOT indexed: they
// carry a controller owner reference, so the Owns() watch handles them.
const UserPasswordSecretNamesIndex = "spec.password.secretRefs"

// userPasswordSecretNames returns the Secret name a KafkaUser's password
// references via secretKeyRef, if any. Only secretKeyRef counts —
// env/file/inline/configMapKeyRef reference no Secret, and generate mode's
// Secret is owner-referenced instead. Safe on a nil user.
func userPasswordSecretNames(u *v1alpha1.KafkaUser) []string {
	if u == nil || u.Spec.Password == nil || u.Spec.Password.ValueFrom == nil {
		return nil
	}
	if ref := u.Spec.Password.ValueFrom.SecretKeyRef; ref != nil && ref.Name != "" {
		return []string{ref.Name}
	}
	return nil
}

// RegisterUserPasswordSecretNamesIndex registers UserPasswordSecretNamesIndex
// on KafkaUser. Call once before mgr.Start (alongside the other index
// registrations in manager.Run).
func RegisterUserPasswordSecretNamesIndex(ctx context.Context, mgr ctrl.Manager) error {
	return mgr.GetFieldIndexer().IndexField(ctx, &v1alpha1.KafkaUser{}, UserPasswordSecretNamesIndex,
		func(obj client.Object) []string {
			u, ok := obj.(*v1alpha1.KafkaUser)
			if !ok {
				return nil
			}
			return userPasswordSecretNames(u)
		})
}

// clustersReferencingSecret returns the names of KafkaClusters in the Secret's
// namespace that reference it (via ClusterSecretNamesIndex). It is the shared
// first hop of every Secret map-func: a Secret is referenced only by a
// KafkaCluster.spec, so the watch resolves Secret → cluster(s) here, and each
// controller's map-func then fans out to its own kind. A List error yields nil
// (the periodic resync remains the backstop).
func clustersReferencingSecret(ctx context.Context, c client.Client, secret client.Object) []string {
	var clusters v1alpha1.KafkaClusterList
	if err := c.List(ctx, &clusters,
		client.InNamespace(secret.GetNamespace()),
		client.MatchingFields{ClusterSecretNamesIndex: secret.GetName()}); err != nil {
		return nil
	}
	out := make([]string, 0, len(clusters.Items))
	for i := range clusters.Items {
		out = append(out, clusters.Items[i].Name)
	}
	return out
}
