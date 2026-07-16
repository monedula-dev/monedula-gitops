// Package index owns the manager-cache field indexes shared by the operator's
// controllers AND its admission webhooks. It is deliberately neutral: the
// controllers' cluster-watch fan-out and the webhooks' duplicate-identity
// scans both List by spec.clusterRef.name, and hosting the index keys (and
// their registration) here keeps the controller packages from importing the
// webhook package just for constants (the layering inversion this package
// fixes). The webhook package re-exports the names it historically declared,
// so existing references keep working.
//
// All five indexes share the same field-path string — the field indexer is
// keyed per object TYPE, so they do not collide.
package index

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// ClusterRefNameIndex is the manager cache field-index key on a KafkaTopic's
// spec.clusterRef.name. The KafkaTopic webhook lists candidate duplicates
// filtered by this index so it scans only topics referencing the same cluster
// ref, not every topic; the controllers' cluster watch uses it the same way.
// Registered unconditionally in RegisterIndexes.
const ClusterRefNameIndex = "spec.clusterRef.name"

// QuotaClusterRefNameIndex is the manager cache field-index key on a
// KafkaQuota's spec.clusterRef.name (the KafkaQuota analogue of
// ClusterRefNameIndex). Registered unconditionally in RegisterIndexes.
const QuotaClusterRefNameIndex = "spec.clusterRef.name"

// PolicyClusterRefNameIndex is the manager cache field-index key on a
// KafkaAccessPolicy's spec.clusterRef.name (the KafkaAccessPolicy analogue of
// ClusterRefNameIndex). Registered unconditionally in RegisterIndexes.
const PolicyClusterRefNameIndex = "spec.clusterRef.name"

// RoleBindingClusterRefNameIndex is the manager cache field-index key on a
// KafkaRoleBinding's spec.clusterRef.name (the KafkaRoleBinding analogue of
// ClusterRefNameIndex). Registered unconditionally in RegisterIndexes.
const RoleBindingClusterRefNameIndex = "spec.clusterRef.name"

// UserClusterRefNameIndex is the manager cache field-index key on a
// KafkaUser's spec.clusterRef.name (the KafkaUser analogue of
// ClusterRefNameIndex). Registered unconditionally in RegisterIndexes.
const UserClusterRefNameIndex = "spec.clusterRef.name"

// RegisterIndexes registers the manager-cache field indexes on KafkaTopic,
// KafkaQuota, KafkaAccessPolicy, KafkaRoleBinding, and KafkaUser
// spec.clusterRef.name (ClusterRefNameIndex / QuotaClusterRefNameIndex /
// PolicyClusterRefNameIndex / RoleBindingClusterRefNameIndex /
// UserClusterRefNameIndex).
// The indexes let the webhook validators (and controllers) List by cluster
// ref efficiently. They are cheap and registered unconditionally —
// independent of whether webhooks are enabled — so the cache always carries
// them. All five indexes share the same field-path string but are keyed per
// object TYPE, so they do not collide. Call once, before mgr.Start.
func RegisterIndexes(ctx context.Context, mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(ctx, &v1alpha1.KafkaTopic{}, ClusterRefNameIndex,
		func(obj client.Object) []string {
			t, ok := obj.(*v1alpha1.KafkaTopic)
			if !ok {
				return nil
			}
			return []string{t.Spec.ClusterRef.Name}
		}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &v1alpha1.KafkaQuota{}, QuotaClusterRefNameIndex,
		func(obj client.Object) []string {
			q, ok := obj.(*v1alpha1.KafkaQuota)
			if !ok {
				return nil
			}
			return []string{q.Spec.ClusterRef.Name}
		}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &v1alpha1.KafkaAccessPolicy{}, PolicyClusterRefNameIndex,
		func(obj client.Object) []string {
			p, ok := obj.(*v1alpha1.KafkaAccessPolicy)
			if !ok {
				return nil
			}
			return []string{p.Spec.ClusterRef.Name}
		}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &v1alpha1.KafkaRoleBinding{}, RoleBindingClusterRefNameIndex,
		func(obj client.Object) []string {
			rb, ok := obj.(*v1alpha1.KafkaRoleBinding)
			if !ok {
				return nil
			}
			return []string{rb.Spec.ClusterRef.Name}
		}); err != nil {
		return err
	}
	return mgr.GetFieldIndexer().IndexField(ctx, &v1alpha1.KafkaUser{}, UserClusterRefNameIndex,
		func(obj client.Object) []string {
			u, ok := obj.(*v1alpha1.KafkaUser)
			if !ok {
				return nil
			}
			return []string{u.Spec.ClusterRef.Name}
		})
}
