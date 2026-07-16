package controller

import (
	"context"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/operator/index"
)

// clusterWatchPredicate is the event filter for the data-plane controllers'
// KafkaCluster watch (review I2). It fans out:
//
//   - Create: the dependent-before-cluster case — a KafkaTopic created before
//     its KafkaCluster sits in ClusterNotFound error backoff (up to ~16m);
//     the cluster's Create event recovers it immediately.
//   - Update, only when metadata.generation changed (a spec write): fixing a
//     cluster's bootstrap servers, credentials refs, or adding
//     authorization.mds must promptly re-reconcile dependents. Status-only
//     updates (the KafkaCluster controller's own writes, every reconcile) do
//     NOT bump generation and are filtered — without this the cluster
//     controller's 5-minute resync would storm every dependent on the cluster.
//     Annotation-only changes are likewise irrelevant to dependents.
//   - Delete: dependents transition to their cluster-missing condition
//     promptly instead of at their next resync.
//
// GenerationChangedPredicate implements only Update; Create/Delete/Generic
// pass through its embedded defaults, which is exactly the shape above.
func clusterWatchPredicate() predicate.Predicate {
	return predicate.GenerationChangedPredicate{}
}

// mapClusterToDependents is the shared core of the five data-plane controllers'
// KafkaCluster map funcs: it returns a reconcile request for every item in
// list (a typed *XxxList) whose spec.clusterRef.name equals the changed
// cluster's name, using the per-type spec.clusterRef.name field index
// registered by index.RegisterIndexes.
//
// Namespace scoping mirrors the controllers' own clusterRef resolution (and
// buildClusterACLView):
//
//   - namespace-local mode (clusterNamespace == ""): a KafkaCluster in
//     namespace X serves only dependents in X, so the list is scoped to the
//     cluster's namespace;
//   - --cluster-namespace mode: every dependent (any namespace) resolves its
//     clusterRef into clusterNamespace, so an event for a cluster in that
//     namespace fans out cluster-wide — and an event for a KafkaCluster in any
//     OTHER namespace is irrelevant to every dependent and maps to nothing.
//
// A List failure returns nil (the event is dropped); the dependents are then
// picked up by their periodic resync / error backoff as before this watch
// existed, so the failure mode is "no worse than the old behavior".
//
// Deliberate divergence from buildClusterACLView: dependents with a non-zero
// DeletionTimestamp are NOT skipped here — their own reconcile handles the
// deleting case, so the enqueue is harmless and skipping it would risk
// leaving a deleting dependent un-reconciled. Do not "fix" this into parity.
func mapClusterToDependents(ctx context.Context, c client.Client, clusterNamespace string,
	obj client.Object, list client.ObjectList, indexKey string) []ctrlreconcile.Request {
	cluster, ok := obj.(*v1alpha1.KafkaCluster)
	if !ok {
		return nil
	}
	listOpts := []client.ListOption{client.MatchingFields{indexKey: cluster.Name}}
	if clusterNamespace == "" {
		listOpts = append(listOpts, client.InNamespace(cluster.Namespace))
	} else if cluster.Namespace != clusterNamespace {
		return nil // dependents only resolve clusters in clusterNamespace
	}
	if err := c.List(ctx, list, listOpts...); err != nil {
		return nil
	}
	items, err := apimeta.ExtractList(list)
	if err != nil {
		return nil
	}
	out := make([]ctrlreconcile.Request, 0, len(items))
	for _, it := range items {
		o, ok := it.(client.Object)
		if !ok {
			continue
		}
		out = append(out, ctrlreconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: o.GetNamespace(),
			Name:      o.GetName(),
		}})
	}
	return out
}

// mapClusterToTopics enqueues the KafkaTopics whose clusterRef resolves to the
// changed KafkaCluster (review I2). See mapClusterToDependents for scoping.
func (r *KafkaTopicReconciler) mapClusterToTopics(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
	return mapClusterToDependents(ctx, r.Client, r.ClusterNamespace, obj,
		&v1alpha1.KafkaTopicList{}, index.ClusterRefNameIndex)
}

// mapClusterToPolicies enqueues the KafkaAccessPolicies whose clusterRef
// resolves to the changed KafkaCluster (review I2).
func (r *KafkaAccessPolicyReconciler) mapClusterToPolicies(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
	return mapClusterToDependents(ctx, r.Client, r.ClusterNamespace, obj,
		&v1alpha1.KafkaAccessPolicyList{}, index.PolicyClusterRefNameIndex)
}

// mapClusterToQuotas enqueues the KafkaQuotas whose clusterRef resolves to the
// changed KafkaCluster (review I2).
func (r *KafkaQuotaReconciler) mapClusterToQuotas(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
	return mapClusterToDependents(ctx, r.Client, r.ClusterNamespace, obj,
		&v1alpha1.KafkaQuotaList{}, index.QuotaClusterRefNameIndex)
}

// mapClusterToUsers enqueues the KafkaUsers whose clusterRef resolves to the
// changed KafkaCluster (review I2).
func (r *KafkaUserReconciler) mapClusterToUsers(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
	return mapClusterToDependents(ctx, r.Client, r.ClusterNamespace, obj,
		&v1alpha1.KafkaUserList{}, index.UserClusterRefNameIndex)
}

// mapClusterToRoleBindings enqueues the KafkaRoleBindings whose clusterRef
// resolves to the changed KafkaCluster (review I2). This is also what un-wedges
// a binding stuck in MDSNotConfigured the moment authorization.mds is added to
// the cluster spec.
func (r *KafkaRoleBindingReconciler) mapClusterToRoleBindings(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
	return mapClusterToDependents(ctx, r.Client, r.ClusterNamespace, obj,
		&v1alpha1.KafkaRoleBindingList{}, index.RoleBindingClusterRefNameIndex)
}
