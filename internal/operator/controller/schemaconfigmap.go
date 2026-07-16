package controller

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// SchemaSourceLabel marks a ConfigMap as a watched schema source. The operator
// caches/watches only ConfigMaps carrying this label = "true" (§11.3); a change
// to such a ConfigMap reconciles the referencing KafkaTopics promptly instead of
// at the periodic resync.
const SchemaSourceLabel = "gitops.monedula.dev/schema-source"

// SchemaSourceLabelValue is the value SchemaSourceLabel must carry to opt a
// ConfigMap into the watch.
const SchemaSourceLabelValue = "true"

// SchemaConfigMapIndex is the KafkaTopic field index of referenced schema
// ConfigMap names (across key + value schema configMapKeyRef). It lets the watch
// map-func List the topics that reference a changed ConfigMap.
const SchemaConfigMapIndex = "spec.schema.configMapRefs"

// schemaConfigMapNames returns the de-duplicated ConfigMap names a topic
// references for its key/value schema bodies (configMapKeyRef). Empty when the
// topic has no ConfigMap-sourced schema.
//
// ValueFrom is a value (not pointer) on TopicSchema fields, so only the outer
// *ValueFrom pointer is nil-checked; ValueFrom.ValueFrom (ValueSource) is always
// present when the outer pointer is non-nil.
func schemaConfigMapNames(t *v1alpha1.KafkaTopic) []string {
	if t == nil || t.Spec.Schema == nil {
		return nil
	}
	seen := map[string]struct{}{}
	var out []string
	add := func(ref *v1alpha1.ValueFrom) {
		if ref == nil || ref.ValueFrom.ConfigMapKeyRef == nil {
			return
		}
		n := ref.ValueFrom.ConfigMapKeyRef.Name
		if n == "" {
			return
		}
		if _, ok := seen[n]; ok {
			return
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	add(t.Spec.Schema.KeySchema)
	add(t.Spec.Schema.ValueSchema)
	return out
}

// RegisterSchemaConfigMapIndex registers SchemaConfigMapIndex on KafkaTopic.
// Call once before mgr.Start (alongside webhook.RegisterIndexes).
func RegisterSchemaConfigMapIndex(ctx context.Context, mgr ctrl.Manager) error {
	return mgr.GetFieldIndexer().IndexField(ctx, &v1alpha1.KafkaTopic{}, SchemaConfigMapIndex,
		func(obj client.Object) []string {
			t, ok := obj.(*v1alpha1.KafkaTopic)
			if !ok {
				return nil
			}
			return schemaConfigMapNames(t)
		})
}
