package controller

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// topicWithSchemaCMs builds a KafkaTopic with ConfigMap-sourced key and/or
// value schema refs. Pass "" to omit either side.
func topicWithSchemaCMs(key, val string) *v1alpha1.KafkaTopic {
	t := &v1alpha1.KafkaTopic{}
	t.Spec.Schema = &v1alpha1.TopicSchema{}
	if key != "" {
		t.Spec.Schema.KeySchema = &v1alpha1.ValueFrom{
			ValueFrom: v1alpha1.ValueSource{ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: key, Key: "k.avsc"}},
		}
	}
	if val != "" {
		t.Spec.Schema.ValueSchema = &v1alpha1.ValueFrom{
			ValueFrom: v1alpha1.ValueSource{ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: val, Key: "v.avsc"}},
		}
	}
	return t
}

func TestSchemaConfigMapNamesNone(t *testing.T) {
	require.Empty(t, schemaConfigMapNames(&v1alpha1.KafkaTopic{}))
	require.Empty(t, schemaConfigMapNames(topicWithSchemaCMs("", "")))
}

func TestSchemaConfigMapNamesKeyAndValue(t *testing.T) {
	got := schemaConfigMapNames(topicWithSchemaCMs("cm-key", "cm-val"))
	require.ElementsMatch(t, []string{"cm-key", "cm-val"}, got)
}

func TestSchemaConfigMapNamesDeduplicates(t *testing.T) {
	got := schemaConfigMapNames(topicWithSchemaCMs("shared", "shared"))
	require.Equal(t, []string{"shared"}, got)
}
