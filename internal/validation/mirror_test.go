package validation

import (
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func mkMirror(name, cluster, source, topicName string) *v1alpha1.KafkaMirrorTopic {
	return &v1alpha1.KafkaMirrorTopic{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaMirrorTopic"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.KafkaMirrorTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: cluster}, LinkName: "l",
			SourceTopicName: source, TopicName: topicName,
		},
	}
}

func mkRESTCluster(name string, rest *v1alpha1.KafkaRESTConfig) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1alpha1.KafkaClusterSpec{BootstrapServers: "b:9092", REST: rest},
	}
}

func errsText(errs []error) string {
	s := ""
	for _, e := range errs {
		s += e.Error() + "\n"
	}
	return s
}

func TestValidateMirrorDeletionPolicyDelete(t *testing.T) {
	m := mkMirror("m", "dr", "t", "")
	m.Spec.DeletionPolicy = "Delete"
	require.Contains(t, errsText(ValidateMirrorTopicShape(m)), "not supported")
	m.Spec.DeletionPolicy = "Orphan"
	require.Empty(t, ValidateMirrorTopicShape(m))
}

func TestValidateMirrorRequiresREST(t *testing.T) {
	errs := Validate(Input{
		MirrorTopics: []*v1alpha1.KafkaMirrorTopic{mkMirror("m", "dr", "t", "")},
		Clusters:     map[string]*v1alpha1.KafkaCluster{"dr": mkRESTCluster("dr", nil)},
	})
	require.Contains(t, errsText(errs), "spec.rest")
}

func TestValidateMirrorTopicCrossKindCollision(t *testing.T) {
	topic := &v1alpha1.KafkaTopic{
		TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaTopic"},
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec:       v1alpha1.KafkaTopicSpec{ClusterRef: v1alpha1.ClusterRef{Name: "dr"}, TopicName: "orders", Partitions: 1},
	}
	in := Input{
		Topics:       []*v1alpha1.KafkaTopic{topic},
		MirrorTopics: []*v1alpha1.KafkaMirrorTopic{mkMirror("m", "dr", "orders", "")},
	}
	require.Contains(t, errsText(Validate(in)), "KafkaMirrorTopic m collides with orders")
}

func TestValidateMirrorNoCollisionAcrossClusters(t *testing.T) {
	in := Input{MirrorTopics: []*v1alpha1.KafkaMirrorTopic{
		mkMirror("a", "dr1", "orders", ""), mkMirror("b", "dr2", "orders", ""),
	}}
	require.Empty(t, Validate(in))
}

func TestValidateMirrorEmptyTopicNameIsOmitted(t *testing.T) {
	in := Input{MirrorTopics: []*v1alpha1.KafkaMirrorTopic{
		mkMirror("a", "dr", "orders", ""), mkMirror("b", "dr", "orders", ""),
	}}
	require.Contains(t, errsText(Validate(in)), "collides")
}

func TestValidateClusterRESTRejectsV3(t *testing.T) {
	c := mkRESTCluster("dr", &v1alpha1.KafkaRESTConfig{Endpoint: "https://h:8090/kafka/v3/"})
	require.Contains(t, errsText(ValidateClusterREST(c)), "/v3")
	c = mkRESTCluster("dr", &v1alpha1.KafkaRESTConfig{Endpoint: "https://h:8090/kafka"})
	require.Empty(t, ValidateClusterREST(c))
	c = mkRESTCluster("dr", &v1alpha1.KafkaRESTConfig{Endpoint: "ftp://h"})
	require.NotEmpty(t, ValidateClusterREST(c))
	c = mkRESTCluster("dr", &v1alpha1.KafkaRESTConfig{})
	require.NotEmpty(t, ValidateClusterREST(c))
}

func TestValidateClusterRESTAuthShape(t *testing.T) {
	vf := &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "X"}}
	c := mkRESTCluster("dr", &v1alpha1.KafkaRESTConfig{Endpoint: "https://h", Auth: &v1alpha1.MDSAuth{Type: "basic", Username: vf}})
	require.NotEmpty(t, ValidateClusterREST(c))
	c = mkRESTCluster("dr", &v1alpha1.KafkaRESTConfig{Endpoint: "https://h", Auth: &v1alpha1.MDSAuth{Type: "basic", Username: vf, Password: vf}})
	require.Empty(t, ValidateClusterREST(c))
	c = mkRESTCluster("dr", &v1alpha1.KafkaRESTConfig{Endpoint: "https://h", Auth: &v1alpha1.MDSAuth{Type: "bearer"}})
	require.NotEmpty(t, ValidateClusterREST(c))
	c = mkRESTCluster("dr", &v1alpha1.KafkaRESTConfig{Endpoint: "https://h", Auth: &v1alpha1.MDSAuth{Type: "mtls"}})
	require.NotEmpty(t, ValidateClusterREST(c))
}
