package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// secretScheme returns a scheme with v1alpha1 + corev1 (newScheme alone omits corev1).
func secretScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := newScheme(t)
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1 to scheme: %v", err)
	}
	return s
}

// clusterWithSecret builds a KafkaCluster whose SCRAM auth references secretName,
// so clusterSecretNames(c) == [secretName].
func clusterWithSecret(ns, name, secretName string) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Auth: &v1alpha1.AuthConfig{
				Mechanism: "SCRAM-SHA-512",
				SCRAM:     &v1alpha1.SCRAMAuth{Username: vfSecretVal(secretName), Password: vfSecretVal(secretName)},
			},
		},
	}
}

// newSecretTestClient builds a fake client with corev1+v1alpha1 and the
// ClusterSecretNamesIndex registered (so List-by-MatchingFields works).
func newSecretTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(secretScheme(t)).
		WithObjects(objs...).
		WithIndex(&v1alpha1.KafkaCluster{}, ClusterSecretNamesIndex, func(obj client.Object) []string {
			return clusterSecretNames(obj.(*v1alpha1.KafkaCluster))
		}).
		Build()
}

func TestMapSecretToClusters(t *testing.T) {
	ns := "team-a"
	c := newSecretTestClient(t,
		clusterWithSecret(ns, "prod", "kafka-creds"),
		clusterWithSecret(ns, "staging", "other-creds"),
	)
	r := &KafkaClusterReconciler{Client: c}

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "kafka-creds"}}
	got := r.mapSecretToClusters(context.Background(), secret)
	if len(got) != 1 {
		t.Fatalf("got %d requests, want 1: %+v", len(got), got)
	}
	if got[0].Name != "prod" || got[0].Namespace != ns {
		t.Errorf("got request %v, want %s/prod", got[0].NamespacedName, ns)
	}
}

func TestMapSecretToClustersNoMatch(t *testing.T) {
	c := newSecretTestClient(t, clusterWithSecret("team-a", "prod", "kafka-creds"))
	r := &KafkaClusterReconciler{Client: c}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "unrelated"}}
	if got := r.mapSecretToClusters(context.Background(), secret); len(got) != 0 {
		t.Errorf("got %d requests, want 0: %+v", len(got), got)
	}
}

func TestSetCredentialSourceUnwatchedCondition(t *testing.T) {
	ns := "team-a"
	labeled := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: ns, Name: "labeled",
		Labels: map[string]string{CredentialSourceLabel: CredentialSourceLabelValue},
	}}
	unlabeled := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "unlabeled"}}
	c := newSecretTestClient(t, labeled, unlabeled)
	r := &KafkaClusterReconciler{Client: c}

	t.Run("unlabeled sets True", func(t *testing.T) {
		cl := clusterWithSecret(ns, "prod", "unlabeled")
		st := &v1alpha1.KafkaClusterStatus{}
		r.setCredentialSourceUnwatchedCondition(context.Background(), cl, st)
		cond := findCond(st.Conditions, v1alpha1.CondCredentialSourceUnwatched)
		if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "SecretNotLabeled" {
			t.Fatalf("got %+v, want True/SecretNotLabeled", cond)
		}
	})

	t.Run("labeled sets False", func(t *testing.T) {
		cl := clusterWithSecret(ns, "prod", "labeled")
		st := &v1alpha1.KafkaClusterStatus{}
		r.setCredentialSourceUnwatchedCondition(context.Background(), cl, st)
		cond := findCond(st.Conditions, v1alpha1.CondCredentialSourceUnwatched)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "AllWatchedOrNone" {
			t.Fatalf("got %+v, want False/AllWatchedOrNone", cond)
		}
	})

	t.Run("no secret refs sets False", func(t *testing.T) {
		// A cluster referencing no Secret (clusterSecretNames == nil) is not
		// "unwatched" — the condition is False/AllWatchedOrNone, matching the
		// ConfigMap analog's no-ref case.
		cl := &v1alpha1.KafkaCluster{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "norefs"},
			Spec:       v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092"},
		}
		st := &v1alpha1.KafkaClusterStatus{}
		r.setCredentialSourceUnwatchedCondition(context.Background(), cl, st)
		cond := findCond(st.Conditions, v1alpha1.CondCredentialSourceUnwatched)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "AllWatchedOrNone" {
			t.Fatalf("got %+v, want False/AllWatchedOrNone", cond)
		}
	})
}
