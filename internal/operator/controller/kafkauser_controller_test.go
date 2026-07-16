package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// newUserSecretTestClient builds a fake client with corev1+v1alpha1 and BOTH
// Secret-related indexes the KafkaUser map-func uses: ClusterSecretNamesIndex
// (cluster credential fan-out) and UserPasswordSecretNamesIndex (direct
// password-ref lookup).
func newUserSecretTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(secretScheme(t)).
		WithObjects(objs...).
		WithIndex(&v1alpha1.KafkaCluster{}, ClusterSecretNamesIndex, func(obj client.Object) []string {
			return clusterSecretNames(obj.(*v1alpha1.KafkaCluster))
		}).
		WithIndex(&v1alpha1.KafkaUser{}, UserPasswordSecretNamesIndex, func(obj client.Object) []string {
			return userPasswordSecretNames(obj.(*v1alpha1.KafkaUser))
		}).
		Build()
}

// testUser builds a KafkaUser on cluster clusterRef; passwordSecret, when
// non-empty, sets a valueFrom secretKeyRef to that Secret (else generate mode).
func testUser(ns, name, clusterRef, passwordSecret string) *v1alpha1.KafkaUser {
	pw := &v1alpha1.UserPassword{Generate: &v1alpha1.GeneratePassword{}}
	if passwordSecret != "" {
		pw = &v1alpha1.UserPassword{ValueFrom: &v1alpha1.ValueSource{
			SecretKeyRef: &v1alpha1.SecretKeyRef{Name: passwordSecret, Key: "password"},
		}}
	}
	return &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1alpha1.KafkaUserSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			Password:   pw,
		},
	}
}

// TestMapSecretToUsersPasswordRef: a change to a Secret referenced by a user's
// spec.password.valueFrom.secretKeyRef enqueues exactly that user (the
// event-driven rotation trigger), not users referencing other Secrets.
func TestMapSecretToUsersPasswordRef(t *testing.T) {
	ns := "team-a"
	c := newUserSecretTestClient(t,
		testUser(ns, "svc-a", "prod", "svc-a-pw"),
		testUser(ns, "svc-b", "prod", "svc-b-pw"),
		testUser(ns, "svc-gen", "prod", ""), // generate mode: no password ref
	)
	r := &KafkaUserReconciler{Client: c}

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "svc-a-pw"}}
	got := r.mapSecretToUsers(context.Background(), secret)
	if len(got) != 1 || got[0].Name != "svc-a" || got[0].Namespace != ns {
		t.Fatalf("mapSecretToUsers = %+v, want [%s/svc-a]", got, ns)
	}
}

// TestMapSecretToUsersClusterCredential: a change to a cluster's credential
// Secret fans out to every user on that cluster (2nd-hop fan-out, §11.4).
func TestMapSecretToUsersClusterCredential(t *testing.T) {
	ns := "team-a"
	c := newUserSecretTestClient(t,
		clusterWithSecret(ns, "prod", "kafka-creds"),
		testUser(ns, "svc-a", "prod", "svc-a-pw"),
		testUser(ns, "svc-other", "staging", "svc-other-pw"),
	)
	r := &KafkaUserReconciler{Client: c}

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "kafka-creds"}}
	got := r.mapSecretToUsers(context.Background(), secret)
	if len(got) != 1 || got[0].Name != "svc-a" {
		t.Fatalf("mapSecretToUsers = %+v, want [%s/svc-a]", got, ns)
	}
}

// TestMapSecretToUsersDeduplicates: a Secret that is BOTH a user's password
// source AND the cluster credential enqueues that user once.
func TestMapSecretToUsersDeduplicates(t *testing.T) {
	ns := "team-a"
	c := newUserSecretTestClient(t,
		clusterWithSecret(ns, "prod", "shared-creds"),
		testUser(ns, "svc-a", "prod", "shared-creds"),
	)
	r := &KafkaUserReconciler{Client: c}

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "shared-creds"}}
	got := r.mapSecretToUsers(context.Background(), secret)
	if len(got) != 1 || got[0].Name != "svc-a" {
		t.Fatalf("mapSecretToUsers = %+v, want exactly one svc-a request", got)
	}
}

func TestMapSecretToUsersNoMatch(t *testing.T) {
	ns := "team-a"
	c := newUserSecretTestClient(t, testUser(ns, "svc-a", "prod", "svc-a-pw"))
	r := &KafkaUserReconciler{Client: c}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "unrelated"}}
	if got := r.mapSecretToUsers(context.Background(), secret); len(got) != 0 {
		t.Fatalf("mapSecretToUsers = %+v, want none", got)
	}
}

func TestUserPasswordSecretNames(t *testing.T) {
	if got := userPasswordSecretNames(nil); got != nil {
		t.Errorf("nil user: got %v, want nil", got)
	}
	if got := userPasswordSecretNames(testUser("ns", "u", "c", "")); got != nil {
		t.Errorf("generate mode: got %v, want nil", got)
	}
	got := userPasswordSecretNames(testUser("ns", "u", "c", "pw-secret"))
	if len(got) != 1 || got[0] != "pw-secret" {
		t.Errorf("secretKeyRef: got %v, want [pw-secret]", got)
	}
}
