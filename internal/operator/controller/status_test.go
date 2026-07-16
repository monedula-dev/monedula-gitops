package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	srmock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// mkTime returns a *metav1.Time offset from a fixed base, so two statuses can
// differ ONLY in their volatile timestamps.
func mkTime(offset time.Duration) *metav1.Time {
	t := metav1.NewTime(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Add(offset))
	return &t
}

func readyCond(ts time.Duration) metav1.Condition {
	return metav1.Condition{
		Type:               v1alpha1.CondReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Reconciled",
		Message:            "all operations succeeded",
		ObservedGeneration: 1,
		LastTransitionTime: *mkTime(ts),
	}
}

func TestStatusEqualIgnoringTimestamps_Cluster(t *testing.T) {
	mk := func(checked time.Duration, phase string) *v1alpha1.KafkaCluster {
		return &v1alpha1.KafkaCluster{Status: &v1alpha1.KafkaClusterStatus{
			ObservedGeneration: 1,
			Phase:              phase,
			Conditions:         []metav1.Condition{readyCond(checked)},
			LastCheckedTime:    mkTime(checked),
		}}
	}

	if !statusEqualIgnoringTimestamps(mk(0, v1alpha1.PhaseReady), mk(time.Minute, v1alpha1.PhaseReady)) {
		t.Error("statuses differing only in timestamps should be equal")
	}
	if statusEqualIgnoringTimestamps(mk(0, v1alpha1.PhaseReady), mk(0, v1alpha1.PhaseError)) {
		t.Error("statuses differing in phase should not be equal")
	}
	if statusEqualIgnoringTimestamps(mk(0, v1alpha1.PhaseReady), &v1alpha1.KafkaCluster{}) {
		t.Error("status vs nil status should not be equal")
	}
}

func TestStatusEqualIgnoringTimestamps_Topic(t *testing.T) {
	mk := func(checked time.Duration, drifted bool, condStatus metav1.ConditionStatus) *v1alpha1.KafkaTopic {
		cond := readyCond(checked)
		cond.Status = condStatus
		return &v1alpha1.KafkaTopic{Status: &v1alpha1.KafkaTopicStatus{
			ObservedGeneration: 1,
			Phase:              v1alpha1.PhaseReady,
			Conditions:         []metav1.Condition{cond},
			Drift:              &v1alpha1.DriftStatus{Detected: drifted},
			LastCheckedTime:    mkTime(checked),
			LastAppliedTime:    mkTime(checked),
		}}
	}

	if !statusEqualIgnoringTimestamps(mk(0, false, metav1.ConditionTrue), mk(time.Hour, false, metav1.ConditionTrue)) {
		t.Error("topic statuses differing only in timestamps should be equal")
	}
	if statusEqualIgnoringTimestamps(mk(0, false, metav1.ConditionTrue), mk(0, true, metav1.ConditionTrue)) {
		t.Error("topic statuses differing in drift should not be equal")
	}
	if statusEqualIgnoringTimestamps(mk(0, false, metav1.ConditionTrue), mk(0, false, metav1.ConditionFalse)) {
		t.Error("topic statuses differing in a condition status should not be equal")
	}
}

func TestStatusEqualIgnoringTimestamps_Policy(t *testing.T) {
	mk := func(checked time.Duration, phase string) *v1alpha1.KafkaAccessPolicy {
		return &v1alpha1.KafkaAccessPolicy{Status: &v1alpha1.KafkaAccessPolicyStatus{
			ObservedGeneration: 1,
			Phase:              phase,
			Conditions:         []metav1.Condition{readyCond(checked)},
			LastCheckedTime:    mkTime(checked),
			LastAppliedTime:    mkTime(checked),
		}}
	}

	if !statusEqualIgnoringTimestamps(mk(0, v1alpha1.PhaseReady), mk(time.Minute, v1alpha1.PhaseReady)) {
		t.Error("policy statuses differing only in timestamps should be equal")
	}
	if statusEqualIgnoringTimestamps(mk(0, v1alpha1.PhaseReady), mk(0, v1alpha1.PhaseDrifted)) {
		t.Error("policy statuses differing in phase should not be equal")
	}
}

func TestStatusEqualIgnoringTimestamps_User(t *testing.T) {
	mk := func(checked time.Duration, phase string) *v1alpha1.KafkaUser {
		return &v1alpha1.KafkaUser{Status: &v1alpha1.KafkaUserStatus{
			ObservedGeneration: 1,
			Phase:              phase,
			Conditions:         []metav1.Condition{readyCond(checked)},
			LastCheckedTime:    mkTime(checked),
			LastAppliedTime:    mkTime(checked),
		}}
	}

	if !statusEqualIgnoringTimestamps(mk(0, v1alpha1.PhaseReady), mk(time.Minute, v1alpha1.PhaseReady)) {
		t.Error("user statuses differing only in timestamps should be equal")
	}
	if statusEqualIgnoringTimestamps(mk(0, v1alpha1.PhaseReady), mk(0, v1alpha1.PhaseError)) {
		t.Error("user statuses differing in phase should not be equal")
	}
	// appliedPasswordRef participates in equality (a rotation watermark change
	// must be persisted, never skipped as a no-op).
	a, b := mk(0, v1alpha1.PhaseReady), mk(0, v1alpha1.PhaseReady)
	b.Status.AppliedPasswordRef = &v1alpha1.AppliedPasswordRef{SecretName: "s", ResourceVersion: "2"}
	if statusEqualIgnoringTimestamps(a, b) {
		t.Error("user statuses differing in appliedPasswordRef should not be equal")
	}
}

func TestStatusEqualIgnoringTimestamps_MismatchedTypes(t *testing.T) {
	// Mismatched / unknown types must report NOT equal so the write proceeds
	// (fail open: an unnecessary write is benign; a missed write is not).
	if statusEqualIgnoringTimestamps(&v1alpha1.KafkaCluster{}, &v1alpha1.KafkaTopic{}) {
		t.Error("mismatched object types should not compare equal")
	}
}

// TestClusterReconcile_SecondPassSkipsStatusWrite drives the cluster reconciler
// twice with an identical outcome and asserts the second pass performs NO
// status write: the object's resourceVersion must be unchanged (only the
// volatile LastCheckedTime would differ, which is not worth a write+requeue).
func TestClusterReconcile_SecondPassSkipsStatusWrite(t *testing.T) {
	s := newScheme(t)
	c := clusterObj()
	cl := newFakeClient(t, s, c)
	r := &KafkaClusterReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: kafkamock.New(nil, nil)}}
	req := ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "ns1", Name: "prod"}}

	res1, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	var afterFirst v1alpha1.KafkaCluster
	if err := cl.Get(context.Background(), req.NamespacedName, &afterFirst); err != nil {
		t.Fatalf("get after first: %v", err)
	}
	if afterFirst.Status == nil || afterFirst.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("first reconcile must still write status, got %+v", afterFirst.Status)
	}

	res2, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	var afterSecond v1alpha1.KafkaCluster
	if err := cl.Get(context.Background(), req.NamespacedName, &afterSecond); err != nil {
		t.Fatalf("get after second: %v", err)
	}
	if afterFirst.ResourceVersion != afterSecond.ResourceVersion {
		t.Fatalf("second reconcile wrote status: resourceVersion %s -> %s",
			afterFirst.ResourceVersion, afterSecond.ResourceVersion)
	}
	// The skip must not change the reconcile result (periodic requeue stays).
	if res1 != res2 || res2.RequeueAfter != clusterRequeueAfter {
		t.Fatalf("results differ on skip: first %+v second %+v", res1, res2)
	}
}

// TestTopicReconcile_SecondPassSkipsStatusWrite is the same settling assertion
// for the topic reconciler: after the first pass (finalizer add + apply +
// status write), an identical second pass must not bump resourceVersion.
func TestTopicReconcile_SecondPassSkipsStatusWrite(t *testing.T) {
	s := topicScheme(t)
	cl := newTopicFakeClient(t, s, topicCluster(), topicObj(""))
	r := &KafkaTopicReconciler{
		Client:  cl,
		Scheme:  s,
		Clients: stubFactory{k: kafkamock.New(nil, nil), sr: srmock.New()},
	}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	var afterFirst v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &afterFirst); err != nil {
		t.Fatalf("get after first: %v", err)
	}
	if afterFirst.Status == nil || afterFirst.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("first reconcile must write Ready status, got %+v", afterFirst.Status)
	}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	var afterSecond v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &afterSecond); err != nil {
		t.Fatalf("get after second: %v", err)
	}
	if afterFirst.ResourceVersion != afterSecond.ResourceVersion {
		t.Fatalf("second reconcile wrote status: resourceVersion %s -> %s",
			afterFirst.ResourceVersion, afterSecond.ResourceVersion)
	}
}
