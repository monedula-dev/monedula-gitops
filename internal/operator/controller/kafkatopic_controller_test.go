package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	srmock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// errReason is a tiny error helper for stub failures in tests.
type errReason string

func (e errReason) Error() string { return string(e) }

// seededTopics returns a one-topic slice for seeding the mock so deletion has
// something to remove.
func seededTopics(name string) []kafka.TopicState {
	return []kafka.TopicState{{Name: name, Partitions: 3, ReplicationFactor: 1}}
}

// compile-time assertion: the topic reconciler is a controller-runtime Reconciler.
var _ reconcile.Reconciler = (*KafkaTopicReconciler)(nil)

// topicScheme returns a scheme with the v1alpha1 types registered, plus corev1
// so schema-body Secrets can be seeded for the K8sResolver.
func topicScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("corev1 AddToScheme: %v", err)
	}
	return s
}

// newTopicFakeClient builds a fake client with the topic + cluster status
// subresources enabled so Status().Update works, and finalizer-respecting
// deletion (the fake client keeps an object with a finalizer until removed).
func newTopicFakeClient(t *testing.T, s *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.KafkaTopic{}, &v1alpha1.KafkaCluster{}).
		Build()
}

func topicCluster() *v1alpha1.KafkaCluster {
	c := &v1alpha1.KafkaCluster{}
	c.Name = "prod"
	c.Namespace = "ns1"
	c.Generation = 1
	return c
}

func topicObj(mode string) *v1alpha1.KafkaTopic {
	tp := &v1alpha1.KafkaTopic{
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			TopicName:  "payments.orders",
			Partitions: 3,
		},
	}
	tp.Name = "orders"
	tp.Namespace = "ns1"
	tp.Generation = 1
	if mode != "" {
		tp.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: mode}
	}
	return tp
}

func topicReq() ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "ns1", Name: "orders"}}
}

// callsContain reports whether the mock recorded a call with the given prefix.
func callsContain(calls []string, prefix string) bool {
	for _, c := range calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

func TestTopicReconcile_NotFound(t *testing.T) {
	s := topicScheme(t)
	cl := newTopicFakeClient(t, s)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{}}

	res, err := r.Reconcile(context.Background(), topicReq())
	if err != nil {
		t.Fatalf("Reconcile NotFound returned err: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("NotFound should not requeue, got %v", res.RequeueAfter)
	}
}

func TestTopicReconcile_CreatesAndAddsFinalizer(t *testing.T) {
	s := topicScheme(t)
	tp := topicObj("Enforce")
	c := topicCluster()
	cl := newTopicFakeClient(t, s, tp, c)
	k := kafkamock.New(nil, nil)
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k}}

	res, err := r.Reconcile(context.Background(), topicReq())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != topicRequeueAfter {
		t.Fatalf("RequeueAfter = %v want %v", res.RequeueAfter, topicRequeueAfter)
	}

	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(tp), &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatalf("finalizer not added: %v", got.Finalizers)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status phase = %+v want Ready", got.Status)
	}
	if cond := findCond(got.Status.Conditions, v1alpha1.CondTopicSynced); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("TopicSynced = %v want True", cond)
	}
	if !callsContain(k.Calls(), "CreateTopic payments.orders") {
		t.Fatalf("expected CreateTopic call, got %v", k.Calls())
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "Normal") {
			t.Fatalf("expected Normal event, got %q", ev)
		}
	default:
		t.Fatal("no event emitted on success")
	}
}

func TestTopicReconcile_ClusterNotFound(t *testing.T) {
	s := topicScheme(t)
	tp := topicObj("Enforce")
	// No cluster object created.
	cl := newTopicFakeClient(t, s, tp)
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: kafkamock.New(nil, nil)}}

	res, err := r.Reconcile(context.Background(), topicReq())
	if err == nil && res.RequeueAfter == 0 {
		t.Fatal("expected requeue (err or RequeueAfter) when cluster not found")
	}

	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(tp), &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("status phase = %+v want Error", got.Status)
	}
	if cond := findCond(got.Status.Conditions, v1alpha1.CondReady); cond == nil ||
		cond.Status != metav1.ConditionFalse || cond.Reason != "ClusterNotFound" {
		t.Fatalf("Ready cond = %v want False/ClusterNotFound", cond)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "Warning") || !strings.Contains(ev, "ClusterNotFound") {
			t.Fatalf("event = %q want Warning/ClusterNotFound", ev)
		}
	default:
		t.Fatal("no event emitted on cluster-not-found")
	}
}

func TestTopicReconcile_ClientsBuildError(t *testing.T) {
	s := topicScheme(t)
	tp := topicObj("Enforce")
	c := topicCluster()
	cl := newTopicFakeClient(t, s, tp, c)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{err: errReason("boom")}}

	_, err := r.Reconcile(context.Background(), topicReq())
	if err == nil {
		t.Fatal("expected error from clients-build failure (requeue signal)")
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(tp), &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("status phase = %+v want Error", got.Status)
	}
	if cond := findCond(got.Status.Conditions, v1alpha1.CondReady); cond == nil ||
		cond.Status != metav1.ConditionFalse || cond.Reason != "ClientsBuildFailed" {
		t.Fatalf("Ready cond = %v want False/ClientsBuildFailed", cond)
	}
}

// deleteSetup creates a topic that already exists in Kafka, with the finalizer
// set, then issues a Delete so the fake client sets DeletionTimestamp and keeps
// the object alive (finalizer present). It returns the client and mock.
func deleteSetup(t *testing.T, deletionPolicy string, annotations map[string]string, k *kafkamock.Client) (client.Client, *runtime.Scheme) {
	t.Helper()
	s := topicScheme(t)
	tp := topicObj("Enforce")
	tp.Spec.DeletionPolicy = deletionPolicy
	tp.Finalizers = []string{FinalizerName}
	if annotations != nil {
		tp.Annotations = annotations
	}
	c := topicCluster()
	cl := newTopicFakeClient(t, s, tp, c)

	// Issue the delete: fake client honors the finalizer and only sets
	// DeletionTimestamp.
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	return cl, s
}

func TestTopicReconcile_DeletePolicyDelete_WithApproval(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	cl, s := deleteSetup(t, "Delete", map[string]string{"gitops.monedula.dev/allow-delete": "true"}, k)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if !callsContain(k.Calls(), "DeleteTopic payments.orders") {
		t.Fatalf("expected DeleteTopic call, got %v", k.Calls())
	}
	// Finalizer removed -> object gone.
	var got v1alpha1.KafkaTopic
	err := cl.Get(context.Background(), topicReq().NamespacedName, &got)
	if err == nil {
		t.Fatalf("topic still present after deletion: finalizers %v", got.Finalizers)
	}
}

// TestTopicReconcile_DeleteDefaultedTopicName pins review I6: a topic CR that
// relies on the metadata-name default (no spec.topicName) must delete the
// DEFAULTED topic name on finalization — not call DeleteTopic with "".
func TestTopicReconcile_DeleteDefaultedTopicName(t *testing.T) {
	s := topicScheme(t)
	tp := topicObj("Enforce")
	tp.Spec.TopicName = "" // rely on the metadata-name default: "orders"
	tp.Spec.DeletionPolicy = "Delete"
	tp.Finalizers = []string{FinalizerName}
	tp.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	c := topicCluster()
	cl := newTopicFakeClient(t, s, tp, c)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	k := kafkamock.New(seededTopics("orders"), nil)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}
	if !callsContain(k.Calls(), "DeleteTopic orders") {
		t.Fatalf("expected DeleteTopic with the defaulted metadata name, got %v", k.Calls())
	}
}

func TestTopicReconcile_DeletePolicyDelete_NoApproval(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	cl, s := deleteSetup(t, "Delete", nil, k)
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if callsContain(k.Calls(), "DeleteTopic") {
		t.Fatalf("topic must NOT be deleted without approval, got %v", k.Calls())
	}
	// Finalizer retained -> object still present.
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err != nil {
		t.Fatalf("topic should still exist (finalizer retained): %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatal("finalizer should be retained without approval")
	}
}

func TestTopicReconcile_DeletePolicyOrphan(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	cl, s := deleteSetup(t, "Orphan", nil, k)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if callsContain(k.Calls(), "DeleteTopic") {
		t.Fatalf("Orphan must NOT delete the Kafka topic, got %v", k.Calls())
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err == nil {
		t.Fatalf("topic should be gone (finalizer removed), finalizers %v", got.Finalizers)
	}
}

// deleteSetupWithClusterDefaults is like deleteSetup but seeds a KafkaCluster
// that carries cluster-level defaults (e.g. topicDeletionPolicy). The topic's
// own spec.deletionPolicy is left at the supplied value (empty string = absent).
func deleteSetupWithClusterDefaults(
	t *testing.T,
	topicDeletionPolicy string, // topic-level spec value ("" = absent)
	clusterDeletionPolicy string, // cluster defaults.topicDeletionPolicy
	annotations map[string]string,
	k *kafkamock.Client,
) (client.Client, *runtime.Scheme) {
	t.Helper()
	s := topicScheme(t)
	tp := topicObj("Enforce")
	tp.Spec.DeletionPolicy = topicDeletionPolicy
	tp.Finalizers = []string{FinalizerName}
	if annotations != nil {
		tp.Annotations = annotations
	}
	c := topicCluster()
	c.Spec.Defaults = &v1alpha1.ClusterDefaults{
		TopicDeletionPolicy: clusterDeletionPolicy,
	}
	cl := newTopicFakeClient(t, s, tp, c)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	return cl, s
}

// TestTopicReconcile_ClusterDefaultDelete_EmptyTopicPolicy: cluster has
// defaults.topicDeletionPolicy=Delete; topic leaves spec.deletionPolicy empty.
// With allow-delete annotation the finalization MUST delete the topic from
// Kafka — the cluster default should be honored.
func TestTopicReconcile_ClusterDefaultDelete_EmptyTopicPolicy(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	cl, s := deleteSetupWithClusterDefaults(t, "", "Delete",
		map[string]string{"gitops.monedula.dev/allow-delete": "true"}, k)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if !callsContain(k.Calls(), "DeleteTopic payments.orders") {
		t.Fatalf("cluster default Delete must trigger topic deletion, got %v", k.Calls())
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err == nil {
		t.Fatalf("topic should be gone after deletion: finalizers %v", got.Finalizers)
	}
}

// TestTopicReconcile_ClusterDefaultDelete_ExplicitOrphan: cluster has
// defaults.topicDeletionPolicy=Delete; topic explicitly sets
// spec.deletionPolicy=Orphan. Explicit value wins — topic must NOT be deleted.
func TestTopicReconcile_ClusterDefaultDelete_ExplicitOrphan(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	cl, s := deleteSetupWithClusterDefaults(t, "Orphan", "Delete", nil, k)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if callsContain(k.Calls(), "DeleteTopic") {
		t.Fatalf("explicit Orphan must win over cluster default Delete, got %v", k.Calls())
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err == nil {
		t.Fatalf("topic should be gone (finalizer removed), finalizers %v", got.Finalizers)
	}
}

// TestTopicReconcile_ClusterDefaultDelete_EmptyTopicPolicy_NoApproval: cluster
// has defaults.topicDeletionPolicy=Delete; topic leaves spec.deletionPolicy
// empty (so the cluster default is the effective policy); the
// allow-delete annotation is ABSENT. On finalization, DeleteTopic must NOT be
// called, the finalizer is retained, and the DeleteNotApproved warning path is
// taken.
func TestTopicReconcile_ClusterDefaultDelete_EmptyTopicPolicy_NoApproval(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	cl, s := deleteSetupWithClusterDefaults(t, "", "Delete", nil /* no allow-delete */, k)
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	// DeleteTopic must NOT have been called — approval gate blocks deletion.
	if callsContain(k.Calls(), "DeleteTopic") {
		t.Fatalf("topic must NOT be deleted without approval (cluster default Delete, no annotation), got %v", k.Calls())
	}
	// Finalizer retained -> object still present.
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err != nil {
		t.Fatalf("topic should still exist (finalizer retained): %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatal("finalizer should be retained without approval")
	}
	// DeleteNotApproved warning event must have been emitted.
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "DeleteNotApproved") {
			t.Fatalf("expected DeleteNotApproved warning event, got %q", ev)
		}
	default:
		t.Fatal("no event emitted — expected DeleteNotApproved warning")
	}
}

// TestTopicReconcile_NoClusterDefault_EmptyTopicPolicy: no cluster default set,
// topic leaves spec.deletionPolicy empty. Existing Orphan behavior must be
// preserved — topic survives finalization.
func TestTopicReconcile_NoClusterDefault_EmptyTopicPolicy(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	cl, s := deleteSetup(t, "", nil, k)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if callsContain(k.Calls(), "DeleteTopic") {
		t.Fatalf("empty policy without cluster default must Orphan (not delete), got %v", k.Calls())
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err == nil {
		t.Fatalf("topic should be gone (finalizer removed), finalizers %v", got.Finalizers)
	}
}

func TestTopicReconcile_DeleteUnreachable_ForceRemoval(t *testing.T) {
	cl, s := deleteSetup(t, "Delete",
		map[string]string{"gitops.monedula.dev/force-finalizer-removal": "true"}, nil)
	rec := events.NewFakeRecorder(8)
	// Clients fail to build (cluster unreachable).
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{err: errReason("unreachable")}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete with force-removal: %v", err)
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err == nil {
		t.Fatalf("topic should be gone (force-removal), finalizers %v", got.Finalizers)
	}
}

func TestTopicReconcile_DeleteUnreachable_BlocksFinalizer(t *testing.T) {
	cl, s := deleteSetup(t, "Delete", map[string]string{"gitops.monedula.dev/allow-delete": "true"}, nil)
	rec := events.NewFakeRecorder(8)
	// Clients fail to build (cluster unreachable), no force-removal annotation.
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{err: errReason("unreachable")}}

	_, err := r.Reconcile(context.Background(), topicReq())
	if err == nil {
		t.Fatal("expected requeue error when cluster unreachable during deletion")
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err != nil {
		t.Fatalf("topic should still exist (finalizer blocked): %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatal("finalizer should be retained when unreachable and not forced")
	}
}

func TestTopicReconcile_DetectOnly_NoMutation(t *testing.T) {
	s := topicScheme(t)
	tp := topicObj("DetectOnly")
	c := topicCluster()
	cl := newTopicFakeClient(t, s, tp, c)
	k := kafkamock.New(nil, nil) // topic absent -> drift
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile DetectOnly: %v", err)
	}
	if callsContain(k.Calls(), "CreateTopic") {
		t.Fatalf("DetectOnly must not mutate, got %v", k.Calls())
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseDrifted {
		t.Fatalf("status phase = %+v want Drifted", got.Status)
	}
}

// schemaTopic returns an Enforce-mode topic declaring a value schema from the
// given source.
func schemaTopic(src v1alpha1.ValueSource) *v1alpha1.KafkaTopic {
	tp := topicObj("Enforce")
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:      "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: src},
	}
	return tp
}

// schemaCluster returns the test cluster with a Schema Registry endpoint set.
func schemaCluster() *v1alpha1.KafkaCluster {
	c := topicCluster()
	c.Spec.SchemaRegistry = &v1alpha1.SchemaRegistryConf{Endpoint: "http://sr.example:8081"}
	return c
}

// TestTopicReconcile_SchemaRegistered is the regression test for review finding
// C2: the controller must pass the Schema Registry client into the reconcile
// core so a secretKeyRef-sourced schema is actually registered (instead of all
// schema work being silently skipped with sr == nil).
func TestTopicReconcile_SchemaRegistered(t *testing.T) {
	s := topicScheme(t)
	tp := schemaTopic(v1alpha1.ValueSource{
		SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "orders-schema", Key: "value.avsc"},
	})
	c := schemaCluster()
	sec := &corev1.Secret{}
	sec.Name = "orders-schema"
	sec.Namespace = "ns1" // must match the topic's namespace (refs are namespace-local)
	sec.Data = map[string][]byte{
		"value.avsc": []byte(`{"type":"record","name":"Order","fields":[]}`),
	}
	cl := newTopicFakeClient(t, s, tp, c, sec)
	k := kafkamock.New(nil, nil)
	sr := srmock.New()
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !callsContain(sr.Calls(), "RegisterSchema payments.orders-value") {
		t.Fatalf("expected RegisterSchema payments.orders-value, got %v", sr.Calls())
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status phase = %+v want Ready", got.Status)
	}
	if cond := findCond(got.Status.Conditions, v1alpha1.CondSchemaSynced); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("SchemaSynced = %v want True", cond)
	}
	if got.Status.Schema == nil || got.Status.Schema.ValueSubject != "payments.orders-value" ||
		got.Status.Schema.ValueSchemaID == 0 {
		t.Fatalf("status.schema = %+v want populated payments.orders-value", got.Status.Schema)
	}
}

// TestTopicReconcile_SchemaFileRefSkipped pins the documented operator-mode
// limitation: a file-based schema ref cannot be resolved (the operator never
// reads the filesystem), so ONLY the schema is skipped — SchemaSynced=False
// reason SchemaUnresolved — while the topic itself still reconciles.
func TestTopicReconcile_SchemaFileRefSkipped(t *testing.T) {
	s := topicScheme(t)
	tp := schemaTopic(v1alpha1.ValueSource{File: "schemas/order.avsc"})
	c := schemaCluster()
	cl := newTopicFakeClient(t, s, tp, c)
	k := kafkamock.New(nil, nil)
	sr := srmock.New()
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if callsContain(sr.Calls(), "RegisterSchema") {
		t.Fatalf("schema registry must NOT be called for a file ref, got %v", sr.Calls())
	}
	if !callsContain(k.Calls(), "CreateTopic payments.orders") {
		t.Fatalf("topic should still reconcile, got %v", k.Calls())
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	// A schema-resolve failure is terminal-but-non-fatal: topic + access apply
	// cleanly, so the phase stays Ready while SchemaSynced reports the skip.
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status phase = %+v want Ready", got.Status)
	}
	cond := findCond(got.Status.Conditions, v1alpha1.CondSchemaSynced)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "SchemaUnresolved" {
		t.Fatalf("SchemaSynced = %v want False/SchemaUnresolved", cond)
	}
	if got.Status.Schema != nil {
		t.Fatalf("status.schema = %+v want nil (schema skipped)", got.Status.Schema)
	}
}

// TestTopicReconcile_SchemaInlineRegistered verifies that a KafkaTopic with a
// valueSchema.valueFrom.inline source resolves the schema body verbatim, calls
// RegisterSchema on the SR mock, and reports SchemaSynced=True (spec §11).
func TestTopicReconcile_SchemaInlineRegistered(t *testing.T) {
	s := topicScheme(t)
	body := `{"type":"record","name":"Order","fields":[]}`
	tp := schemaTopic(v1alpha1.ValueSource{Inline: body})
	c := schemaCluster()
	cl := newTopicFakeClient(t, s, tp, c)
	k := kafkamock.New(nil, nil)
	sr := srmock.New()
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !callsContain(sr.Calls(), "RegisterSchema payments.orders-value") {
		t.Fatalf("expected RegisterSchema payments.orders-value, got %v", sr.Calls())
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status phase = %+v want Ready", got.Status)
	}
	if cond := findCond(got.Status.Conditions, v1alpha1.CondSchemaSynced); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("SchemaSynced = %v want True", cond)
	}
}

// TestTopicReconcile_SchemaConfigMapRegistered verifies that a KafkaTopic with a
// valueSchema.valueFrom.configMapKeyRef source reads the schema from a ConfigMap
// and calls RegisterSchema on the SR mock (spec §11).
// Note: ConfigMap content is picked up by the periodic resync, not watched.
func TestTopicReconcile_SchemaConfigMapRegistered(t *testing.T) {
	s := topicScheme(t)
	tp := schemaTopic(v1alpha1.ValueSource{
		ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: "schemas", Key: "order.avsc"},
	})
	c := schemaCluster()
	cm := &corev1.ConfigMap{}
	cm.Name = "schemas"
	cm.Namespace = "ns1"
	cm.Data = map[string]string{
		"order.avsc": `{"type":"record","name":"Order","fields":[]}`,
	}
	cl := newTopicFakeClient(t, s, tp, c, cm)
	k := kafkamock.New(nil, nil)
	sr := srmock.New()
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !callsContain(sr.Calls(), "RegisterSchema payments.orders-value") {
		t.Fatalf("expected RegisterSchema payments.orders-value, got %v", sr.Calls())
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status phase = %+v want Ready", got.Status)
	}
	if cond := findCond(got.Status.Conditions, v1alpha1.CondSchemaSynced); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("SchemaSynced = %v want True", cond)
	}
}

// TestTopicReconcile_SchemaConfigMapMissing verifies that a missing ConfigMap
// causes the schema to be skipped (SchemaSynced=False/SchemaUnresolved) while
// the topic itself still reconciles — mirroring the secretKeyRef-missing path.
func TestTopicReconcile_SchemaConfigMapMissing(t *testing.T) {
	s := topicScheme(t)
	tp := schemaTopic(v1alpha1.ValueSource{
		ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: "schemas", Key: "order.avsc"},
	})
	c := schemaCluster()
	// No ConfigMap seeded — it is missing.
	cl := newTopicFakeClient(t, s, tp, c)
	k := kafkamock.New(nil, nil)
	sr := srmock.New()
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Schema must have been skipped — no RegisterSchema call.
	if callsContain(sr.Calls(), "RegisterSchema") {
		t.Fatalf("schema registry must NOT be called when ConfigMap is missing, got %v", sr.Calls())
	}
	// Topic itself must still reconcile.
	if !callsContain(k.Calls(), "CreateTopic payments.orders") {
		t.Fatalf("topic should still reconcile even with missing ConfigMap, got %v", k.Calls())
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	// Phase stays Ready (schema failure is non-fatal); SchemaSynced=False/SchemaUnresolved.
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status phase = %+v want Ready (schema failure is non-fatal)", got.Status)
	}
	cond := findCond(got.Status.Conditions, v1alpha1.CondSchemaSynced)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "SchemaUnresolved" {
		t.Fatalf("SchemaSynced = %v want False/SchemaUnresolved", cond)
	}
}

// TestTopicReconcile_SelfLockoutWarningEvent pins spec §30.3 in the operator:
// the topic's desired ACLs list only User:svc-checkout, but the cluster's
// connecting principal (auth.scram.username, resolved from the Secret via the
// K8sResolver) is "admin" -> User:admin is not listed -> a Warning event with
// reason SelfLockoutRisk on the Enforce reconcile.
func TestTopicReconcile_SelfLockoutWarningEvent(t *testing.T) {
	s := topicScheme(t)
	tp := topicObj("Enforce")
	tp.Spec.Access = v1alpha1.TopicAccess{Producers: []v1alpha1.ProducerAccess{{Principal: "User:svc-checkout"}}}
	c := topicCluster()
	c.Spec.Auth = &v1alpha1.AuthConfig{
		Mechanism: "SCRAM-SHA-512",
		SCRAM: &v1alpha1.SCRAMAuth{
			Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "kafka-creds", Key: "username"}}},
			Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "kafka-creds", Key: "password"}}},
		},
	}
	sec := &corev1.Secret{Data: map[string][]byte{"username": []byte("admin"), "password": []byte("pw")}}
	sec.Name = "kafka-creds"
	sec.Namespace = c.Namespace
	cl := newTopicFakeClient(t, s, tp, c, sec)
	rec := events.NewFakeRecorder(16)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: kafkamock.New(nil, nil)}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	found := false
	for len(rec.Events) > 0 {
		ev := <-rec.Events
		if strings.Contains(ev, "SelfLockoutRisk") && strings.Contains(ev, "User:admin") {
			found = true
		}
	}
	if !found {
		t.Fatal("expected a SelfLockoutRisk warning event naming User:admin")
	}
}

// TestTopicReconcile_NoSelfLockoutEventWhenListed: the connecting principal IS
// among the topic's desired principals -> no SelfLockoutRisk event.
func TestTopicReconcile_NoSelfLockoutEventWhenListed(t *testing.T) {
	s := topicScheme(t)
	tp := topicObj("Enforce")
	tp.Spec.Access = v1alpha1.TopicAccess{Producers: []v1alpha1.ProducerAccess{{Principal: "User:admin"}}}
	c := topicCluster()
	c.Spec.Auth = &v1alpha1.AuthConfig{
		Mechanism: "SCRAM-SHA-512",
		SCRAM: &v1alpha1.SCRAMAuth{
			Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "kafka-creds", Key: "username"}}},
			Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "kafka-creds", Key: "password"}}},
		},
	}
	sec := &corev1.Secret{Data: map[string][]byte{"username": []byte("admin"), "password": []byte("pw")}}
	sec.Name = "kafka-creds"
	sec.Namespace = c.Namespace
	cl := newTopicFakeClient(t, s, tp, c, sec)
	rec := events.NewFakeRecorder(16)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: kafkamock.New(nil, nil)}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	for len(rec.Events) > 0 {
		if ev := <-rec.Events; strings.Contains(ev, "SelfLockoutRisk") {
			t.Fatalf("unexpected SelfLockoutRisk event: %q", ev)
		}
	}
}

// ---- Subject deletion tests (spec §12) ----

// deleteSetupWithSchema builds a deletion fixture with a topic that declares a
// content-mode schema (inline value schema body). The schema body is stored in a
// Secret or as an inline value; schemaObj pre-populates the topic schema block.
// The SR mock sr is passed in so callers can pre-seed subjects or inject errors.
func deleteSetupWithSchema(
	t *testing.T,
	body string, // inline schema body — passed directly so no Secret is needed
	annotations map[string]string,
	k *kafkamock.Client,
	sr *srmock.Client,
) (client.Client, *runtime.Scheme) {
	t.Helper()
	s := topicScheme(t)
	tp := topicObj("Enforce")
	tp.Spec.DeletionPolicy = "Delete"
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:      "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: body}},
	}
	tp.Finalizers = []string{FinalizerName}
	if annotations != nil {
		tp.Annotations = annotations
	}
	c := schemaCluster()
	cl := newTopicFakeClient(t, s, tp, c)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	return cl, s
}

// avroBody is the minimal AVRO schema body used in deletion tests.
const avroBody = `{"type":"record","name":"Order","fields":[]}`

// TestTopicReconcile_DeleteWithSchema_ValueSubjectDeleted: Delete policy +
// approval + content-mode schema (TopicName strategy) — the topic is deleted
// AND the <topic>-value subject is removed from the SR mock; finalizer removed.
func TestTopicReconcile_DeleteWithSchema_ValueSubjectDeleted(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	sr := srmock.New()
	sr.Seed("payments.orders-value", "AVRO", "", avroBody)

	cl, s := deleteSetupWithSchema(t,
		avroBody,
		map[string]string{"gitops.monedula.dev/allow-delete": "true"},
		k, sr)
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete with schema: %v", err)
	}

	// Kafka topic must be deleted.
	if !callsContain(k.Calls(), "DeleteTopic payments.orders") {
		t.Fatalf("expected DeleteTopic, got %v", k.Calls())
	}
	// SR subject must be soft-deleted.
	if !callsContain(sr.Calls(), "DeleteSubject payments.orders-value") {
		t.Fatalf("expected DeleteSubject payments.orders-value, got %v", sr.Calls())
	}
	// Subject must be gone from the mock.
	got, err := sr.GetSubject(context.Background(), "payments.orders-value")
	if err != nil || got != nil {
		t.Fatalf("subject should be absent after deletion, got %v err %v", got, err)
	}
	// Finalizer removed -> object gone.
	var topic v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &topic); err == nil {
		t.Fatalf("topic should be gone (finalizer removed), finalizers %v", topic.Finalizers)
	}
}

// TestTopicReconcile_DeleteWithSchema_KeySubjectAlsoDeleted: a topic with both
// valueSchema and keySchema — both subjects must be deleted.
func TestTopicReconcile_DeleteWithSchema_KeySubjectAlsoDeleted(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	sr := srmock.New()
	sr.Seed("payments.orders-value", "AVRO", "", avroBody)
	sr.Seed("payments.orders-key", "AVRO", "", avroBody)

	s := topicScheme(t)
	tp := topicObj("Enforce")
	tp.Spec.DeletionPolicy = "Delete"
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:      "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: avroBody}},
		KeySchema:   &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: avroBody}},
	}
	tp.Finalizers = []string{FinalizerName}
	tp.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	c := schemaCluster()
	cl := newTopicFakeClient(t, s, tp, c)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if !callsContain(sr.Calls(), "DeleteSubject payments.orders-value") {
		t.Fatalf("expected DeleteSubject value, got %v", sr.Calls())
	}
	if !callsContain(sr.Calls(), "DeleteSubject payments.orders-key") {
		t.Fatalf("expected DeleteSubject key, got %v", sr.Calls())
	}
}

// TestTopicReconcile_DeleteGovernanceMode_SubjectSurvives: a governance-mode
// topic (spec.schema without valueSchema/keySchema) — the subject is owned by
// the producer, NOT deleted on topic removal (spec §12.2). The producer's
// registered versions must remain after topic finalization.
func TestTopicReconcile_DeleteGovernanceMode_SubjectSurvives(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	sr := srmock.New()
	// Seed with a producer-registered version so GetSubject returns non-nil.
	sr.Seed("payments.orders-value", "AVRO", "BACKWARD", avroBody)

	s := topicScheme(t)
	tp := topicObj("Enforce")
	tp.Spec.DeletionPolicy = "Delete"
	// Governance mode: schema block present but no valueSchema/keySchema body.
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:        "AVRO",
		Compatibility: "BACKWARD",
	}
	tp.Finalizers = []string{FinalizerName}
	tp.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	c := schemaCluster()
	cl := newTopicFakeClient(t, s, tp, c)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete governance: %v", err)
	}

	// Kafka topic deleted.
	if !callsContain(k.Calls(), "DeleteTopic payments.orders") {
		t.Fatalf("expected DeleteTopic, got %v", k.Calls())
	}
	// Subject must NOT be deleted — governance mode.
	if callsContain(sr.Calls(), "DeleteSubject") {
		t.Fatalf("governance subject must NOT be deleted, got %v", sr.Calls())
	}
	// Subject still present (producer's version survives).
	got, err := sr.GetSubject(context.Background(), "payments.orders-value")
	if err != nil || got == nil {
		t.Fatalf("governance subject must survive; got %v err %v", got, err)
	}
	// Finalizer removed.
	var topic v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &topic); err == nil {
		t.Fatalf("topic should be gone, finalizers %v", topic.Finalizers)
	}
}

// TestTopicReconcile_DeleteWithSchema_SubjectAbsent_OK: if the subject is
// already absent in the SR mock, finalization succeeds cleanly (idempotent
// DeleteSubject) with no warning event.
func TestTopicReconcile_DeleteWithSchema_SubjectAbsent_OK(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	sr := srmock.New() // subject NOT seeded — already absent

	cl, s := deleteSetupWithSchema(t,
		avroBody,
		map[string]string{"gitops.monedula.dev/allow-delete": "true"},
		k, sr)
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete (subject absent): %v", err)
	}

	// DeleteSubject must still be called (idempotent).
	if !callsContain(sr.Calls(), "DeleteSubject payments.orders-value") {
		t.Fatalf("expected DeleteSubject call even when absent, got %v", sr.Calls())
	}
	// No warning event.
	for len(rec.Events) > 0 {
		ev := <-rec.Events
		if strings.Contains(ev, "SubjectDeletionFailed") {
			t.Fatalf("unexpected SubjectDeletionFailed event for absent subject: %q", ev)
		}
	}
	// Finalizer removed.
	var topic v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &topic); err == nil {
		t.Fatalf("topic should be gone, finalizers %v", topic.Finalizers)
	}
}

// TestTopicReconcile_DeleteWithSchema_SRError_FinalizerRemoved: if DeleteSubject
// returns an error, a SubjectDeletionFailed Warning event is emitted but the
// finalizer is STILL removed (topic is already gone; orphaned subject beats
// wedged namespace — spec §12).
func TestTopicReconcile_DeleteWithSchema_SRError_FinalizerRemoved(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	sr := srmock.New()
	sr.Seed("payments.orders-value", "AVRO", "", avroBody)
	sr.FailOn("DeleteSubject", "payments.orders-value", errReason("registry unavailable"))

	cl, s := deleteSetupWithSchema(t,
		avroBody,
		map[string]string{"gitops.monedula.dev/allow-delete": "true"},
		k, sr)
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete (SR error): %v (expected nil — finalizer removed despite SR failure)", err)
	}

	// SubjectDeletionFailed warning must be emitted.
	found := false
	for len(rec.Events) > 0 {
		ev := <-rec.Events
		if strings.Contains(ev, "SubjectDeletionFailed") {
			found = true
		}
	}
	if !found {
		t.Fatal("expected SubjectDeletionFailed warning event")
	}
	// Finalizer removed despite SR error.
	var topic v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &topic); err == nil {
		t.Fatalf("topic should be gone even with SR error (finalizer must be removed), finalizers %v", topic.Finalizers)
	}
}

// TestTopicReconcile_DeleteOrphanPolicy_NoSubjectDeletion: Orphan policy skips
// subject deletion entirely (no SR interaction beyond what the factory provides).
func TestTopicReconcile_DeleteOrphanPolicy_NoSubjectDeletion(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	sr := srmock.New()
	sr.Seed("payments.orders-value", "AVRO", "", avroBody)

	s := topicScheme(t)
	tp := topicObj("Enforce")
	tp.Spec.DeletionPolicy = "Orphan"
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:      "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: avroBody}},
	}
	tp.Finalizers = []string{FinalizerName}
	c := schemaCluster()
	cl := newTopicFakeClient(t, s, tp, c)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile orphan with schema: %v", err)
	}

	// No Kafka deletion.
	if callsContain(k.Calls(), "DeleteTopic") {
		t.Fatalf("Orphan must not delete the Kafka topic, got %v", k.Calls())
	}
	// No subject deletion.
	if callsContain(sr.Calls(), "DeleteSubject") {
		t.Fatalf("Orphan must not delete subjects, got %v", sr.Calls())
	}
	// Finalizer removed -> object gone.
	var topic v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &topic); err == nil {
		t.Fatalf("topic should be gone (finalizer removed), finalizers %v", topic.Finalizers)
	}
}

// TestTopicReconcile_DeleteNoSchema_NoSubjectDeletion: topic has no schema
// block — no SR interaction on deletion.
func TestTopicReconcile_DeleteNoSchema_NoSubjectDeletion(t *testing.T) {
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	sr := srmock.New()

	cl, s := deleteSetup(t, "Delete", map[string]string{"gitops.monedula.dev/allow-delete": "true"}, k)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete (no schema): %v", err)
	}

	if callsContain(sr.Calls(), "DeleteSubject") {
		t.Fatalf("no schema block: DeleteSubject must not be called, got %v", sr.Calls())
	}
}

// ---- Bodyless subject-deletion tests (spec §12, fix 1 + fix 3) ----

// TestTopicReconcile_DeleteTopicName_SecretDeleted_BodylessPath is the
// regression test for fix 1: a topic whose valueSchema comes from a secretKeyRef
// AND the Secret was already deleted before finalization. With TopicName strategy
// the subject name is deterministic — no body resolution needed. The subject MUST
// be deleted and NO SubjectDeletionFailed event must be emitted (spec §12).
func TestTopicReconcile_DeleteTopicName_SecretDeleted_BodylessPath(t *testing.T) {
	s := topicScheme(t)

	tp := topicObj("Enforce")
	tp.Spec.DeletionPolicy = "Delete"
	// ValueSchema comes from a Secret that we deliberately do NOT seed in the
	// fake client — it is already deleted before finalization.
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format: "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
			SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "orders-schema", Key: "value.avsc"},
		}},
	}
	tp.Finalizers = []string{FinalizerName}
	tp.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}

	c := schemaCluster()
	// Secret is NOT seeded: simulates Secret deleted before the topic is finalized.
	cl := newTopicFakeClient(t, s, tp, c) // no Secret object
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	k := kafkamock.New(seededTopics("payments.orders"), nil)
	sr := srmock.New()
	sr.Seed("payments.orders-value", "AVRO", "", avroBody)

	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete (Secret deleted, TopicName bodyless): %v", err)
	}

	// Topic must be deleted from Kafka.
	if !callsContain(k.Calls(), "DeleteTopic payments.orders") {
		t.Fatalf("expected DeleteTopic, got %v", k.Calls())
	}
	// <topic>-value must be deleted from SR (bodyless path — no Secret needed).
	if !callsContain(sr.Calls(), "DeleteSubject payments.orders-value") {
		t.Fatalf("expected DeleteSubject payments.orders-value, got %v", sr.Calls())
	}
	// NO SubjectDeletionFailed event: the Secret being absent must NOT cause a
	// warning for TopicName strategy.
	for len(rec.Events) > 0 {
		ev := <-rec.Events
		if strings.Contains(ev, "SubjectDeletionFailed") {
			t.Fatalf("unexpected SubjectDeletionFailed event (bodyless TopicName should not resolve body): %q", ev)
		}
	}
	// Finalizer removed -> object gone.
	var topic v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &topic); err == nil {
		t.Fatalf("topic should be gone (finalizer removed), finalizers %v", topic.Finalizers)
	}
}

// TestTopicReconcile_DeleteRecordName_SecretDeleted_SubjectDeletionFailed mirrors
// TestTopicReconcile_DeleteTopicName_SecretDeleted_BodylessPath for the RecordName
// strategy. When the schema body lives in a secretKeyRef and the Secret is
// ABSENT at finalization, ManagedSubjects returns an error — spec §12 requires:
//
//	(a) SubjectDeletionFailed Warning event fires,
//	(b) finalizer is still removed (CR gone), and
//	(c) DeleteTopic still happened.
func TestTopicReconcile_DeleteRecordName_SecretDeleted_SubjectDeletionFailed(t *testing.T) {
	s := topicScheme(t)

	tp := topicObj("Enforce")
	tp.Spec.DeletionPolicy = "Delete"
	// ValueSchema comes from a Secret that we deliberately do NOT seed in the
	// fake client — it is already deleted before finalization.
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:          "AVRO",
		SubjectStrategy: "RecordName",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
			SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "orders-schema", Key: "value.avsc"},
		}},
	}
	tp.Finalizers = []string{FinalizerName}
	tp.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}

	c := schemaCluster()
	// Secret is NOT seeded: simulates Secret deleted before the topic is finalized.
	cl := newTopicFakeClient(t, s, tp, c) // no Secret object
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	k := kafkamock.New(seededTopics("payments.orders"), nil)
	sr := srmock.New()

	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete (RecordName, Secret deleted): %v (expected nil — finalizer removed despite error)", err)
	}

	// (c) DeleteTopic must still have been called.
	if !callsContain(k.Calls(), "DeleteTopic payments.orders") {
		t.Fatalf("expected DeleteTopic, got %v", k.Calls())
	}
	// (a) SubjectDeletionFailed warning must be emitted.
	found := false
	for len(rec.Events) > 0 {
		ev := <-rec.Events
		if strings.Contains(ev, "SubjectDeletionFailed") {
			found = true
		}
	}
	if !found {
		t.Fatal("expected SubjectDeletionFailed warning event for RecordName with absent Secret")
	}
	// (b) Finalizer removed -> CR gone.
	var topic v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &topic); err == nil {
		t.Fatalf("topic should be gone (finalizer removed), finalizers %v", topic.Finalizers)
	}
}

// TestTopicRecordName_Delete_InlineBody_SubjectDeleted covers gap #4: a topic
// with RecordName strategy and an INLINE schema body (resolvable at finalization
// — no Secret needed). The record-name-derived subject must be deleted from the SR mock.
func TestTopicRecordName_Delete_InlineBody_SubjectDeleted(t *testing.T) {
	s := topicScheme(t)

	const inlineBody = `{"type":"record","name":"OrderEvent","namespace":"com.example","fields":[]}`
	const expectedSubject = "com.example.OrderEvent" // RecordName strategy

	tp := topicObj("Enforce")
	tp.Spec.DeletionPolicy = "Delete"
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:          "AVRO",
		SubjectStrategy: "RecordName",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{
			Inline: inlineBody,
		}},
	}
	tp.Finalizers = []string{FinalizerName}
	tp.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}

	c := schemaCluster()
	cl := newTopicFakeClient(t, s, tp, c)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	k := kafkamock.New(seededTopics("payments.orders"), nil)
	sr := srmock.New()
	sr.Seed(expectedSubject, "AVRO", "", inlineBody)

	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k, sr: sr}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete (RecordName inline): %v", err)
	}

	// Kafka topic deleted.
	if !callsContain(k.Calls(), "DeleteTopic payments.orders") {
		t.Fatalf("expected DeleteTopic, got %v", k.Calls())
	}
	// Record-name-derived subject must be deleted.
	if !callsContain(sr.Calls(), "DeleteSubject "+expectedSubject) {
		t.Fatalf("expected DeleteSubject %s, got %v", expectedSubject, sr.Calls())
	}
	// No failure events.
	for len(rec.Events) > 0 {
		ev := <-rec.Events
		if strings.Contains(ev, "SubjectDeletionFailed") {
			t.Fatalf("unexpected SubjectDeletionFailed event: %q", ev)
		}
	}
	// Finalizer removed.
	var topic v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &topic); err == nil {
		t.Fatalf("topic should be gone (finalizer removed), finalizers %v", topic.Finalizers)
	}
}
