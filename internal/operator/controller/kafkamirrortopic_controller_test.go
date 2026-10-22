package controller

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	restmock "github.com/monedula-dev/monedula-gitops/internal/kafkarest/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

const (
	mirrorCtrlNS        = "team-a"
	mirrorCtrlCluster   = "prod"
	mirrorCtrlClusterID = "lkc-test"
	mirrorCtrlLink      = "link-a"
	mirrorCtrlTopic     = "orders"
)

// countingFactory wraps stubFactory and counts every client build, so a test
// can prove a code path never contacts the cluster.
type countingFactory struct {
	stubFactory
	calls atomic.Int32
}

func (f *countingFactory) For(ctx context.Context, c *v1alpha1.KafkaCluster) (kafka.AdminClient, schemaregistry.Client, func(), error) {
	f.calls.Add(1)
	return f.stubFactory.For(ctx, c)
}

func (f *countingFactory) KafkaFor(ctx context.Context, c *v1alpha1.KafkaCluster) (kafka.AdminClient, func(), error) {
	f.calls.Add(1)
	return f.stubFactory.KafkaFor(ctx, c)
}

func (f *countingFactory) MDSFor(ctx context.Context, c *v1alpha1.KafkaCluster) (mds.Client, error) {
	f.calls.Add(1)
	return f.stubFactory.MDSFor(ctx, c)
}

func (f *countingFactory) RESTFor(ctx context.Context, c *v1alpha1.KafkaCluster) (kafkarest.Client, error) {
	f.calls.Add(1)
	return f.stubFactory.RESTFor(ctx, c)
}

// mirrorCtrlFixture returns a destination cluster whose Kafka and REST IDs
// match and whose link is ACTIVE. When status is non-empty the destination
// topic exists and REST reports it as a mirror with that status.
func mirrorCtrlFixture(status string) (*kafkamock.Client, *restmock.Client) {
	var topics []kafka.TopicState
	if status != "" {
		topics = []kafka.TopicState{{Name: mirrorCtrlTopic, Partitions: 3, ReplicationFactor: 3}}
	}
	k := kafkamock.New(topics, nil)
	k.SetClusterID(mirrorCtrlClusterID)
	r := restmock.New(mirrorCtrlClusterID)
	r.AddLink(kafkarest.Link{Name: mirrorCtrlLink, State: "ACTIVE"},
		map[string]string{"topic.config.sync.include": "cleanup.policy"})
	if status != "" {
		r.AddMirror(kafkarest.Mirror{
			LinkName:        mirrorCtrlLink,
			MirrorTopicName: mirrorCtrlTopic,
			SourceTopicName: mirrorCtrlTopic,
			Status:          status,
		})
	}
	return k, r
}

func mirrorCtrlCR(name string, ts metav1.Time) *v1alpha1.KafkaMirrorTopic {
	return &v1alpha1.KafkaMirrorTopic{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: mirrorCtrlNS, Name: name, Generation: 1, CreationTimestamp: ts,
			UID: types.UID(mirrorCtrlNS + "/" + name),
		},
		Spec: v1alpha1.KafkaMirrorTopicSpec{
			ClusterRef:      v1alpha1.ClusterRef{Name: mirrorCtrlCluster},
			LinkName:        mirrorCtrlLink,
			SourceTopicName: mirrorCtrlTopic,
		},
	}
}

func mirrorCtrlClusterObj() *v1alpha1.KafkaCluster {
	c := &v1alpha1.KafkaCluster{}
	c.Name = mirrorCtrlCluster
	c.Namespace = mirrorCtrlNS
	c.Generation = 1
	return c
}

func mirrorFakeBuilder(s *runtime.Scheme, objs ...client.Object) *fake.ClientBuilder {
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.KafkaMirrorTopic{}, &v1alpha1.KafkaTopic{}, &v1alpha1.KafkaCluster{})
}

func reconcileMirrorReq(t *testing.T, r *KafkaMirrorTopicReconciler, name string) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: mirrorCtrlNS, Name: name},
	})
}

func getMirror(t *testing.T, c client.Client, name string) *v1alpha1.KafkaMirrorTopic {
	t.Helper()
	var m v1alpha1.KafkaMirrorTopic
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: mirrorCtrlNS, Name: name}, &m); err != nil {
		t.Fatalf("get mirror %s: %v", name, err)
	}
	return &m
}

// TestMirrorReconcilerSkipsSchemaRegistryClient: mirrors never use Schema
// Registry, so a registry client that cannot be built must not block them.
func TestMirrorReconcilerSkipsSchemaRegistryClient(t *testing.T) {
	s := newScheme(t)
	c := mirrorFakeBuilder(s, mirrorCtrlClusterObj(), mirrorCtrlCR("orders-mirror", dupT0)).Build()
	k, rest := mirrorCtrlFixture("")
	r := &KafkaMirrorTopicReconciler{Client: c, Scheme: s,
		Clients: stubFactory{k: k, rest: rest, srErr: errors.New("injected: schema registry secret missing")}}

	if _, err := reconcileMirrorReq(t, r, "orders-mirror"); err != nil {
		t.Fatalf("a Schema Registry build failure must not affect a mirror: %v", err)
	}
	if got := rest.Created(); len(got) != 1 {
		t.Fatalf("CreateMirror calls = %v, want 1", got)
	}
}

func TestMirrorReconcilerCreatesMirror(t *testing.T) {
	s := newScheme(t)
	c := mirrorFakeBuilder(s, mirrorCtrlClusterObj(), mirrorCtrlCR("orders-mirror", dupT0)).Build()
	k, rest := mirrorCtrlFixture("")
	r := &KafkaMirrorTopicReconciler{Client: c, Scheme: s, Clients: stubFactory{k: k, rest: rest}}

	res, err := reconcileMirrorReq(t, r, "orders-mirror")
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != resyncInterval(0) {
		t.Fatalf("RequeueAfter = %v, want resync %v", res.RequeueAfter, resyncInterval(0))
	}
	if created := rest.Created(); len(created) != 1 {
		t.Fatalf("want 1 CreateMirror, got %d", len(created))
	}
	got := getMirror(t, c, "orders-mirror")
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status = %+v, want phase Ready", got.Status)
	}
	if condStatusOf(got.Status.Conditions, v1alpha1.CondReady) != metav1.ConditionTrue {
		t.Fatalf("Ready not True: %+v", got.Status.Conditions)
	}
	if len(got.Finalizers) != 0 {
		t.Fatalf("a KafkaMirrorTopic must never get a finalizer, got %v", got.Finalizers)
	}
}

// TestMirrorReconcilerDeleteNeverContactsCluster: a deleting mirror whose
// cluster CR is gone and whose client factory would fail finishes cleanly
// without building any client; a leftover finalizer from an older build is
// removed so the CR can go away.
func TestMirrorReconcilerDeleteNeverContactsCluster(t *testing.T) {
	s := newScheme(t)
	m := mirrorCtrlCR("orders-mirror", dupT0)
	m.Finalizers = []string{FinalizerName}
	now := metav1.Now()
	m.DeletionTimestamp = &now
	c := mirrorFakeBuilder(s, m).Build()
	f := &countingFactory{stubFactory: stubFactory{err: errors.New("boom"), restErr: errors.New("boom")}}
	r := &KafkaMirrorTopicReconciler{Client: c, Scheme: s, Clients: f}

	res, err := reconcileMirrorReq(t, r, "orders-mirror")
	if err != nil {
		t.Fatalf("deletion reconcile returned error: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Fatalf("deletion reconcile result = %+v, want empty", res)
	}
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("deletion built %d clients; it must never contact the cluster", n)
	}
	var gone v1alpha1.KafkaMirrorTopic
	err = c.Get(context.Background(), types.NamespacedName{Namespace: mirrorCtrlNS, Name: "orders-mirror"}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("mirror should be gone after the leftover finalizer is removed; get err = %v, finalizers = %v",
			err, gone.Finalizers)
	}
}

// TestMirrorReconcilerStatusWriteIdempotent: a second reconcile against
// unchanged live state, at a later time, writes no status (only
// LastCheckedTime would differ).
func TestMirrorReconcilerStatusWriteIdempotent(t *testing.T) {
	s := newScheme(t)
	var statusWrites atomic.Int32
	c := mirrorFakeBuilder(s, mirrorCtrlClusterObj(), mirrorCtrlCR("orders-mirror", dupT0)).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if sub == "status" {
					statusWrites.Add(1)
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).Build()
	k, rest := mirrorCtrlFixture("ACTIVE")
	clock := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	r := &KafkaMirrorTopicReconciler{
		Client: c, Scheme: s, Clients: stubFactory{k: k, rest: rest},
		Now: func() time.Time { return clock },
	}

	if _, err := reconcileMirrorReq(t, r, "orders-mirror"); err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if n := statusWrites.Load(); n != 1 {
		t.Fatalf("first reconcile status writes = %d, want 1", n)
	}
	first := getMirror(t, c, "orders-mirror")
	if first.Status == nil || first.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("first status = %+v, want Ready", first.Status)
	}

	clock = clock.Add(5 * time.Minute)
	if _, err := reconcileMirrorReq(t, r, "orders-mirror"); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if n := statusWrites.Load(); n != 1 {
		t.Fatalf("second reconcile wrote status again (total writes %d); unchanged state must not be written", n)
	}
	if len(rest.Created()) != 0 {
		t.Fatalf("an existing ACTIVE mirror must not be re-created")
	}
}

// TestMirrorReconcilerLosesToOlderTopic: an older KafkaTopic on the same
// destination topic makes the mirror terminal DuplicateIdentity before any
// client is built.
func TestMirrorReconcilerLosesToOlderTopic(t *testing.T) {
	s := newScheme(t)
	topic := stampedTopic(mirrorCtrlNS, "orders-topic", mirrorCtrlCluster, mirrorCtrlTopic, dupT0)
	c := mirrorFakeBuilder(s, mirrorCtrlClusterObj(), topic, mirrorCtrlCR("orders-mirror", dupLater())).Build()
	f := &countingFactory{}
	r := &KafkaMirrorTopicReconciler{Client: c, Scheme: s, Clients: f, APIReader: c}

	res, err := reconcileMirrorReq(t, r, "orders-mirror")
	if err != nil {
		t.Fatalf("loser reconcile must be terminal (nil error), got %v", err)
	}
	if res.RequeueAfter != resyncInterval(0) {
		t.Fatalf("RequeueAfter = %v, want resync", res.RequeueAfter)
	}
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("the loser built %d clients; it must not contact the cluster", n)
	}
	got := getMirror(t, c, "orders-mirror")
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("status = %+v, want phase Error", got.Status)
	}
	vf := dupCond(got.Status.Conditions, v1alpha1.CondValidationFailed)
	if vf == nil || vf.Status != metav1.ConditionTrue || vf.Reason != reasonDuplicateIdentity {
		t.Fatalf("ValidationFailed = %+v, want True/DuplicateIdentity", vf)
	}
	if !strings.Contains(vf.Message, "KafkaTopic "+mirrorCtrlNS+"/orders-topic") {
		t.Fatalf("message must name the winning topic: %q", vf.Message)
	}
}

// TestMirrorReconcilerClusterNotFound: a missing cluster is an error status
// (Ready False / ClusterNotFound) and a backoff requeue.
func TestMirrorReconcilerClusterNotFound(t *testing.T) {
	s := newScheme(t)
	c := mirrorFakeBuilder(s, mirrorCtrlCR("orders-mirror", dupT0)).Build()
	f := &countingFactory{}
	r := &KafkaMirrorTopicReconciler{Client: c, Scheme: s, Clients: f}

	if _, err := reconcileMirrorReq(t, r, "orders-mirror"); err == nil {
		t.Fatalf("missing cluster must return an error for backoff")
	}
	got := getMirror(t, c, "orders-mirror")
	rc := dupCond(got.Status.Conditions, v1alpha1.CondReady)
	if got.Status.Phase != v1alpha1.PhaseError || rc == nil || rc.Reason != "ClusterNotFound" {
		t.Fatalf("status = %+v, want Error with Ready reason ClusterNotFound", got.Status)
	}
	if n := f.calls.Load(); n != 0 {
		t.Fatalf("built %d clients without a cluster", n)
	}
}

// TestMirrorReconcilerClientsBuildFailed: a REST client build failure is a
// ClientsBuildFailed error status and a backoff requeue.
func TestMirrorReconcilerClientsBuildFailed(t *testing.T) {
	s := newScheme(t)
	c := mirrorFakeBuilder(s, mirrorCtrlClusterObj(), mirrorCtrlCR("orders-mirror", dupT0)).Build()
	k, _ := mirrorCtrlFixture("")
	r := &KafkaMirrorTopicReconciler{Client: c, Scheme: s,
		Clients: stubFactory{k: k, restErr: errors.New("rest down")}}

	if _, err := reconcileMirrorReq(t, r, "orders-mirror"); err == nil {
		t.Fatalf("client build failure must return an error")
	}
	got := getMirror(t, c, "orders-mirror")
	rc := dupCond(got.Status.Conditions, v1alpha1.CondReady)
	if rc == nil || rc.Reason != "ClientsBuildFailed" {
		t.Fatalf("Ready = %+v, want reason ClientsBuildFailed", rc)
	}
}

// TestMapSecretToMirrorTopics: a cluster credential Secret fans out to the
// mirrors on the clusters referencing it.
func TestMapSecretToMirrorTopics(t *testing.T) {
	ns := mirrorCtrlNS
	onProd := mirrorCtrlCR("on-prod", dupT0)
	onOther := mirrorCtrlCR("on-other", dupT0)
	onOther.Spec.ClusterRef.Name = "staging"
	c := newUserSecretTestClient(t, clusterWithSecret(ns, mirrorCtrlCluster, "kafka-creds"), onProd, onOther)
	r := &KafkaMirrorTopicReconciler{Client: c}

	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "kafka-creds"}}
	got := r.mapSecretToMirrorTopics(context.Background(), secret)
	if len(got) != 1 || got[0].Name != "on-prod" || got[0].Namespace != ns {
		t.Fatalf("mapSecretToMirrorTopics = %+v, want [%s/on-prod]", got, ns)
	}
}

func condStatusOf(conds []metav1.Condition, typ string) metav1.ConditionStatus {
	if c := dupCond(conds, typ); c != nil {
		return c.Status
	}
	return ""
}
