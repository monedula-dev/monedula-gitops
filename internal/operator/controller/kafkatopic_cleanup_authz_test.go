package controller

import (
	"context"
	"strings"
	"testing"

	"k8s.io/client-go/tools/events"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	srmock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// Tests for the finalizer cleanup-authorization gate (handleDeletion step 1b)
// and subject tenancy: a KafkaTopic may only make the operator touch Kafka
// topics, ACLs and Schema Registry subjects that a reconcile of its applied
// spec, under the current tenancy policy, would manage.

const inlineAvro = `{"type":"record","name":"OrderEvent","namespace":"com.example","fields":[]}`

// tenantCluster is a schema-registry cluster that restricts ns1 to the
// "payments." prefix.
func tenantCluster() *v1alpha1.KafkaCluster {
	c := schemaCluster()
	c.Spec.Tenancy = &v1alpha1.TenancyConfig{
		TopicPrefixes: []v1alpha1.TopicPrefixRule{{Namespaces: []string{"ns1"}, Prefixes: []string{"payments."}}},
	}
	return c
}

// customSubjectTopic is an approved, Delete-policy payments.orders topic whose
// Custom-strategy value subject is subject.
func customSubjectTopic(subject string) *v1alpha1.KafkaTopic {
	tp := topicObj("Enforce")
	tp.Spec.DeletionPolicy = "Delete"
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:          "AVRO",
		SubjectStrategy: "Custom",
		ValueSubject:    subject,
		ValueSchema:     &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: inlineAvro}},
	}
	tp.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	return tp
}

func runTopicDeletion(t *testing.T, tp *v1alpha1.KafkaTopic, c *v1alpha1.KafkaCluster) (*kafkamock.Client, *srmock.Client, []string) {
	t.Helper()
	s := topicScheme(t)
	tp.Finalizers = []string{FinalizerName}
	cl := newTopicFakeClient(t, s, tp, c)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	k := kafkamock.New(seededTopics("payments.orders"), nil)
	sr := srmock.New()
	sr.Seed("payments.orders-value", "AVRO", "", inlineAvro)
	sr.Seed("billing.invoices-value", "AVRO", "", inlineAvro)
	rec := events.NewFakeRecorder(16)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k, sr: sr}}
	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}
	var evs []string
	for len(rec.Events) > 0 {
		evs = append(evs, <-rec.Events)
	}
	return k, sr, evs
}

func hasEvent(evs []string, substr string) bool {
	for _, e := range evs {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

// A spec edited after the last reconcile (e.g. while the CR terminates) must
// not drive destructive cleanup: the finalizer orphans instead.
func TestTopicDelete_UnreconciledSpecSkipsCleanup(t *testing.T) {
	tp := customSubjectTopic("payments.orders-value")
	tp.Generation = 3 // reconciled at 1; +1 deletion bump, +1 spec edit
	tp.Status = &v1alpha1.KafkaTopicStatus{ObservedGeneration: 1}
	k, sr, evs := runTopicDeletion(t, tp, tenantCluster())
	if len(k.Calls()) != 0 || callsContain(sr.Calls(), "DeleteSubject") {
		t.Fatalf("no cleanup expected; kafka %v, sr %v", k.Calls(), sr.Calls())
	}
	if !hasEvent(evs, reasonCleanupSkipped) {
		t.Fatalf("expected a %s event, got %v", reasonCleanupSkipped, evs)
	}
}

// A topic whose name the current tenancy policy denies must not have the
// operator delete that Kafka topic on its behalf.
func TestTopicDelete_TenancyDeniedTopicSkipsCleanup(t *testing.T) {
	tp := customSubjectTopic("payments.orders-value")
	markReconciled(tp)
	c := tenantCluster()
	c.Spec.Tenancy.TopicPrefixes[0].Prefixes = []string{"billing."}
	k, _, evs := runTopicDeletion(t, tp, c)
	if callsContain(k.Calls(), "DeleteTopic") || callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("no Kafka cleanup expected, got %v", k.Calls())
	}
	if !hasEvent(evs, reasonCleanupSkipped) {
		t.Fatalf("expected a %s event, got %v", reasonCleanupSkipped, evs)
	}
}

// A Custom subject outside the namespace's prefixes is never deleted, while
// the topic itself (in scope) still is.
func TestTopicDelete_ForeignSubjectNotDeleted(t *testing.T) {
	tp := customSubjectTopic("billing.invoices-value")
	markReconciled(tp)
	k, sr, evs := runTopicDeletion(t, tp, tenantCluster())
	if !callsContain(k.Calls(), "DeleteTopic payments.orders") {
		t.Fatalf("in-scope topic should still be deleted, got %v", k.Calls())
	}
	if callsContain(sr.Calls(), "DeleteSubject") {
		t.Fatalf("foreign subject must not be deleted, got %v", sr.Calls())
	}
	if !hasEvent(evs, "billing.invoices-value left in place") {
		t.Fatalf("expected a subject-left-in-place event, got %v", evs)
	}
}

// Reconcile must refuse (TenancyDenied, no registry writes) a topic whose
// Custom subject is outside the namespace's prefixes.
func TestTopicReconcile_ForeignSubjectTenancyDenied(t *testing.T) {
	s := topicScheme(t)
	tp := customSubjectTopic("billing.invoices-value")
	cl := newTopicFakeClient(t, s, tp, tenantCluster())
	k := kafkamock.New(nil, nil)
	sr := srmock.New()
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k, sr: sr}}
	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	for _, c := range sr.Calls() {
		if strings.HasPrefix(c, "Register") || strings.HasPrefix(c, "SetCompatibility") || strings.HasPrefix(c, "DeleteSubject") {
			t.Fatalf("no registry mutation expected, got %v", sr.Calls())
		}
	}
	if callsContain(k.Calls(), "CreateTopic") {
		t.Fatalf("no Kafka mutation expected, got %v", k.Calls())
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("expected Phase Error, got %+v", got.Status)
	}
	found := false
	for _, cond := range got.Status.Conditions {
		if cond.Reason == "TenancyDenied" && strings.Contains(cond.Message, "billing.invoices-value") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected a TenancyDenied condition naming the subject, got %+v", got.Status.Conditions)
	}
}
