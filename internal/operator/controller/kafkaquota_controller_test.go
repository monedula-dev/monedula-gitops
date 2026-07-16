//go:build envtest

package controller

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	creconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// compile-time assertion: the quota reconciler is a controller-runtime Reconciler.
var _ creconcile.Reconciler = (*KafkaQuotaReconciler)(nil)

// --- KafkaQuota create ---

func TestEnvtestQuotaCreate(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "quota-create-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	q := &v1alpha1.KafkaQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-create", Namespace: testNamespace},
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "quota-create-cluster"},
			Entity:     v1alpha1.QuotaEntity{User: "User:alice"},
			Limits:     v1alpha1.QuotaLimits{ProducerByteRate: ptr.To(1048576.0)},
		},
	}
	if err := env.cl.Create(ctx, q); err != nil {
		t.Fatalf("create quota: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := &KafkaQuotaReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: mk, sr: schemamock.New()},
	}
	reconcileFor(t, r, testNamespace, "quota-create")

	// The mock does not record quota mutations via Calls(); assert via state.
	quotas, err := mk.ListQuotas(ctx)
	if err != nil {
		t.Fatalf("ListQuotas: %v", err)
	}
	if len(quotas) != 1 {
		t.Fatalf("mock quotas = %+v, want exactly one (the alice quota)", quotas)
	}
	if got := quotas[0].Limits["producer_byte_rate"]; got != 1048576.0 {
		t.Fatalf("producer_byte_rate = %v, want 1048576", got)
	}

	var got v1alpha1.KafkaQuota
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "quota-create"}, &got); err != nil {
		t.Fatalf("re-get quota: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatal("finalizer not persisted on quota")
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status = %+v, want phase Ready", got.Status)
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondQuotaSynced); s != metav1.ConditionTrue {
		t.Fatalf("QuotaSynced = %q, want True", s)
	}
	if got.Status.ObservedLimits == nil || got.Status.ObservedLimits.ProducerByteRate == nil ||
		*got.Status.ObservedLimits.ProducerByteRate != 1048576.0 {
		t.Fatalf("ObservedLimits = %+v, want producerByteRate 1048576", got.Status.ObservedLimits)
	}
}

// --- KafkaQuota tenancy (spec §20.2) ---

// condReason returns the reason of the named condition, or "" if absent.
func condReason(conds []metav1.Condition, typ string) string {
	for _, c := range conds {
		if c.Type == typ {
			return c.Reason
		}
	}
	return ""
}

// TestEnvtestQuotaTenancyDenied verifies that a KafkaQuota in a namespace
// outside the cluster's tenancy allow-list is terminally rejected: Phase
// Error, ValidationFailed=True with reason TenancyDenied, and NO quota written
// to the (mock) Kafka cluster.
func TestEnvtestQuotaTenancyDenied(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	cluster := newCluster(testNamespace, "quota-tenancy-cluster")
	cluster.Spec.Tenancy = &v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-*"}, // testNamespace "default" is NOT allowed
	}
	if err := env.cl.Create(ctx, cluster); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	q := &v1alpha1.KafkaQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-tenancy", Namespace: testNamespace},
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "quota-tenancy-cluster"},
			Entity:     v1alpha1.QuotaEntity{User: "User:mallory"},
			Limits:     v1alpha1.QuotaLimits{ProducerByteRate: ptr.To(1048576.0)},
		},
	}
	if err := env.cl.Create(ctx, q); err != nil {
		t.Fatalf("create quota: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := &KafkaQuotaReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: mk, sr: schemamock.New()},
	}
	reconcileFor(t, r, testNamespace, "quota-tenancy")

	// No quota must have been written to Kafka.
	if quotas, _ := mk.ListQuotas(ctx); len(quotas) != 0 {
		t.Fatalf("tenancy denial must not mutate: mock quotas = %+v", quotas)
	}

	var got v1alpha1.KafkaQuota
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "quota-tenancy"}, &got); err != nil {
		t.Fatalf("re-get quota: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("status = %+v, want phase Error", got.Status)
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondValidationFailed); s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %q, want True", s)
	}
	if reason := condReason(got.Status.Conditions, v1alpha1.CondValidationFailed); reason != "TenancyDenied" {
		t.Fatalf("ValidationFailed reason = %q, want TenancyDenied", reason)
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse {
		t.Fatalf("Ready = %q, want False", s)
	}
}

// --- KafkaQuota delete (ungated: finalizer removes the quota) ---

func TestEnvtestQuotaDelete(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "quota-delete-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	q := &v1alpha1.KafkaQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "quota-delete", Namespace: testNamespace},
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "quota-delete-cluster"},
			Entity:     v1alpha1.QuotaEntity{User: "User:bob"},
			Limits:     v1alpha1.QuotaLimits{ConsumerByteRate: ptr.To(2048.0)},
		},
	}
	if err := env.cl.Create(ctx, q); err != nil {
		t.Fatalf("create quota: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := &KafkaQuotaReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: mk, sr: schemamock.New()},
	}

	// First reconcile: adds the finalizer + sets the quota.
	reconcileFor(t, r, testNamespace, "quota-delete")
	if quotas, _ := mk.ListQuotas(ctx); len(quotas) != 1 {
		t.Fatalf("mock quotas after create = %+v, want one", quotas)
	}
	var afterCreate v1alpha1.KafkaQuota
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "quota-delete"}, &afterCreate); err != nil {
		t.Fatalf("re-get after create: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&afterCreate, FinalizerName) {
		t.Fatal("finalizer not added before deletion")
	}

	if err := env.cl.Delete(ctx, &afterCreate); err != nil {
		t.Fatalf("delete quota: %v", err)
	}

	// Reconcile deletion: the entity's quota is removed (ungated), finalizer
	// removed, object GC'd.
	reconcileFor(t, r, testNamespace, "quota-delete")
	if quotas, _ := mk.ListQuotas(ctx); len(quotas) != 0 {
		t.Fatalf("mock quotas after delete = %+v, want none (quota removed)", quotas)
	}

	var gone v1alpha1.KafkaQuota
	err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "quota-delete"}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("quota still present after finalizer removal; get err = %v", err)
	}
}
