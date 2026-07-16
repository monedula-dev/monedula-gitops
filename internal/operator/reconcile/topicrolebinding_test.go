package reconcile

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

// rbacCluster returns a KafkaCluster with both "acl" and "rbac" access backends
// and a full MDS config, as required by CompileTopicAccess.
func rbacCluster(backends ...string) *v1alpha1.KafkaCluster {
	if len(backends) == 0 {
		backends = []string{"acl", "rbac"}
	}
	return &v1alpha1.KafkaCluster{
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Authorization: &v1alpha1.AuthorizationConfig{
				AccessBackends: backends,
				MDS: &v1alpha1.MDSConfig{
					Endpoint: "http://mds:8090",
					Clusters: v1alpha1.MDSClusters{
						KafkaCluster: "lkc-abc123",
					},
				},
			},
		},
	}
}

// rbacOnlyCluster returns a cluster with only "rbac" backend (no "acl").
func rbacOnlyCluster() *v1alpha1.KafkaCluster {
	return rbacCluster("rbac")
}

// producerTopic returns a KafkaTopic with a producer access entry.
func producerTopic() *v1alpha1.KafkaTopic {
	tp := &v1alpha1.KafkaTopic{
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "c"},
			TopicName:  "payments.orders",
			Partitions: 3,
			Access: v1alpha1.TopicAccess{
				Producers: []v1alpha1.ProducerAccess{
					{Principal: "User:svc-producer"},
				},
			},
		},
	}
	tp.Name = "orders"
	tp.Namespace = "default"
	tp.Generation = 5
	tp.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: "Enforce"}
	return tp
}

// rbViewFor builds a ClusterRoleBindingView for a given topic + cluster MDS config.
// Used by tests that want a non-nil view.
func rbViewFor(tp *v1alpha1.KafkaTopic, mdsCfg *v1alpha1.MDSConfig) *ClusterRoleBindingView {
	compiled, _, err := rbac.CompileTopicAccess(tp, mdsCfg)
	if err != nil {
		return &ClusterRoleBindingView{}
	}
	rbac.StampPrune(compiled, tp.Spec.Prune)
	return &ClusterRoleBindingView{
		DesiredBindings: compiled,
		DesiredScope:    rbac.BuildScope(compiled),
	}
}

// hasCond reports whether conds contains a condition with the given type and
// status. It is a thin adapter to the existing condStatus helper.
func hasCond(conds []metav1.Condition, typ string, want metav1.ConditionStatus) bool {
	s, _, ok := condStatus(conds, typ)
	return ok && s == want
}

// TestReconcileTopicEmitsRoleBindingsOnRBACBackend verifies that a topic on an
// [acl,rbac] cluster with a producer entry causes at least one MDS AddRoleBinding
// and that the status carries RoleBindingSynced=True.
func TestReconcileTopicEmitsRoleBindingsOnRBACBackend(t *testing.T) {
	cl := rbacCluster("acl", "rbac")
	tp := producerTopic()
	k := kafkamock.New(nil, nil)
	mock := mdsmock.New() // empty live state → AddRoleBinding expected

	rbv := rbViewFor(tp, cl.Spec.Authorization.MDS)
	st, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, mock, rbv)
	if err != nil {
		t.Fatalf("unexpected transient error: %v", err)
	}

	calls := mock.Calls()
	addCalls := 0
	for _, c := range calls {
		if strings.HasPrefix(c, "AddRoleBinding") {
			addCalls++
		}
	}
	if addCalls == 0 {
		t.Fatalf("expected ≥1 AddRoleBinding call, got MDS calls: %v", calls)
	}
	if !hasCond(st.Conditions, v1alpha1.CondRoleBindingSynced, metav1.ConditionTrue) {
		t.Fatalf("RoleBindingSynced should be True; conditions: %v", st.Conditions)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}

// TestReconcileTopicSkipsMDSOnACLBackend verifies that a topic on an acl-only
// cluster never calls MDS, even when an MDS mock is provided.
func TestReconcileTopicSkipsMDSOnACLBackend(t *testing.T) {
	cl := cluster() // default: no accessBackends configured (defaults to "acl")
	tp := producerTopic()
	k := kafkamock.New(nil, nil)
	mock := mdsmock.New()

	_, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, mock, nil)
	if err != nil {
		t.Fatalf("unexpected transient error: %v", err)
	}

	if got := mock.Calls(); len(got) != 0 {
		t.Fatalf("MDS should not be called for non-rbac cluster; got calls: %v", got)
	}
}

// TestReconcileTopicSetsRBACCoarsenedCondition verifies that a topic on an rbac
// cluster whose access entry specifies a non-"*" host gets RBACCoarsened=True
// (RBAC cannot express the host restriction).
func TestReconcileTopicSetsRBACCoarsenedCondition(t *testing.T) {
	cl := rbacOnlyCluster()
	tp := &v1alpha1.KafkaTopic{
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "c"},
			TopicName:  "payments.orders",
			Partitions: 3,
			Access: v1alpha1.TopicAccess{
				Producers: []v1alpha1.ProducerAccess{
					{
						Principal: "User:svc-producer",
						Host:      "10.0.0.1", // non-"*" host → coarsening
					},
				},
			},
			Reconciliation: &v1alpha1.Reconciliation{Mode: "Enforce"},
		},
	}
	tp.Name = "orders"
	tp.Namespace = "default"
	tp.Generation = 5

	k := kafkamock.New(nil, nil)
	mock := mdsmock.New()

	st, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, mock, nil)
	if err != nil {
		t.Fatalf("unexpected transient error: %v", err)
	}

	if !hasCond(st.Conditions, v1alpha1.CondRBACCoarsened, metav1.ConditionTrue) {
		t.Fatalf("RBACCoarsened should be True for non-* host; conditions: %v", st.Conditions)
	}
}

// TestReconcileTopicRBACWithoutMDSIsTerminal verifies that when a cluster has
// the "rbac" backend but Authorization.MDS is nil, the nil-MDS guard fires
// first and returns a TERMINAL (nil-error) ValidationFailed status. The
// mdsClient==nil transient guard never runs because it is guarded behind the
// MDS config check.
func TestReconcileTopicRBACWithoutMDSIsTerminal(t *testing.T) {
	cl := &v1alpha1.KafkaCluster{
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Authorization: &v1alpha1.AuthorizationConfig{
				AccessBackends: []string{"rbac"},
				MDS:            nil, // intentionally absent → terminal guard fires
			},
		},
	}
	tp := producerTopic()
	k := kafkamock.New(nil, nil)
	mock := mdsmock.New()

	st, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, mock, nil)
	if err != nil {
		t.Fatalf("nil-MDS guard returns terminal (nil error); got: %v", err)
	}

	if got := mock.Calls(); len(got) != 0 {
		t.Fatalf("MDS should not be called when MDS config is absent; got calls: %v", got)
	}
	if !hasCond(st.Conditions, v1alpha1.CondValidationFailed, metav1.ConditionTrue) {
		t.Fatalf("ValidationFailed should be True when MDS config absent; conditions: %v", st.Conditions)
	}
}

// TestReconcileTopicRBACListErrorIsTransient verifies that a ListRoleBindings
// failure from MDS is TRANSIENT: ReconcileTopic returns a non-nil error,
// CondMDSReachable=False, CondRoleBindingSynced=False, and CondReady=False.
func TestReconcileTopicRBACListErrorIsTransient(t *testing.T) {
	cl := rbacCluster("acl", "rbac")
	tp := producerTopic()
	k := kafkamock.New(nil, nil)

	listErr := errors.New("MDS connection refused")
	failMock := &failListMDSClient{err: listErr}

	rbv := rbViewFor(tp, cl.Spec.Authorization.MDS)
	st, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, failMock, rbv)
	if err == nil {
		t.Fatalf("ListRoleBindings failure must be transient (non-nil error)")
	}
	if !errors.Is(err, listErr) {
		t.Fatalf("error = %v, want listErr wrapped", err)
	}
	if !hasCond(st.Conditions, v1alpha1.CondMDSReachable, metav1.ConditionFalse) {
		t.Fatalf("MDSReachable should be False on list error; conditions: %v", st.Conditions)
	}
	if !hasCond(st.Conditions, v1alpha1.CondRoleBindingSynced, metav1.ConditionFalse) {
		t.Fatalf("RoleBindingSynced should be False on list error; conditions: %v", st.Conditions)
	}
	if !hasCond(st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse) {
		t.Fatalf("Ready should be False on list error; conditions: %v", st.Conditions)
	}
}
