package reconcile

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	srmock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// failTopicsAdmin wraps a kafka.AdminClient and forces ListTopics to error,
// simulating an unreachable / unauthenticated cluster.
type failTopicsAdmin struct {
	kafka.AdminClient
}

func (f failTopicsAdmin) ListTopics(context.Context) ([]kafka.TopicState, error) {
	return nil, errors.New("dial tcp: connection refused")
}

// failSubjectsSR wraps a schemaregistry.Client and forces ListSubjects to error.
type failSubjectsSR struct {
	schemaregistry.Client
}

func (f failSubjectsSR) ListSubjects(context.Context) ([]string, error) {
	return nil, errors.New("schema registry unreachable")
}

func newCluster(withSR bool) *v1alpha1.KafkaCluster {
	c := &v1alpha1.KafkaCluster{}
	c.Name = "prod"
	c.Generation = 7
	if withSR {
		c.Spec.SchemaRegistry = &v1alpha1.SchemaRegistryConf{Endpoint: "http://sr:8081"}
	}
	return c
}

func clusterCond(conds []metav1.Condition, typ string) metav1.ConditionStatus {
	if c := meta.FindStatusCondition(conds, typ); c != nil {
		return c.Status
	}
	return ""
}

func TestReconcileCluster_Reachable(t *testing.T) {
	c := newCluster(false)
	k := kafkamock.New(nil, nil)
	st := ReconcileCluster(context.Background(), c, k, nil)

	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q want Ready", st.Phase)
	}
	if got := clusterCond(st.Conditions, v1alpha1.CondClusterReachable); got != metav1.ConditionTrue {
		t.Fatalf("ClusterReachable = %q want True", got)
	}
	if got := clusterCond(st.Conditions, v1alpha1.CondAuthenticated); got != metav1.ConditionTrue {
		t.Fatalf("Authenticated = %q want True", got)
	}
	if got := clusterCond(st.Conditions, v1alpha1.CondReady); got != metav1.ConditionTrue {
		t.Fatalf("Ready = %q want True", got)
	}
	if st.ObservedGeneration != 7 {
		t.Fatalf("ObservedGeneration = %d want 7", st.ObservedGeneration)
	}
	if st.LastCheckedTime == nil {
		t.Fatal("LastCheckedTime not set")
	}
	// No SR configured -> no SchemaRegistryReachable condition.
	if meta.FindStatusCondition(st.Conditions, v1alpha1.CondSchemaRegistryReachable) != nil {
		t.Fatal("SchemaRegistryReachable should be omitted when no SR configured")
	}
}

func TestReconcileCluster_Unreachable(t *testing.T) {
	c := newCluster(false)
	k := failTopicsAdmin{kafkamock.New(nil, nil)}
	st := ReconcileCluster(context.Background(), c, k, nil)

	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q want Error", st.Phase)
	}
	if got := clusterCond(st.Conditions, v1alpha1.CondClusterReachable); got != metav1.ConditionFalse {
		t.Fatalf("ClusterReachable = %q want False", got)
	}
	if got := clusterCond(st.Conditions, v1alpha1.CondReady); got != metav1.ConditionFalse {
		t.Fatalf("Ready = %q want False", got)
	}
	// Authenticated should be False (or Unknown) when the admin call failed.
	if got := clusterCond(st.Conditions, v1alpha1.CondAuthenticated); got == metav1.ConditionTrue {
		t.Fatalf("Authenticated = True; want not-True when admin call failed")
	}
}

func TestReconcileCluster_SRReachable(t *testing.T) {
	c := newCluster(true)
	k := kafkamock.New(nil, nil)
	sr := srmock.New()
	st := ReconcileCluster(context.Background(), c, k, sr)

	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q want Ready", st.Phase)
	}
	if got := clusterCond(st.Conditions, v1alpha1.CondSchemaRegistryReachable); got != metav1.ConditionTrue {
		t.Fatalf("SchemaRegistryReachable = %q want True", got)
	}
}

// TestReconcileCluster_SRConditionRemovedWhenUnconfigured pins review I11: a
// cluster that HAD a Schema Registry (SchemaRegistryReachable set) and then
// drops it from the spec must see the stale condition REMOVED on the next
// reconcile — conditions are seeded from the prior status, so without an
// explicit removal it would linger forever.
func TestReconcileCluster_SRConditionRemovedWhenUnconfigured(t *testing.T) {
	// First pass: SR configured and reachable -> condition True.
	c1 := newCluster(true)
	k := kafkamock.New(nil, nil)
	st1 := ReconcileCluster(context.Background(), c1, k, srmock.New())
	if got := clusterCond(st1.Conditions, v1alpha1.CondSchemaRegistryReachable); got != metav1.ConditionTrue {
		t.Fatalf("SchemaRegistryReachable = %q want True on first pass", got)
	}

	// Second pass: SR removed from the spec, prior status fed back.
	c2 := newCluster(false)
	c2.Status = &st1
	st2 := ReconcileCluster(context.Background(), c2, k, nil)
	if cond := meta.FindStatusCondition(st2.Conditions, v1alpha1.CondSchemaRegistryReachable); cond != nil {
		t.Fatalf("SchemaRegistryReachable = %+v, want condition REMOVED when SR no longer configured (stale condition)", cond)
	}
	if st2.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q want Ready without SR", st2.Phase)
	}
}

func TestReconcileCluster_SRUnreachable(t *testing.T) {
	c := newCluster(true)
	k := kafkamock.New(nil, nil)
	sr := failSubjectsSR{srmock.New()}
	st := ReconcileCluster(context.Background(), c, k, sr)

	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q want Error", st.Phase)
	}
	if got := clusterCond(st.Conditions, v1alpha1.CondClusterReachable); got != metav1.ConditionTrue {
		t.Fatalf("ClusterReachable = %q want True", got)
	}
	if got := clusterCond(st.Conditions, v1alpha1.CondSchemaRegistryReachable); got != metav1.ConditionFalse {
		t.Fatalf("SchemaRegistryReachable = %q want False", got)
	}
	if got := clusterCond(st.Conditions, v1alpha1.CondReady); got != metav1.ConditionFalse {
		t.Fatalf("Ready = %q want False", got)
	}
}
