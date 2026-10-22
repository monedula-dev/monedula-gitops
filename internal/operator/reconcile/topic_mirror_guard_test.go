package reconcile

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	restmock "github.com/monedula-dev/monedula-gitops/internal/kafkarest/mock"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
)

// guardTopic is a KafkaTopic for "orders" whose desired config differs from
// guardLiveTopic, so an unguarded Enforce reconcile plans UpdateTopicConfig.
func guardTopic(mode string) *v1alpha1.KafkaTopic {
	tp := baseTopic(mode)
	tp.Spec.TopicName = "orders"
	return tp
}

func guardLiveTopic() []kafka.TopicState {
	return []kafka.TopicState{{Name: "orders", Partitions: 3, Config: map[string]string{"retention.ms": "1000"}}}
}

func guardKafka(topics []kafka.TopicState) *kafkamock.Client {
	k := kafkamock.New(topics, nil)
	k.SetClusterID("lkc-1")
	return k
}

func TestReconcileTopicRejectsLiveMirror(t *testing.T) {
	for _, status := range []string{"ACTIVE", "PENDING_STOPPED"} {
		t.Run(status, func(t *testing.T) {
			k := guardKafka(guardLiveTopic())
			r := restmock.New("lkc-1")
			r.AddMirror(kafkarest.Mirror{LinkName: "link-a", MirrorTopicName: "orders", SourceTopicName: "orders", Status: status})

			st, err := ReconcileTopic(context.Background(), guardTopic("Enforce"), cluster(), k, nil, stubResolver{}, nil, nil, nil, r)
			if err != nil {
				t.Fatalf("Rejected is terminal, want nil error, got %v", err)
			}
			if calls := k.Calls(); len(calls) != 0 {
				t.Fatalf("guarded mirror topic was mutated: calls = %v", calls)
			}
			if st.Drift == nil || !st.Drift.Detected || len(st.Drift.Fields) != 1 || !strings.HasPrefix(st.Drift.Fields[0], "Rejected orders") {
				t.Fatalf("drift = %+v, want Rejected orders only", st.Drift)
			}
			if st.Phase != v1alpha1.PhaseError {
				t.Fatalf("phase = %q, want Error", st.Phase)
			}
		})
	}
}

func TestReconcileTopicRESTDownBlocks(t *testing.T) {
	for _, mode := range []string{"Enforce", "DetectOnly", "ObserveOnly"} {
		t.Run(mode, func(t *testing.T) {
			k := guardKafka(nil) // topic absent: unguarded Enforce would CreateTopic
			r := restmock.New("lkc-1")
			r.Fail("ListMirrors", errors.New("rest down"))

			st, err := ReconcileTopic(context.Background(), guardTopic(mode), cluster(), k, nil, stubResolver{}, nil, nil, nil, r)
			if err == nil || !strings.Contains(err.Error(), "rest down") {
				t.Fatalf("err = %v, want a non-nil error mentioning the REST failure", err)
			}
			if calls := k.Calls(); len(calls) != 0 {
				t.Fatalf("state read error must block mutation: calls = %v", calls)
			}
			if st.Phase != v1alpha1.PhaseError {
				t.Fatalf("phase = %q, want Error", st.Phase)
			}
			if s, reason, _ := condStatus(st.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse || reason != reasonLiveStateError {
				t.Fatalf("Ready = %v/%s, want False/%s", s, reason, reasonLiveStateError)
			}
		})
	}
}

func TestReconcileTopicClusterIDMismatchBlocks(t *testing.T) {
	k := guardKafka(nil)
	r := restmock.New("lkc-other")
	_, err := ReconcileTopic(context.Background(), guardTopic("Enforce"), cluster(), k, nil, stubResolver{}, nil, nil, nil, r)
	if err == nil {
		t.Fatal("cluster identity mismatch must block with an error")
	}
	if calls := k.Calls(); len(calls) != 0 {
		t.Fatalf("calls = %v, want none", calls)
	}
}

func TestReconcileTopicNoRESTUnchanged(t *testing.T) {
	want := []string{"UpdateTopicConfig orders"}

	k := guardKafka(guardLiveTopic())
	st, err := ReconcileTopic(context.Background(), guardTopic("Enforce"), cluster(), k, nil, stubResolver{}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := k.Calls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}

	// A normal topic on a REST-configured cluster plans the same calls.
	k2 := guardKafka(guardLiveTopic())
	r := restmock.New("lkc-1")
	r.AddMirror(kafkarest.Mirror{LinkName: "link-a", MirrorTopicName: "other", Status: "ACTIVE"})
	if _, err := ReconcileTopic(context.Background(), guardTopic("Enforce"), cluster(), k2, nil, stubResolver{}, nil, nil, nil, r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := k2.Calls(); !reflect.DeepEqual(got, want) {
		t.Fatalf("normal topic with REST: calls = %v, want %v", got, want)
	}
}

func TestReconcileTopicStoppedFormerMirrorManaged(t *testing.T) {
	k := guardKafka(guardLiveTopic())
	r := restmock.New("lkc-1")
	r.AddMirror(kafkarest.Mirror{LinkName: "link-a", MirrorTopicName: "orders", Status: "STOPPED"})
	if _, err := ReconcileTopic(context.Background(), guardTopic("Enforce"), cluster(), k, nil, stubResolver{}, nil, nil, nil, r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := k.Calls(); !reflect.DeepEqual(got, []string{"UpdateTopicConfig orders"}) {
		t.Fatalf("calls = %v, want UpdateTopicConfig on a stopped former mirror", got)
	}
}

// Access-derived mutations of a guarded topic are blocked on the rbac backend
// too: no role binding is written for a live mirror topic.
func TestReconcileTopicLiveMirrorSkipsRoleBindings(t *testing.T) {
	cl := rbacCluster("rbac")
	tp := producerTopic()
	tp.Spec.TopicName = "orders"
	k := guardKafka([]kafka.TopicState{{Name: "orders", Partitions: 3}})
	r := restmock.New("lkc-1")
	r.AddMirror(kafkarest.Mirror{LinkName: "link-a", MirrorTopicName: "orders", Status: "ACTIVE"})
	m := mdsmock.New()

	if _, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, m, rbViewFor(tp, cl.Spec.Authorization.MDS), r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, c := range m.Calls() {
		if strings.HasPrefix(c, "AddRoleBinding") || strings.HasPrefix(c, "RemoveRoleBinding") {
			t.Fatalf("role binding mutated for a live mirror topic: %v", m.Calls())
		}
	}
}
