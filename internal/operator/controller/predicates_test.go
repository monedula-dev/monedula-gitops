package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// filterTopic builds a KafkaTopic with the metadata fields the watch filter
// inspects (generation, annotations, finalizers, deletionTimestamp) plus a
// status, so each test case can vary exactly one signal.
func filterTopic(gen int64, rv string) *v1alpha1.KafkaTopic {
	tp := &v1alpha1.KafkaTopic{}
	tp.Name = "orders"
	tp.Namespace = "ns1"
	tp.Generation = gen
	tp.ResourceVersion = rv
	return tp
}

func TestWatchEventFilter_StatusOnlyUpdateFiltered(t *testing.T) {
	p := watchEventFilter()

	oldObj := filterTopic(1, "100")
	newObj := filterTopic(1, "101") // status write: RV bumped, nothing else
	now := metav1.Now()
	newObj.Status = &v1alpha1.KafkaTopicStatus{Phase: v1alpha1.PhaseReady, LastCheckedTime: &now}

	if p.Update(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}) {
		t.Error("status-only update must be filtered out (no re-enqueue)")
	}
}

func TestWatchEventFilter_GenerationChangePasses(t *testing.T) {
	p := watchEventFilter()
	if !p.Update(event.UpdateEvent{ObjectOld: filterTopic(1, "100"), ObjectNew: filterTopic(2, "101")}) {
		t.Error("spec change (generation bump) must pass")
	}
}

func TestWatchEventFilter_AnnotationChangePasses(t *testing.T) {
	p := watchEventFilter()
	oldObj := filterTopic(1, "100")
	newObj := filterTopic(1, "101")
	// The risk gates are annotations; flipping one must re-trigger reconcile.
	newObj.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	if !p.Update(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}) {
		t.Error("annotation change must pass (risk-gate annotations)")
	}
}

func TestWatchEventFilter_DeletionTimestampPasses(t *testing.T) {
	p := watchEventFilter()
	oldObj := filterTopic(1, "100")
	newObj := filterTopic(1, "101")
	now := metav1.Now()
	newObj.DeletionTimestamp = &now
	newObj.Finalizers = []string{FinalizerName}
	oldObj.Finalizers = []string{FinalizerName}
	if !p.Update(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}) {
		t.Error("deletionTimestamp being set must pass (deletion handling)")
	}
}

func TestWatchEventFilter_FinalizerChangePasses(t *testing.T) {
	p := watchEventFilter()
	oldObj := filterTopic(1, "100")
	newObj := filterTopic(1, "101")
	newObj.Finalizers = []string{FinalizerName}
	if !p.Update(event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}) {
		t.Error("finalizer change must pass")
	}
}

func TestWatchEventFilter_CreateAndDeletePass(t *testing.T) {
	p := watchEventFilter()
	obj := filterTopic(1, "100")
	if !p.Create(event.CreateEvent{Object: obj}) {
		t.Error("create events must always pass")
	}
	if !p.Delete(event.DeleteEvent{Object: obj}) {
		t.Error("delete events must always pass")
	}
	if !p.Generic(event.GenericEvent{Object: obj}) {
		t.Error("generic events must always pass")
	}
}
