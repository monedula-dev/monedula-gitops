package e2e

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/operator/manager"
)

func topicWithCond(ns, name, ctype, status, reason string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schemaGVK("KafkaTopic"))
	u.SetNamespace(ns)
	u.SetName(name)
	_ = unstructured.SetNestedSlice(u.Object, []interface{}{
		map[string]interface{}{"type": ctype, "status": status, "reason": reason},
	}, "status", "conditions")
	return u
}

func TestCheckConditions(t *testing.T) {
	scheme, err := manager.BuildScheme()
	if err != nil {
		t.Fatal(err)
	}
	obj := topicWithCond("default", "payments-orders", "Ready", "True", "Reconciled")
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(obj).Build()

	rep := CheckConditions(context.Background(), cl, "default", []ConditionExpect{
		{Kind: "KafkaTopic", Name: "payments-orders", Type: "Ready", Status: "True"},
	})
	if rep.Failed() {
		t.Errorf("expected pass:\n%s", rep.String())
	}
	// Package convention: Detail is empty on a passing check.
	if len(rep.Results) != 1 || rep.Results[0].Detail != "" {
		t.Errorf("passing condition should have empty Detail: %+v", rep.Results)
	}

	rep2 := CheckConditions(context.Background(), cl, "default", []ConditionExpect{
		{Kind: "KafkaTopic", Name: "payments-orders", Type: "Ready", Status: "False"},
	})
	if !rep2.Failed() {
		t.Errorf("wrong status should fail:\n%s", rep2.String())
	}

	rep3 := CheckConditions(context.Background(), cl, "default", []ConditionExpect{
		{Kind: "KafkaTopic", Name: "missing", Type: "Ready", Status: "True"},
	})
	if !rep3.Failed() || !strings.Contains(rep3.String(), "missing") {
		t.Errorf("missing object should fail and name it:\n%s", rep3.String())
	}

	// Reason mismatch fails when a reason is specified.
	rep4 := CheckConditions(context.Background(), cl, "default", []ConditionExpect{
		{Kind: "KafkaTopic", Name: "payments-orders", Type: "Ready", Status: "True", Reason: "Nope"},
	})
	if !rep4.Failed() {
		t.Errorf("reason mismatch should fail:\n%s", rep4.String())
	}

	// Absent condition type fails.
	rep5 := CheckConditions(context.Background(), cl, "default", []ConditionExpect{
		{Kind: "KafkaTopic", Name: "payments-orders", Type: "DoesNotExist", Status: "True"},
	})
	if !rep5.Failed() {
		t.Errorf("absent condition type should fail:\n%s", rep5.String())
	}
}
