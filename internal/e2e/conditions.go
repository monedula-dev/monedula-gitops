package e2e

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// apiGroup/apiVersion for the project's CRDs.
const (
	apiGroup   = "gitops.monedula.dev"
	apiVersion = "v1alpha1"
)

// schemaGVK returns the GroupVersionKind for a project CR kind.
func schemaGVK(kind string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: apiGroup, Version: apiVersion, Kind: kind}
}

// CheckConditions asserts each ConditionExpect against the live cluster: it Gets
// the named CR (in namespace ns) and verifies the named status condition has the
// expected status (and reason, if specified). A missing object or absent
// condition is a failure, not an error.
func CheckConditions(ctx context.Context, cl client.Client, ns string, conds []ConditionExpect) Report {
	var rep Report
	for _, ce := range conds {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(schemaGVK(ce.Kind))
		key := types.NamespacedName{Namespace: ns, Name: ce.Name}
		if err := cl.Get(ctx, key, u); err != nil {
			rep.Add(CheckResult{Name: fmt.Sprintf("%s/%s condition %s", ce.Kind, ce.Name, ce.Type),
				Pass: false, Detail: fmt.Sprintf("get %s/%s: %v", ce.Kind, ce.Name, err)})
			continue
		}
		conditions, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
		found := false
		for _, raw := range conditions {
			m, ok := raw.(map[string]interface{})
			if !ok || m["type"] != ce.Type {
				continue
			}
			found = true
			gotStatus, _ := m["status"].(string)
			gotReason, _ := m["reason"].(string)
			pass := gotStatus == ce.Status && (ce.Reason == "" || gotReason == ce.Reason)
			detail := ""
			if !pass {
				detail = fmt.Sprintf("expected status=%s", ce.Status)
				if ce.Reason != "" {
					detail += fmt.Sprintf(" reason=%s", ce.Reason)
				}
				detail += fmt.Sprintf("; got status=%s reason=%s", gotStatus, gotReason)
			}
			rep.Add(CheckResult{Name: fmt.Sprintf("%s/%s condition %s", ce.Kind, ce.Name, ce.Type),
				Pass: pass, Detail: detail})
			break
		}
		if !found {
			rep.Add(CheckResult{Name: fmt.Sprintf("%s/%s condition %s", ce.Kind, ce.Name, ce.Type),
				Pass: false, Detail: fmt.Sprintf("condition %q not present", ce.Type)})
		}
	}
	return rep
}
