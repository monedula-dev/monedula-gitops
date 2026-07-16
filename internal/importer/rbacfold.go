package importer

import (
	"sort"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
)

// foldRoleBindings folds unambiguous producer/consumer role bindings into topic
// access (spec §40 import), appending only entries not already present (so it
// coexists with the ACL fold on dual clusters). It returns the leftover role
// bindings that must be emitted as explicit KafkaRoleBinding: cluster-scoped
// roles, ResourceOwner, non-kafka scopes, prefixed patterns, custom/unknown
// roles, producers on non-imported topics, and ambiguous/unmatched consumers.
//
// It only mutates topic.Spec.Access. Build's two-sided recompile-verify is the
// safety net that guarantees the result round-trips on every active backend; a
// fold that the cluster cannot faithfully dual-emit is caught there and triggers
// the all-explicit fallback.
func foldRoleBindings(rbs []mds.RoleBinding, topicByName map[string]*v1alpha1.KafkaTopic) []mds.RoleBinding {
	var leftover []mds.RoleBinding

	type perPrincipal struct {
		producerTopics []string
		topicReads     []string
		groupReads     []string
	}
	byP := map[string]*perPrincipal{}
	var principals []string
	getP := func(p string) *perPrincipal {
		if _, ok := byP[p]; !ok {
			byP[p] = &perPrincipal{}
			principals = append(principals, p)
		}
		return byP[p]
	}

	for _, rb := range rbs {
		if rb.Scope.Type != "kafka" || rb.Resource == nil || rb.Resource.PatternType != "literal" {
			leftover = append(leftover, rb)
			continue
		}
		switch {
		case rb.Role == "DeveloperWrite" && rb.Resource.Type == "Topic":
			if _, ok := topicByName[rb.Resource.Name]; ok {
				pp := getP(rb.Principal)
				pp.producerTopics = append(pp.producerTopics, rb.Resource.Name)
			} else {
				leftover = append(leftover, rb)
			}
		case rb.Role == "DeveloperRead" && rb.Resource.Type == "Topic":
			if _, ok := topicByName[rb.Resource.Name]; ok {
				pp := getP(rb.Principal)
				pp.topicReads = append(pp.topicReads, rb.Resource.Name)
			} else {
				leftover = append(leftover, rb)
			}
		case rb.Role == "DeveloperRead" && rb.Resource.Type == "Group":
			getP(rb.Principal).groupReads = append(getP(rb.Principal).groupReads, rb.Resource.Name)
		default:
			leftover = append(leftover, rb)
		}
	}
	sort.Strings(principals)

	for _, p := range principals {
		pp := byP[p]
		sort.Strings(pp.producerTopics)
		sort.Strings(pp.topicReads)
		sort.Strings(pp.groupReads)

		for _, tName := range pp.producerTopics {
			addProducer(topicByName[tName], p)
		}

		switch {
		case len(pp.topicReads) == 1 && len(pp.groupReads) == 1:
			addConsumer(topicByName[pp.topicReads[0]], p, pp.groupReads[0])
		default:
			for _, tName := range pp.topicReads {
				leftover = append(leftover, topicRBOf(p, "DeveloperRead", tName))
			}
			for _, g := range pp.groupReads {
				leftover = append(leftover, groupRBOf(p, g))
			}
		}
	}

	sort.Slice(leftover, func(i, j int) bool { return leftover[i].Key() < leftover[j].Key() })
	return leftover
}

func addProducer(tp *v1alpha1.KafkaTopic, principal string) {
	for _, e := range tp.Spec.Access.Producers {
		if e.Principal == principal && e.Host == "" {
			return
		}
	}
	tp.Spec.Access.Producers = append(tp.Spec.Access.Producers, v1alpha1.ProducerAccess{Principal: principal})
}

func addConsumer(tp *v1alpha1.KafkaTopic, principal, group string) {
	for _, e := range tp.Spec.Access.Consumers {
		if e.Principal == principal && e.Group == group && e.Host == "" {
			return
		}
	}
	tp.Spec.Access.Consumers = append(tp.Spec.Access.Consumers, v1alpha1.ConsumerAccess{Principal: principal, Group: group})
}

// topicRBOf and groupRBOf rebuild a kafka-scoped literal mds.RoleBinding for the
// leftover list. The concrete KafkaCluster (and SubCluster) scope IDs are
// intentionally dropped — roleBindingManifest reads only Scope.Type, so emission
// is unaffected. Build's RBAC verify must NOT compare leftover keys directly to
// live keys; it must recompile the generated manifests through rbac.Compile
// (which re-injects scope IDs from mdsCfg) and compare those to live FullKeys.
func topicRBOf(principal, role, topic string) mds.RoleBinding {
	return mds.RoleBinding{Principal: principal, Role: role, Scope: mds.Scope{Type: "kafka"},
		Resource: &mds.ResourcePattern{Type: "Topic", Name: topic, PatternType: "literal"}}
}
func groupRBOf(principal, group string) mds.RoleBinding {
	return mds.RoleBinding{Principal: principal, Role: "DeveloperRead", Scope: mds.Scope{Type: "kafka"},
		Resource: &mds.ResourcePattern{Type: "Group", Name: group, PatternType: "literal"}}
}
