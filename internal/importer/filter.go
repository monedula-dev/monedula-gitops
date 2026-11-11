package importer

import (
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
)

// TopicFilter selects the part of a cluster that import reconstructs
// (`import --topic` / `--exclude-topic`). Patterns use path.Match syntax, the
// same glob dialect as KafkaCluster.spec.tenancy; since topic names contain no
// "/", "*" matches any run of characters, dots included.
//
// A topic is selected when it matches at least one Include pattern (or Include
// is empty) and no Exclude pattern. The zero value selects every topic and
// leaves import unchanged.
type TopicFilter struct {
	Include []string
	Exclude []string
}

// Active reports whether the filter narrows the import at all.
func (f TopicFilter) Active() bool {
	return len(f.Include) > 0 || len(f.Exclude) > 0
}

// Validate rejects empty and malformed patterns, so a typo fails the import up
// front instead of silently selecting nothing.
func (f TopicFilter) Validate() error {
	check := func(flag string, patterns []string) error {
		for _, p := range patterns {
			if p == "" {
				return fmt.Errorf("%s: empty pattern", flag)
			}
			if _, err := path.Match(p, ""); errors.Is(err, path.ErrBadPattern) {
				return fmt.Errorf("%s: malformed pattern %q", flag, p)
			}
		}
		return nil
	}
	if err := check("--topic", f.Include); err != nil {
		return err
	}
	return check("--exclude-topic", f.Exclude)
}

// Match reports whether the topic name is selected. Patterns are assumed
// valid (see Validate); a malformed one matches nothing.
func (f TopicFilter) Match(name string) bool {
	if len(f.Include) > 0 && !matchAny(f.Include, name) {
		return false
	}
	return !matchAny(f.Exclude, name)
}

func matchAny(patterns []string, name string) bool {
	for _, p := range patterns {
		if ok, _ := path.Match(p, name); ok {
			return true
		}
	}
	return false
}

// FilterStats counts what an active TopicFilter left out of the snapshot.
// Internal topics skipped by their own rule (isInternalTopic) are not counted.
type FilterStats struct {
	Topics       int `json:"topics"`
	ACLs         int `json:"acls"`
	RoleBindings int `json:"roleBindings"`
	Subjects     int `json:"subjects"`
}

// filterACLs keeps the ACLs that belong to the selected part of the cluster:
//
//   - a TOPIC ACL is kept when its resource name, read as a topic name,
//     matches the filter. For a PREFIXED ACL this means the prefix itself must
//     be selected ("payments." under --topic 'payments.*'); a shorter prefix
//     that also covers other teams' topics ("pay") is left out.
//   - any other ACL (GROUP, CLUSTER, TRANSACTIONAL_ID, ...) cannot be tied to
//     a topic by name, so it is kept only when its principal also holds a kept
//     TOPIC ACL: the consumer-group ACLs of the selected topics' clients come
//     along, while unrelated principals' ACLs do not.
func filterACLs(acls []kafka.ACLState, f TopicFilter) (kept []kafka.ACLState, dropped int) {
	principals := map[string]bool{}
	for _, a := range acls {
		if strings.EqualFold(a.ResourceType, "TOPIC") && f.Match(a.ResourceName) {
			principals[a.Principal] = true
		}
	}
	for _, a := range acls {
		var keep bool
		if strings.EqualFold(a.ResourceType, "TOPIC") {
			keep = f.Match(a.ResourceName)
		} else {
			keep = principals[a.Principal]
		}
		if keep {
			kept = append(kept, a)
		} else {
			dropped++
		}
	}
	return kept, dropped
}

// filterRoleBindings applies the filterACLs rule to MDS role bindings: a
// binding on a Topic resource is kept when the resource name matches; any
// other binding (Group, cluster-scoped, ...) is kept only when its principal
// holds a kept Topic binding.
func filterRoleBindings(rbs []mds.RoleBinding, f TopicFilter) (kept []mds.RoleBinding, dropped int) {
	isTopic := func(rb mds.RoleBinding) bool {
		return rb.Resource != nil && strings.EqualFold(rb.Resource.Type, "Topic")
	}
	principals := map[string]bool{}
	for _, rb := range rbs {
		if isTopic(rb) && f.Match(rb.Resource.Name) {
			principals[rb.Principal] = true
		}
	}
	for _, rb := range rbs {
		var keep bool
		if isTopic(rb) {
			keep = f.Match(rb.Resource.Name)
		} else {
			keep = principals[rb.Principal]
		}
		if keep {
			kept = append(kept, rb)
		} else {
			dropped++
		}
	}
	return kept, dropped
}

// subjectOwner attributes a Schema Registry subject to the live topic whose
// name is the longest "<topic>-" prefix of the subject (covering both
// TopicName "<topic>-value"/"-key" and TopicRecordName "<topic>-<record>"
// subjects). It returns "" when no live topic prefixes the subject. Longest
// match keeps "orders-eu-value" with topic "orders-eu" even when a topic
// "orders" also exists.
func subjectOwner(subject string, topics map[string]bool) string {
	for i := len(subject) - 1; i > 0; i-- {
		if subject[i] == '-' && topics[subject[:i]] {
			return subject[:i]
		}
	}
	return ""
}
