// Package importer provides the read side of `import cluster`: a deterministic
// snapshot of live Kafka state used to generate manifests. Determinism matters
// because import output must be reproducible for golden/round-trip tests.
package importer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry/recordname"
)

// TopicSnapshot is a single topic as observed during import. Config holds ONLY
// explicitly-set configuration (ConfigEntry.Default==false); inherited/broker
// defaults are omitted so generated manifests stay minimal.
type TopicSnapshot struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Config            map[string]string
}

// TopicSchemas holds the value and/or key subject state matched to a topic via
// a recognized subject naming strategy. Either field may be nil when the
// corresponding subject is not registered.
//
// ValueStrategy and KeyStrategy record which naming strategy was used to
// attribute each slot to this topic: "TopicName" (subject is "<topic>-value" /
// "<topic>-key") or "TopicRecordName" (subject is "<topic>-<recordFullName>",
// verified by extracting the record name from the schema body). The strategies
// are recorded PER SLOT because a key-subject match must never overwrite the
// value slot's detected strategy (and vice versa): a topic can legitimately
// carry a TopicRecordName value subject alongside a TopicName key subject, and
// collapsing the two into one field silently corrupted the value attribution.
// The zero value "" behaves as "TopicName" for any consumer that predates
// these fields; the strategy of an empty (nil) slot is meaningless and stays "".
type TopicSchemas struct {
	Value         *schemaregistry.SubjectState
	Key           *schemaregistry.SubjectState
	ValueStrategy string // "TopicName" or "TopicRecordName" ("" behaves as "TopicName")
	KeyStrategy   string // "TopicName" or "TopicRecordName" ("" behaves as "TopicName")
}

// RecordNameSubject is a Schema Registry subject minted by the RecordName
// strategy (subject name == the schema's record full name). It cannot be
// attributed to an owning topic from cluster state (no topic in the name; the
// schema may be shared), so import reports it for manual attribution rather than
// writing it into a manifest (spec §24.1).
type RecordNameSubject struct {
	Subject    string `json:"subject"`
	RecordName string `json:"recordName"`
	SchemaType string `json:"schemaType"`
}

// Snapshot is the deterministic, reproducible view of a cluster used by import.
// Topics are sorted by Name and ACLs by a stable composite key.
type Snapshot struct {
	Topics []TopicSnapshot
	ACLs   []kafka.ACLState
	// Quotas holds the live client quotas observed at import time.
	// Sorted by a deterministic entity key for reproducibility.
	Quotas []kafka.QuotaState
	// ScramCredentials holds the live SCRAM credential identities observed at
	// import time (ListScramCredentials with no names = all users). Sorted by
	// (User, Mechanism) for reproducibility.
	ScramCredentials []kafka.ScramCredential

	// Schemas maps an imported topic name to its matched value/key subjects.
	// Subjects are matched via TopicName or TopicRecordName strategies.
	Schemas map[string]TopicSchemas
	// RecordNameSubjects holds subjects recognized as RecordName-strategy
	// subjects (subject name == the schema's record full name). These cannot be
	// attributed to an owning topic from cluster state alone, so they are
	// reported for manual attribution rather than written into a manifest
	// (spec §24.1). Sorted by Subject for determinism.
	RecordNameSubjects []RecordNameSubject
	// SchemaAmbiguities holds warning messages for cases where two or more
	// record-based subjects (TopicRecordName) resolved to the same topic's value
	// slot. The first subject in sorted order was attributed; the rest were
	// skipped and recorded here. Sorted for determinism.
	SchemaAmbiguities []string
	// UnmatchedSubjects lists subjects that do not map to any imported topic via
	// any recognized strategy. Sorted for determinism.
	UnmatchedSubjects []string
	// RoleBindings holds the live MDS role bindings observed across the
	// cluster's configured scopes. Empty when no MDS client is supplied.
	// Sorted by mds.RoleBinding.Key() for determinism (spec §40).
	RoleBindings []mds.RoleBinding
}

// SnapshotOptions carries flags that skip optional, potentially-expensive
// collection steps of ReadSnapshot (a trailing variadic keeps every existing
// 5-arg call site compiling unchanged).
type SnapshotOptions struct {
	// SkipUsers skips the ListScramCredentials call entirely, mirroring a nil
	// srClient/mdsClient skipping schema/role-binding collection.
	SkipUsers bool

	// SkipQuotas skips the ListQuotas (DescribeClientQuotas) call entirely,
	// mirroring SkipUsers. Confluent Cloud rejects quota describes outright
	// (CLUSTER_AUTHORIZATION_FAILED), so without this an import against Cloud
	// fails before reading anything else; it also serves clusters whose quotas
	// are externally managed.
	SkipQuotas bool

	// IncludeInternal disables the default internal-topic filter (isInternalTopic):
	// with it set, Kafka/Confluent housekeeping topics (__* consumer-offsets/
	// transaction-state/connect-*, plus Confluent's _schemas and _confluent*
	// topics) are imported as manifests like any other topic. Default false —
	// these topics are broker/platform-managed, not something a GitOps manifest
	// should claim ownership of.
	IncludeInternal bool
}

// isInternalTopic reports whether name is Kafka or Confluent-platform
// housekeeping rather than an application topic:
//   - "__*"        - Kafka's own convention (__consumer_offsets,
//     __transaction_state, Connect's __connect-* topics, etc.)
//   - "_schemas"    - the Confluent Schema Registry's backing topic
//   - "_confluent*" - Confluent Platform housekeeping (_confluent-monitoring,
//     _confluent-command, _confluent-telemetry-metrics, ksqlDB's
//     _confluent-ksql-* topics, etc.)
//
// Import excludes these by default (SnapshotOptions.IncludeInternal opts in);
// generating a manifest for platform-managed topics would make monedula-gitops
// contend with the platform itself over topics it does not actually own.
func isInternalTopic(name string) bool {
	return strings.HasPrefix(name, "__") ||
		name == "_schemas" ||
		strings.HasPrefix(name, "_confluent")
}

// ReadSnapshot reads live cluster state through kClient and returns a
// deterministic Snapshot. Internal topics (see isInternalTopic) are excluded
// by default (opts.IncludeInternal overrides), and only explicitly-set topic
// configs are retained.
//
// When srClient is non-nil, schemas are gathered per §24.1:
//   - TopicName subjects ("<topic>-value" / "<topic>-key") and TopicRecordName
//     subjects ("<topic>-<recordFullName>", body-verified) are matched into
//     Snapshot.Schemas with the per-slot TopicSchemas.ValueStrategy /
//     TopicSchemas.KeyStrategy set accordingly.
//   - RecordName subjects (subject name == schema record full name) are
//     collected in Snapshot.RecordNameSubjects for manual attribution.
//   - Record-based subjects that would fill an already-attributed value slot
//     are recorded in Snapshot.SchemaAmbiguities.
//   - Everything else lands in Snapshot.UnmatchedSubjects.
//
// A nil srClient skips schema collection entirely.
//
// When mdsClient is non-nil, role bindings are read across mdsScopes,
// deduplicated by Key, and sorted by Key for determinism (spec §40). A nil
// mdsClient (or empty mdsScopes) skips role-binding collection entirely.
//
// SCRAM credentials are read via ListScramCredentials (no names = all users)
// unless opts requests SkipUsers, mirroring the srClient-nil / mdsClient-nil
// skip pattern for schemas/role-bindings (--skip-users avoids the extra admin
// call entirely for clusters where users are application-managed). Client
// quotas are read via ListQuotas unless opts requests SkipQuotas (--skip-quotas
// — Confluent Cloud rejects quota describes, and externally-managed quotas
// need no reconstruction).
func ReadSnapshot(ctx context.Context, kClient kafka.AdminClient, srClient schemaregistry.Client,
	mdsClient mds.Client, mdsScopes []mds.Scope, opts ...SnapshotOptions) (Snapshot, error) {
	var opt SnapshotOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	topics, err := kClient.ListTopics(ctx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("list topics: %w", err)
	}

	out := make([]TopicSnapshot, 0, len(topics))
	for _, t := range topics {
		if !opt.IncludeInternal && isInternalTopic(t.Name) {
			continue // exclude Kafka/Confluent housekeeping topics by default
		}

		entries, err := kClient.DescribeTopicConfigs(ctx, t.Name)
		if err != nil {
			return Snapshot{}, fmt.Errorf("describe configs for topic %q: %w", t.Name, err)
		}

		cfg := make(map[string]string)
		for _, e := range entries {
			if e.Default {
				continue // keep only explicitly-set configs
			}
			cfg[e.Name] = e.Value
		}

		out = append(out, TopicSnapshot{
			Name:              t.Name,
			Partitions:        t.Partitions,
			ReplicationFactor: t.ReplicationFactor,
			Config:            cfg,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})

	acls, err := kClient.ListACLs(ctx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("list acls: %w", err)
	}
	sort.Slice(acls, func(i, j int) bool {
		return aclKey(acls[i]) < aclKey(acls[j])
	})

	var quotas []kafka.QuotaState
	if !opt.SkipQuotas {
		quotas, err = kClient.ListQuotas(ctx)
		if err != nil {
			return Snapshot{}, fmt.Errorf("list quotas: %w", err)
		}
		sort.Slice(quotas, func(i, j int) bool {
			return quotaEntityKey(quotas[i]) < quotaEntityKey(quotas[j])
		})
	}

	var creds []kafka.ScramCredential
	if !opt.SkipUsers {
		creds, err = kClient.ListScramCredentials(ctx)
		if err != nil {
			return Snapshot{}, fmt.Errorf("list scram credentials: %w", err)
		}
		sort.Slice(creds, func(i, j int) bool {
			if creds[i].User != creds[j].User {
				return creds[i].User < creds[j].User
			}
			return creds[i].Mechanism < creds[j].Mechanism
		})
	}

	snap := Snapshot{Topics: out, ACLs: acls, Quotas: quotas, ScramCredentials: creds}

	if mdsClient != nil {
		var rbs []mds.RoleBinding
		seen := map[string]struct{}{}
		for _, scope := range mdsScopes {
			listed, err := mdsClient.ListRoleBindings(ctx, scope)
			if err != nil {
				return Snapshot{}, fmt.Errorf("list role bindings (scope %q): %w", scope.Type+"/"+scope.SubCluster, err)
			}
			for _, rb := range listed {
				k := rb.Key()
				if _, ok := seen[k]; !ok {
					seen[k] = struct{}{}
					rbs = append(rbs, rb)
				}
			}
		}
		sort.Slice(rbs, func(i, j int) bool { return rbs[i].Key() < rbs[j].Key() })
		snap.RoleBindings = rbs
	}

	if srClient != nil {
		sr, err := gatherSchemas(ctx, srClient, out)
		if err != nil {
			return Snapshot{}, err
		}
		snap.Schemas = sr.Schemas
		snap.RecordNameSubjects = sr.RecordName
		snap.SchemaAmbiguities = sr.Ambiguities
		snap.UnmatchedSubjects = sr.Unmatched
	}

	return snap, nil
}

// schemaMatch is the structured result of gatherSchemas, replacing the
// previous (map, []string, error) triple.
type schemaMatch struct {
	Schemas     map[string]TopicSchemas
	RecordName  []RecordNameSubject
	Unmatched   []string
	Ambiguities []string
}

// gatherSchemas classifies every Schema Registry subject against the imported
// topic list using a four-level strategy cascade (evaluated in order per
// subject; first match wins):
//
//  1. TopicName: subject ∈ {"<topic>-value", "<topic>-key"} for an imported
//     topic → assign Value/Key and set that slot's strategy to "TopicName".
//     Exactly matches the legacy behavior so that golden tests remain stable.
//  2. GetSubject; extract the record full name from the schema body.
//     If extraction fails (primitive/unnamed schema), the subject falls through
//     to Unmatched (not an error — just not a named record).
//  3. TopicRecordName: subject == "<T>-<recordName>" for some imported topic T
//     (body-verified). If the topic's Value slot is empty, fill it and set
//     ValueStrategy "TopicRecordName"; otherwise record an ambiguity warning.
//  4. RecordName: subject == recordName (body-verified). Append to RecordName
//     for report-only attribution (spec §24.1).
//  5. Else: Unmatched.
//
// Results (Unmatched, RecordName, Ambiguities) are sorted for determinism.
// TopicName key/value behavior is byte-identical to the previous implementation.
func gatherSchemas(ctx context.Context, srClient schemaregistry.Client, topics []TopicSnapshot) (schemaMatch, error) {
	subjects, err := srClient.ListSubjects(ctx)
	if err != nil {
		return schemaMatch{}, fmt.Errorf("list subjects: %w", err)
	}

	// Build the TopicName expected-subject index for O(1) lookup.
	type topicNameTarget struct {
		topic   string
		isValue bool
	}
	expectedTopicName := make(map[string]topicNameTarget, len(topics)*2)
	for _, t := range topics {
		expectedTopicName[t.Name+"-value"] = topicNameTarget{topic: t.Name, isValue: true}
		expectedTopicName[t.Name+"-key"] = topicNameTarget{topic: t.Name, isValue: false}
	}

	result := schemaMatch{
		Schemas: map[string]TopicSchemas{},
	}

	sort.Strings(subjects)
	for _, subject := range subjects {
		// ── Step 1: TopicName ──────────────────────────────────────────────
		if tgt, ok := expectedTopicName[subject]; ok {
			state, err := srClient.GetSubject(ctx, subject)
			if err != nil {
				return schemaMatch{}, fmt.Errorf("get subject %q: %w", subject, err)
			}
			if state == nil {
				continue // subject vanished between list and get
			}
			level, err := srClient.GetCompatibility(ctx, subject)
			if err != nil {
				return schemaMatch{}, fmt.Errorf("get compatibility %q: %w", subject, err)
			}
			state.Compatibility = level

			ts := result.Schemas[tgt.topic]
			// The strategy is recorded per slot: a key match must not touch the
			// value slot's detected strategy (and vice versa) — a single shared
			// field previously let "<topic>-key" flip an already-attributed
			// TopicRecordName value slot to TopicName, generating a manifest
			// whose recomputed value subject did not exist live.
			if tgt.isValue {
				// If a TopicRecordName subject already claimed this value slot,
				// the TopicName subject takes precedence but the displaced
				// record-based subject must be surfaced rather than silently
				// dropped.
				if ts.Value != nil && ts.ValueStrategy == "TopicRecordName" {
					result.Ambiguities = append(result.Ambiguities,
						fmt.Sprintf("topic %q: TopicName subject %q overrides record-based subject %q already attributed to the value slot; the record-based subject is dropped",
							tgt.topic, subject, ts.Value.Subject))
				}
				ts.Value = state
				ts.ValueStrategy = "TopicName"
			} else {
				ts.Key = state
				ts.KeyStrategy = "TopicName"
			}
			result.Schemas[tgt.topic] = ts
			continue
		}

		// ── Step 2: fetch subject state + extract record name ───────────────
		state, err := srClient.GetSubject(ctx, subject)
		if err != nil {
			return schemaMatch{}, fmt.Errorf("get subject %q: %w", subject, err)
		}
		if state == nil {
			continue // subject vanished between list and get
		}

		rn, rnErr := recordname.Extract(string(state.Schema.Type), state.Schema.Definition)
		if rnErr != nil {
			// Primitive/unnamed schema — cannot classify further.
			result.Unmatched = append(result.Unmatched, subject)
			continue
		}

		// ── Step 3: TopicRecordName ─────────────────────────────────────────
		// subject must equal "<topic>-<recordFullName>" for some known topic.
		// The body-verification (rn extracted from the schema) makes this exact.
		matched := false
		for _, t := range topics {
			if subject == t.Name+"-"+rn {
				// Body-verified TopicRecordName match.
				level, err := srClient.GetCompatibility(ctx, subject)
				if err != nil {
					return schemaMatch{}, fmt.Errorf("get compatibility %q: %w", subject, err)
				}
				state.Compatibility = level

				ts := result.Schemas[t.Name]
				if ts.Value == nil {
					ts.Value = state
					ts.ValueStrategy = "TopicRecordName"
					result.Schemas[t.Name] = ts
				} else {
					result.Ambiguities = append(result.Ambiguities,
						fmt.Sprintf("topic %q: subject %q also reconstructs to a record-based value; already attributed to %q, skipped this one", t.Name, subject, ts.Value.Subject))
				}
				matched = true
				break // subject can match at most one topic (body-verified equality)
			}
		}
		if matched {
			continue
		}

		// ── Step 4: RecordName ──────────────────────────────────────────────
		if subject == rn {
			result.RecordName = append(result.RecordName, RecordNameSubject{
				Subject:    subject,
				RecordName: rn,
				SchemaType: string(state.Schema.Type),
			})
			continue
		}

		// ── Step 5: generic unmatched ───────────────────────────────────────
		result.Unmatched = append(result.Unmatched, subject)
	}

	sort.Slice(result.RecordName, func(i, j int) bool {
		return result.RecordName[i].Subject < result.RecordName[j].Subject
	})
	sort.Strings(result.Unmatched)
	sort.Strings(result.Ambiguities)

	return result, nil
}

// quotaEntityKey builds a stable sort key for a QuotaState by joining the
// entity components sorted by type then name. NUL separators prevent aliasing.
func quotaEntityKey(q kafka.QuotaState) string {
	parts := make([]string, 0, len(q.Entity))
	for _, c := range q.Entity {
		n := ""
		if c.Name != nil {
			n = *c.Name
		}
		parts = append(parts, c.Type+"\x00"+n)
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x01")
}

// aclKey builds a stable composite sort key over all ACL fields. NUL separators
// keep field boundaries unambiguous so concatenation can't alias distinct ACLs.
func aclKey(a kafka.ACLState) string {
	return strings.Join([]string{
		a.Principal,
		a.ResourceType,
		a.ResourceName,
		a.PatternType,
		a.Operation,
		a.Host,
		a.Permission,
	}, "\x00")
}

// ScopesFromMDSConfig returns the distinct MDS scopes to enumerate for import:
// always kafka, plus schema-registry / connect / ksql when their cluster IDs are
// configured. Returns nil for a nil config (no RBAC import). Deterministic order.
func ScopesFromMDSConfig(cfg *v1alpha1.MDSConfig) []mds.Scope {
	if cfg == nil {
		return nil
	}
	scopes := []mds.Scope{{Type: "kafka", KafkaCluster: cfg.Clusters.KafkaCluster}}
	if cfg.Clusters.SchemaRegistryCluster != "" {
		scopes = append(scopes, mds.Scope{Type: "schema-registry", KafkaCluster: cfg.Clusters.KafkaCluster, SubCluster: cfg.Clusters.SchemaRegistryCluster})
	}
	if cfg.Clusters.ConnectCluster != "" {
		scopes = append(scopes, mds.Scope{Type: "connect", KafkaCluster: cfg.Clusters.KafkaCluster, SubCluster: cfg.Clusters.ConnectCluster})
	}
	if cfg.Clusters.KsqlCluster != "" {
		scopes = append(scopes, mds.Scope{Type: "ksql", KafkaCluster: cfg.Clusters.KafkaCluster, SubCluster: cfg.Clusters.KsqlCluster})
	}
	return scopes
}
