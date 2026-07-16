// Package e2e is the scenario assertion engine for the scenarios/ suite. It
// parses a scenario's declarative expect.yaml and checks it against four
// observable surfaces — CLI exit code/output, k8s resource conditions, k8s
// admission rejections, and live Kafka/Schema-Registry state — so that the
// same declared contract drives both the human-readable README and the
// machine-checked e2e test. It is consumed by the hidden `monedula-gitops e2e
// check` subcommand (internal/cli/e2e.go).
package e2e

import (
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

// Expect is the full contract a scenario declares (expect.yaml). Each section
// is optional; the checker asserts only the sections relevant to the active
// mode. CLI/K8s carry mode-specific surfaces; LiveState is probed in either
// mode against the real cluster.
type Expect struct {
	CLI       *CLIExpect `json:"cli,omitempty"`
	K8s       *K8sExpect `json:"k8s,omitempty"`
	LiveState LiveState  `json:"liveState,omitempty"`
	Steps     []Step     `json:"steps,omitempty"`
}

// CLIExpect groups the CLI command invocations a scenario asserts on. Each key
// is a documented command (apply/validate/diff/verify); nil means "not run".
type CLIExpect struct {
	Apply    *CommandExpect `json:"apply,omitempty"`
	Validate *CommandExpect `json:"validate,omitempty"`
	Diff     *CommandExpect `json:"diff,omitempty"`
	Verify   *CommandExpect `json:"verify,omitempty"`
}

// CommandExpect is the assertion for one CLI command run: its exit code and/or
// substrings/regexes its combined output must contain. ExitCode is a pointer so
// "0 expected" is distinguishable from "unset".
type CommandExpect struct {
	ExitCode       *int     `json:"exitCode,omitempty"`
	OutputContains []string `json:"outputContains,omitempty"`
	OutputMatches  []string `json:"outputMatches,omitempty"`
}

// K8sExpect groups the k8s-mode surfaces: resource conditions and (for "bad"
// scenarios) an admission-webhook rejection.
type K8sExpect struct {
	Conditions []ConditionExpect `json:"conditions,omitempty"`
	Admission  *AdmissionExpect  `json:"admission,omitempty"`
}

// ConditionExpect asserts one status condition on a named CR.
type ConditionExpect struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// AdmissionExpect asserts that `kubectl apply` of the scenario manifests was
// rejected by an admission webhook, optionally matching the rejection message.
type AdmissionExpect struct {
	Rejected       bool   `json:"rejected"`
	MessageMatches string `json:"messageMatches,omitempty"`
}

// LiveState probes the real cluster. v0.20 adds acls/quotas/subjects to the
// original topics surface. v0.23 adds absent for deletion assertions. v0.35
// adds users (SCRAM credential identities) and absent.users. Each section is
// subset-matched: only the entries you list are asserted; extra live state is
// ignored.
type LiveState struct {
	Topics   []TopicExpect   `json:"topics,omitempty"`
	ACLs     []ACLExpect     `json:"acls,omitempty"`
	Quotas   []QuotaExpect   `json:"quotas,omitempty"`
	Subjects []SubjectExpect `json:"subjects,omitempty"`
	Users    []UserExpect    `json:"users,omitempty"`
	Absent   *AbsentState    `json:"absent,omitempty"`
}

// AbsentState lists live resources that must NOT exist on the broker. Used by
// deletion scenarios to prove a Delete-policy topic (or, since v0.35, a
// deleted KafkaUser's credential) was actually removed. Topics are matched by
// name; users are matched by (username, mechanism) since a principal may hold
// more than one mechanism's credential.
type AbsentState struct {
	Topics []string          `json:"topics,omitempty"`
	Users  []UserAbsentEntry `json:"users,omitempty"`
}

// UserExpect asserts a SCRAM credential exists for (Username, Mechanism).
// Iterations is checked only when non-zero (0 means "don't care" — the same
// convention as a nil KafkaUser.Spec.Iterations meaning "broker default").
type UserExpect struct {
	Username   string `json:"username"`
	Mechanism  string `json:"mechanism"`
	Iterations int32  `json:"iterations,omitempty"`
}

// UserAbsentEntry identifies a (username, mechanism) credential that must NOT
// exist on the broker.
type UserAbsentEntry struct {
	Username  string `json:"username"`
	Mechanism string `json:"mechanism"`
}

// TopicExpect asserts a topic exists and (optionally) carries the given config
// subset (only the listed keys are checked; others are ignored).
type TopicExpect struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config,omitempty"`
}

// ACLExpect asserts one ACL tuple exists on the broker. PatternType defaults to
// "literal", Permission to "Allow", and Host to "*" when empty (applied at
// match time so authored YAML stays terse).
type ACLExpect struct {
	Principal    string `json:"principal"`
	Operation    string `json:"operation"`
	ResourceType string `json:"resourceType"`
	ResourceName string `json:"resourceName"`
	PatternType  string `json:"patternType,omitempty"`
	Permission   string `json:"permission,omitempty"`
	Host         string `json:"host,omitempty"`
}

// QuotaExpect asserts a quota entity carries the given limit subset. Entity sets
// exactly one of user/clientId/ip. Limit keys are the Kafka wire keys (e.g.
// producer_byte_rate, connection_creation_rate).
type QuotaExpect struct {
	Entity QuotaEntityExpect  `json:"entity"`
	Limits map[string]float64 `json:"limits"`
}

// QuotaEntityExpect identifies a quota target by exactly one of user/clientId/ip.
// Default-entity assertions (the Kafka null-name entity) are not supported yet —
// set one of the three named fields.
type QuotaEntityExpect struct {
	User     string `json:"user,omitempty"`
	ClientID string `json:"clientId,omitempty"`
	IP       string `json:"ip,omitempty"`
}

// SubjectExpect asserts a Schema Registry subject exists and (when Compatibility
// is set) carries that subject-level compatibility level.
type SubjectExpect struct {
	Name          string `json:"name"`
	Compatibility string `json:"compatibility,omitempty"`
}

// Step is one action in a scenario's ordered steps sequence (expect.yaml
// `steps:`). When a scenario declares steps, the CLI runner executes them in
// order instead of the single-command flow. Run is apply|verify|diff|mutate|import|doctor
// (import + the "@imported" manifests sentinel, and doctor, are CLI-runner-only conventions).
type Step struct {
	Run       string         `json:"run"`
	Manifests string         `json:"manifests,omitempty"` // subdir override (default the scenario's manifests/)
	Flags     []string       `json:"flags,omitempty"`     // extra CLI flags, e.g. ["--prune"]
	Expect    *CommandExpect `json:"expect,omitempty"`    // exit/output assertion for apply|verify|diff|import|doctor
	Mutate    *MutateSpec    `json:"mutate,omitempty"`    // out-of-band change when Run == "mutate"
}

// MutateSpec is the out-of-band broker change a mutate step applies. v1 supports
// topic-config set; ACL mutation is a future extension.
type MutateSpec struct {
	TopicConfig *TopicConfigMutation `json:"topicConfig,omitempty"`
}

// TopicConfigMutation sets config keys on a topic directly on the broker
// (simulating drift), applied via AdminClient.UpdateTopicConfig.
type TopicConfigMutation struct {
	Topic string            `json:"topic"`
	Set   map[string]string `json:"set"`
}

// LoadExpect reads and parses a scenario's expect.yaml.
func LoadExpect(path string) (*Expect, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading expect file: %w", err)
	}
	var e Expect
	if err := yaml.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	return &e, nil
}
