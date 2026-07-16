package importer_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apivalidation "k8s.io/apimachinery/pkg/util/validation"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/importer"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/loader"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry/recordname"
	"github.com/monedula-dev/monedula-gitops/internal/user"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// ptr is a generic helper to take the address of a value.
func ptr[T any](v T) *T { return &v }

// TestImportedManifestsPassValidate is the round-trip property "generated
// manifests pass validate": a mixed snapshot (placement-constrained topic,
// schema subjects, foldable and raw ACLs) is built, namespaced, written to a
// directory, loaded back with the real loader, and run through
// validation.Validate against the source cluster. Any validation error means
// import produced manifests the tool itself rejects (review I14 was exactly
// such a bug: replicationFactor emitted alongside placement constraints).
func TestImportedManifestsPassValidate(t *testing.T) {
	const clusterName = "prod"

	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{
			// Placement-constrained topic: RF must be omitted (I14).
			{Name: "payments.placed", Partitions: 6, ReplicationFactor: 3, Config: map[string]string{
				"confluent.placement.constraints": `{"version":2,"replicas":[{"count":3}]}`,
				"min.insync.replicas":             "2",
			}},
			// Plain topic with RF and a schema subject.
			{Name: "payments.orders", Partitions: 3, ReplicationFactor: 3, Config: map[string]string{
				"retention.ms": "604800000",
			}},
			// Topic with no explicit RF.
			{Name: "logs.app", Partitions: 1, ReplicationFactor: 0},
		},
		Quotas: []kafka.QuotaState{
			// Named user + named client-id with producer limit.
			{
				Entity: []kafka.QuotaEntityComponent{
					{Type: "user", Name: ptr("svc-checkout")},
					{Type: "client-id", Name: ptr("batch")},
				},
				Limits: map[string]float64{"producer_byte_rate": 1048576},
			},
			// User-default entity with consumer limit.
			{
				Entity: []kafka.QuotaEntityComponent{
					{Type: "user", Name: nil},
				},
				Limits: map[string]float64{"consumer_byte_rate": 524288},
			},
		},
		ACLs: []kafka.ACLState{
			// Foldable producer on payments.orders.
			acl("User:svc-checkout", "topic", "payments.orders", "Write"),
			acl("User:svc-checkout", "topic", "payments.orders", "Describe"),
			// Foldable consumer with a single group.
			acl("User:svc-billing", "topic", "payments.orders", "Read"),
			acl("User:svc-billing", "topic", "payments.orders", "Describe"),
			acl("User:svc-billing", "group", "billing-cg", "Read"),
			// Raw: prefixed pattern, deny, cluster resource.
			acl("User:legacy", "topic", "logs.", "Write", withPattern("prefixed")),
			acl("User:banned", "topic", "payments.orders", "Read", withPerm("Deny")),
			acl("User:admin", "cluster", "kafka-cluster", "Alter"),
		},
		Schemas: map[string]importer.TopicSchemas{
			"payments.orders": {
				Value: &schemaregistry.SubjectState{
					Subject:       "payments.orders-value",
					Schema:        schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroBody},
					Compatibility: "BACKWARD",
				},
			},
		},
		ScramCredentials: []kafka.ScramCredential{
			{User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
		},
	}

	res := importer.Build(snap, clusterName, nil, nil)
	if err := importer.AssignNamespaces(&res, importer.NamespaceStrategy{Kind: "prefix", Separator: "."}); err != nil {
		t.Fatalf("AssignNamespaces: %v", err)
	}

	dir := t.TempDir()
	if _, err := importer.WriteToDir(res, dir, "never"); err != nil {
		t.Fatalf("WriteToDir: %v", err)
	}

	// Load the written tree back with the real loader (recursive: files are
	// nested at <ns>/topics/<name>.yaml and <ns>/access/<name>.yaml).
	objs, err := loader.Load(loader.Options{Filenames: []string{dir}, Recursive: true})
	if err != nil {
		t.Fatalf("loader.Load: %v", err)
	}

	in := validation.Input{
		Clusters: map[string]*v1alpha1.KafkaCluster{
			clusterName: {
				TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaCluster"},
				ObjectMeta: metav1.ObjectMeta{Name: clusterName},
				Spec: v1alpha1.KafkaClusterSpec{
					BootstrapServers: "localhost:9092",
					SchemaRegistry:   &v1alpha1.SchemaRegistryConf{Endpoint: "http://sr"},
				},
			},
		},
	}
	for _, o := range objs {
		switch {
		case o.Topic != nil:
			in.Topics = append(in.Topics, o.Topic)
		case o.Policy != nil:
			in.Policies = append(in.Policies, o.Policy)
		case o.User != nil:
			in.Users = append(in.Users, o.User)
		}
	}
	if len(in.Topics) != 3 {
		t.Fatalf("want 3 topics loaded back, got %d", len(in.Topics))
	}
	if len(in.Policies) == 0 {
		t.Fatalf("want at least one policy loaded back, got none")
	}
	if len(in.Users) != 1 {
		t.Fatalf("want 1 user loaded back, got %d", len(in.Users))
	}

	if errs := validation.Validate(in); len(errs) != 0 {
		t.Fatalf("imported manifests must pass validate; got %d error(s):\n%v", len(errs), errs)
	}

	// Round-trip property for quotas: every reconstructed KafkaQuota must pass
	// ValidateQuotaShape (spec §39.6 — the importer must not emit invalid quotas).
	if len(res.Quotas) != 2 {
		t.Fatalf("want 2 reconstructed quotas (one named-entity, one user-default), got %d", len(res.Quotas))
	}
	for _, q := range res.Quotas {
		if errs := validation.ValidateQuotaShape(q); len(errs) != 0 {
			t.Fatalf("reconstructed KafkaQuota %q must pass ValidateQuotaShape; got %d error(s):\n%v",
				q.Name, len(errs), errs)
		}
	}

	// Round-trip property for users: the loaded-back KafkaUser's compiled
	// observable identity must equal the live credential exactly (username,
	// mechanism, iterations) — proving import->apply->verify would stay clean.
	if len(res.Users) != 1 {
		t.Fatalf("want 1 reconstructed KafkaUser, got %d", len(res.Users))
	}
	compiled := user.Compile(in.Users[0])
	wantCred := user.Credential{Username: "svc-checkout", Mechanism: "SCRAM-SHA-512"}
	if compiled != wantCred {
		t.Fatalf("recomputed identity %+v != live identity %+v", compiled, wantCred)
	}
}

// avroOrderCreatedBody is the body whose record full name is com.acme.OrderCreated,
// used to exercise the TopicRecordName strategy in the round-trip property test.
const avroOrderCreatedBody = `{"type":"record","namespace":"com.acme","name":"OrderCreated","fields":[{"name":"id","type":"string"}]}`

// TestTopicRecordNameRoundTripProperty is the round-trip proof for the
// TopicRecordName strategy (spec §24.1): importing a TopicRecordName-matched
// topic produces a manifest whose spec.schema.subjectStrategy is
// "TopicRecordName", and calling recordname.Subjects with the GENERATED
// manifest + the EMITTED schema file body re-derives the ORIGINAL subject name
// exactly — proving that the import->forward-compute chain is lossless.
//
// The test also confirms that the generated manifest passes validation, i.e. the
// importer never produces a manifest that the tool itself rejects.
func TestTopicRecordNameRoundTripProperty(t *testing.T) {
	const clusterName = "prod"
	const topicName = "orders"
	// Original subject as it appears in the Schema Registry.
	const originalSubject = "orders-com.acme.OrderCreated"

	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{
			{Name: topicName, Partitions: 3, ReplicationFactor: 3},
		},
		Schemas: map[string]importer.TopicSchemas{
			topicName: {
				Value: &schemaregistry.SubjectState{
					Subject:       originalSubject,
					Schema:        schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: avroOrderCreatedBody},
					Compatibility: "FULL",
				},
				ValueStrategy: "TopicRecordName",
			},
		},
	}

	res := importer.Build(snap, clusterName, nil, nil)
	if err := importer.AssignNamespaces(&res, importer.NamespaceStrategy{Kind: "single", Single: "default"}); err != nil {
		t.Fatalf("AssignNamespaces: %v", err)
	}

	dir := t.TempDir()
	if _, err := importer.WriteToDir(res, dir, "never"); err != nil {
		t.Fatalf("WriteToDir: %v", err)
	}

	// --- Part 1: generated manifest passes validate ---
	objs, err := loader.Load(loader.Options{Filenames: []string{dir}, Recursive: true})
	if err != nil {
		t.Fatalf("loader.Load: %v", err)
	}
	in := validation.Input{
		Clusters: map[string]*v1alpha1.KafkaCluster{
			clusterName: {
				TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaCluster"},
				ObjectMeta: metav1.ObjectMeta{Name: clusterName},
				Spec: v1alpha1.KafkaClusterSpec{
					BootstrapServers: "localhost:9092",
					SchemaRegistry:   &v1alpha1.SchemaRegistryConf{Endpoint: "http://sr"},
				},
			},
		},
	}
	for _, o := range objs {
		if o.Topic != nil {
			in.Topics = append(in.Topics, o.Topic)
		}
	}
	if len(in.Topics) != 1 {
		t.Fatalf("want 1 topic loaded back, got %d", len(in.Topics))
	}
	generatedTopic := in.Topics[0]
	if errs := validation.Validate(in); len(errs) != 0 {
		t.Fatalf("TopicRecordName manifest must pass validate; got %d error(s):\n%v", len(errs), errs)
	}

	// --- Part 2: round-trip subject re-derivation ---
	// The generated manifest's spec.schema must carry subjectStrategy TopicRecordName.
	sc := generatedTopic.Spec.Schema
	if sc == nil {
		t.Fatalf("generated topic must have spec.schema set")
	}
	if sc.SubjectStrategy != "TopicRecordName" {
		t.Fatalf("want subjectStrategy TopicRecordName in generated manifest, got %q", sc.SubjectStrategy)
	}

	// Resolve the emitted schema file from disk. The file is written to
	// <dir>/<namespace>/schemas/<metaName>-value.avsc (same path that
	// spec.schema.valueSchema.valueFrom.file references as ../schemas/...).
	// We match it by examining res.SchemaFiles.
	var emittedBody string
	for _, sf := range res.SchemaFiles {
		if sf.MetaName == topicName && sf.BaseName == topicName+"-value" {
			// Read from the location WriteToDir wrote it.
			p := filepath.Join(dir, sf.Namespace, "schemas", sf.BaseName+"."+sf.Ext)
			data, err := os.ReadFile(p)
			if err != nil {
				t.Fatalf("read emitted schema file %q: %v", p, err)
			}
			emittedBody = string(data)
			break
		}
	}
	if emittedBody == "" {
		t.Fatalf("no emitted schema file found for topic %q", topicName)
	}

	// Re-derive the value subject from the GENERATED manifest + EMITTED body via
	// the forward computation. This is the exact call the CLI/operator would make
	// at apply time to reconstruct the subject name.
	derivedValue, _, err := recordname.Subjects(
		sc.SubjectStrategy,
		generatedTopic.Spec.TopicName,
		sc,
		emittedBody,
		"", // no key schema
	)
	if err != nil {
		t.Fatalf("recordname.Subjects: %v", err)
	}

	// The re-derived subject must equal the ORIGINAL subject from the Schema
	// Registry. Any mismatch means the import->forward-compute chain is lossy.
	if derivedValue != originalSubject {
		t.Fatalf("round-trip subject mismatch: re-derived %q, want original %q", derivedValue, originalSubject)
	}

	// --- Part 3: Result-wide round-trip invariant (in-memory, mirrors Part 2 but
	// exercised via the same helper the internal schemaverify tests use) ---
	// This is a second, independent check over res/snap directly (no disk I/O),
	// reusing the exact assertion schemaverify_test.go's mixed-strategy tests rely
	// on, so both the disk-and-CLI-facing path and the in-memory Result are
	// proven not to carry a mutating spec.schema block.
	importer.RequireNoMutatingSchemaBlocks(t, snap, res)
}

// TestImportedManifestNamesAreDNS1123Property is the naming counterpart to
// TestImportedManifestsPassValidate: every emitted manifest's metadata.name,
// across all four importer-emitted kinds (KafkaTopic, KafkaAccessPolicy,
// KafkaQuota, KafkaRoleBinding), must pass
// k8s.io/apimachinery/pkg/util/validation.IsDNS1123Subdomain. The seed data is
// deliberately adversarial: uppercase and underscore topic names (review bug
// 1), a 260-char topic name (exercises the truncate-to-253 path — the slug
// must both fit AND leave room for a disambiguateName suffix), leading-dash-
// and leading-digit-producing inputs, and colliding quota entities (review bug
// 2: default-sentinel-vs-literal and cross-component aliasing). A manifest
// that fails this property is one kubectl apply would reject outright.
func TestImportedManifestNamesAreDNS1123Property(t *testing.T) {
	const clusterName = "prod"

	longTopic := strings.Repeat("Z", 260) // uppercase + over the 253 limit
	mdsCfg := &v1alpha1.MDSConfig{Endpoint: "https://mds", Clusters: v1alpha1.MDSClusters{KafkaCluster: "kid"}}

	snap := importer.Snapshot{
		Topics: []importer.TopicSnapshot{
			// Bug 1: uppercase + underscore, invalid as a k8s object name.
			{Name: "Orders_V2", Partitions: 3, ReplicationFactor: 1},
			// Slugs to the SAME base as the above ("orders-v2") -> disambiguation.
			{Name: "orders-v2", Partitions: 1, ReplicationFactor: 1},
			// Already-valid dotted name (must round-trip unmutated).
			{Name: "payments.orders", Partitions: 1, ReplicationFactor: 1},
			// Leading-digit / leading-dash-producing adversarial name.
			{Name: "9-Weird__Name--", Partitions: 1, ReplicationFactor: 1},
			// Over the 253-char limit once slugged.
			{Name: longTopic, Partitions: 1, ReplicationFactor: 1},
		},
		Quotas: []kafka.QuotaState{
			// Bug 2a: default sentinel vs. literal name "default".
			{Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: nil}}, Limits: map[string]float64{"producer_byte_rate": 1}},
			{Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: ptr("default")}}, Limits: map[string]float64{"producer_byte_rate": 2}},
			// Bug 2b: cross-component alias ("client-id-b-user-a" both ways).
			{Entity: []kafka.QuotaEntityComponent{{Type: "client-id", Name: ptr("b-user-a")}}, Limits: map[string]float64{"producer_byte_rate": 3}},
			{Entity: []kafka.QuotaEntityComponent{
				{Type: "user", Name: ptr("a")},
				{Type: "client-id", Name: ptr("b")},
			}, Limits: map[string]float64{"producer_byte_rate": 4}},
			// Adversarial entity name: uppercase, punctuation.
			{Entity: []kafka.QuotaEntityComponent{{Type: "ip", Name: ptr("10.0.0.1")}}, Limits: map[string]float64{"connection_creation_rate": 5}},
		},
		ACLs: []kafka.ACLState{
			// Raw ACLs so at least one KafkaAccessPolicy is emitted, with
			// slug-colliding principals to exercise policy disambiguation too.
			acl("User:a-b", "topic", "payments.orders", "Write", withPattern("prefixed")),
			acl("User:a:b", "topic", "payments.orders", "Write", withPattern("prefixed")),
		},
		RoleBindings: []mds.RoleBinding{
			// Cluster-scoped: always emitted as an explicit KafkaRoleBinding.
			{Principal: "User:Admin_1", Role: "SystemAdmin", Scope: mds.Scope{Type: "kafka", KafkaCluster: "kid"}},
		},
	}

	r := importer.Build(snap, clusterName, []string{"acl", "rbac"}, mdsCfg)
	if err := importer.AssignNamespaces(&r, importer.NamespaceStrategy{Kind: "single", Single: "default"}); err != nil {
		t.Fatalf("AssignNamespaces: %v", err)
	}

	assertName := func(kind, name string) {
		t.Helper()
		if errs := apivalidation.IsDNS1123Subdomain(name); len(errs) != 0 {
			t.Errorf("%s metadata.name %q is not DNS-1123-valid: %v", kind, name, errs)
		}
	}

	if len(r.Topics) == 0 {
		t.Fatalf("want topics emitted, got none")
	}
	for _, tp := range r.Topics {
		assertName("KafkaTopic", tp.Name)
	}
	for _, p := range r.Policies {
		assertName("KafkaAccessPolicy", p.Name)
	}
	if len(r.Quotas) != 5 {
		t.Fatalf("want 5 quotas, got %d", len(r.Quotas))
	}
	for _, q := range r.Quotas {
		assertName("KafkaQuota", q.Name)
	}
	for _, rb := range r.RoleBindings {
		assertName("KafkaRoleBinding", rb.Name)
	}

	// Every metadata.name across every kind must also be globally unique
	// within its own kind (WriteToDir writes one file per name per kind-dir;
	// a same-kind collision would silently clobber a file).
	seen := map[string]map[string]bool{}
	check := func(kind, name string) {
		if seen[kind] == nil {
			seen[kind] = map[string]bool{}
		}
		if seen[kind][name] {
			t.Errorf("duplicate %s metadata.name %q", kind, name)
		}
		seen[kind][name] = true
	}
	for _, tp := range r.Topics {
		check("KafkaTopic", tp.Name)
	}
	for _, p := range r.Policies {
		check("KafkaAccessPolicy", p.Name)
	}
	for _, q := range r.Quotas {
		check("KafkaQuota", q.Name)
	}
	for _, rb := range r.RoleBindings {
		check("KafkaRoleBinding", rb.Name)
	}

	// The 260-char uppercase topic must have been truncated to fit within
	// maxNameLen (253) while remaining DNS-1123-valid (already checked above)
	// AND its live name must still be recoverable via spec.topicName.
	var sawLongTopic bool
	for _, tp := range r.Topics {
		if tp.Spec.TopicName == longTopic {
			sawLongTopic = true
			if len(tp.Name) > 253 {
				t.Errorf("truncated topic metadata.name exceeds 253 chars: %d", len(tp.Name))
			}
		}
	}
	if !sawLongTopic {
		t.Fatalf("want the 260-char topic's spec.topicName preserved verbatim, not found among %d topics", len(r.Topics))
	}

	// Sanity: the already-valid dotted topic name must be byte-identical
	// (slugging must not touch names that are already DNS-1123-valid).
	var sawDottedUnchanged bool
	for _, tp := range r.Topics {
		if tp.Spec.TopicName == "payments.orders" {
			sawDottedUnchanged = true
			if tp.Name != "payments.orders" {
				t.Errorf("already-valid dotted topic name must be unchanged, got %q", tp.Name)
			}
		}
	}
	if !sawDottedUnchanged {
		t.Fatalf("want payments.orders topic present")
	}
}

// TestImportedUsersRoundTripProperty is the round-trip proof for SCRAM user
// import: adversarial live usernames (uppercase, dots, a both-mechanisms
// user) are captured, written to disk, loaded back with the real loader, and
// must (a) pass validation.Validate, (b) carry DNS-1123-valid metadata.names,
// and (c) recompute (via user.Compile) to EXACTLY the live identity for
// whichever mechanism the importer captured — proving verify would report
// clean immediately after apply.
func TestImportedUsersRoundTripProperty(t *testing.T) {
	const clusterName = "prod"

	snap := importer.Snapshot{
		ScramCredentials: []kafka.ScramCredential{
			// Adversarial: uppercase + underscore username (review-bug-1-style,
			// mirroring the topic naming adversarial case).
			{User: "Svc_Checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
			// Adversarial: dotted username (dots preserved by topicMetaName-style
			// slugging, unlike slug()).
			{User: "svc.billing", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
			// Both mechanisms live simultaneously: only SCRAM-SHA-512 must be
			// captured, with a warning naming the dropped SCRAM-SHA-256.
			{User: "svc-reporting", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
			{User: "svc-reporting", Mechanism: "SCRAM-SHA-512", Iterations: 8192},
		},
	}

	res := importer.Build(snap, clusterName, nil, nil)
	if err := importer.AssignNamespaces(&res, importer.NamespaceStrategy{Kind: "single", Single: "default"}); err != nil {
		t.Fatalf("AssignNamespaces: %v", err)
	}

	if len(res.Users) != 3 {
		t.Fatalf("want 3 KafkaUser manifests (one per distinct username), got %d", len(res.Users))
	}

	// (b) DNS-1123 metadata.name.
	for _, u := range res.Users {
		if errs := apivalidation.IsDNS1123Subdomain(u.Name); len(errs) != 0 {
			t.Errorf("KafkaUser metadata.name %q is not DNS-1123-valid: %v", u.Name, errs)
		}
	}

	// Both-mechanisms warning must name the dropped SCRAM-SHA-256 credential.
	wantWarning := `user "svc-reporting" also has a SCRAM-SHA-256 credential; only the SCRAM-SHA-512 one is captured — manage the other manually or add a second manifest with a different metadata.name`
	sawWarning := false
	for _, w := range res.Warnings {
		if w == wantWarning {
			sawWarning = true
		}
	}
	if !sawWarning {
		t.Errorf("want both-mechanisms warning, got: %v", res.Warnings)
	}

	dir := t.TempDir()
	if _, err := importer.WriteToDir(res, dir, "never"); err != nil {
		t.Fatalf("WriteToDir: %v", err)
	}

	objs, err := loader.Load(loader.Options{Filenames: []string{dir}, Recursive: true})
	if err != nil {
		t.Fatalf("loader.Load: %v", err)
	}
	in := validation.Input{
		Clusters: map[string]*v1alpha1.KafkaCluster{
			clusterName: {
				TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaCluster"},
				ObjectMeta: metav1.ObjectMeta{Name: clusterName},
				Spec:       v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092"},
			},
		},
	}
	for _, o := range objs {
		if o.User != nil {
			in.Users = append(in.Users, o.User)
		}
	}
	if len(in.Users) != 3 {
		t.Fatalf("want 3 users loaded back, got %d", len(in.Users))
	}

	// (a) generated manifests pass validate.
	if errs := validation.Validate(in); len(errs) != 0 {
		t.Fatalf("imported KafkaUser manifests must pass validate; got %d error(s):\n%v", len(errs), errs)
	}

	// (c) recomputed identity == live identity, for the CAPTURED mechanism only.
	wantByUsername := map[string]user.Credential{
		"Svc_Checkout":  {Username: "Svc_Checkout", Mechanism: "SCRAM-SHA-512"},
		"svc.billing":   {Username: "svc.billing", Mechanism: "SCRAM-SHA-256"},
		"svc-reporting": {Username: "svc-reporting", Mechanism: "SCRAM-SHA-512", Iterations: 8192},
	}
	seen := map[string]bool{}
	for _, u := range in.Users {
		want, ok := wantByUsername[u.Spec.Username]
		if !ok {
			t.Fatalf("unexpected username in loaded manifests: %q", u.Spec.Username)
		}
		seen[u.Spec.Username] = true
		got := user.Compile(u)
		if got != want {
			t.Errorf("user %q: recomputed identity %+v != live identity %+v", u.Spec.Username, got, want)
		}
	}
	if len(seen) != len(wantByUsername) {
		t.Fatalf("want all %d usernames present, saw %d", len(wantByUsername), len(seen))
	}
}
