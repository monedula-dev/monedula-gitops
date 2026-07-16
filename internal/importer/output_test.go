package importer_test

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/importer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var update = flag.Bool("update", false, "update golden files")

func assertGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	p := filepath.Join("testdata", name)
	if *update {
		require.NoError(t, os.WriteFile(p, got, 0o644))
	}
	want, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
}

func intp(i int) *int { return &i }

// sampleResult builds a fixed, post-namespace-assignment Result: two topics
// (each in its own namespace, with config + producer/consumer access) and one
// raw access policy. Used by all output goldens.
func sampleResult() importer.Result {
	t1 := &v1alpha1.KafkaTopic{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaTopic"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "orders",
			Namespace:   "payments",
			Annotations: map[string]string{"gitops.monedula.dev/imported-from-cluster": "prod-eu"},
		},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef:        v1alpha1.ClusterRef{Name: "prod-eu"},
			TopicName:         "payments.orders",
			Partitions:        6,
			ReplicationFactor: intp(3),
			Config:            map[string]string{"retention.ms": "604800000", "cleanup.policy": "delete"},
			Access: v1alpha1.TopicAccess{
				Producers: []v1alpha1.ProducerAccess{{Principal: "User:svc-checkout"}},
				Consumers: []v1alpha1.ConsumerAccess{{Principal: "User:svc-reporting", Group: "reporting"}},
			},
			DeletionPolicy: "Orphan",
		},
	}
	t2 := &v1alpha1.KafkaTopic{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaTopic"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "events",
			Namespace:   "analytics",
			Annotations: map[string]string{"gitops.monedula.dev/imported-from-cluster": "prod-eu"},
		},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef:     v1alpha1.ClusterRef{Name: "prod-eu"},
			TopicName:      "analytics.events",
			Partitions:     3,
			DeletionPolicy: "Orphan",
		},
	}
	p1 := &v1alpha1.KafkaAccessPolicy{
		TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaAccessPolicy"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "imported-user-admin",
			Namespace:   "platform",
			Annotations: map[string]string{"gitops.monedula.dev/imported-from-cluster": "prod-eu"},
		},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod-eu"},
			Rules: []v1alpha1.ACLRule{
				{
					Principal:  "User:admin",
					Permission: "Allow",
					Host:       "*",
					Resource:   v1alpha1.ACLResource{Type: "cluster", Name: "kafka-cluster", PatternType: "literal"},
					Operations: []string{"Alter", "Describe"},
				},
			},
			DeletionPolicy: "Delete",
		},
	}
	// Deliberately supply topics/policies out of sorted order to exercise sorting.
	return importer.Result{
		Topics:   []*v1alpha1.KafkaTopic{t1, t2},
		Policies: []*v1alpha1.KafkaAccessPolicy{p1},
		Warnings: []string{"principal \"User:x\" has ambiguous consumer group mapping"},
	}
}

func TestRenderManifestsGolden(t *testing.T) {
	got, err := importer.RenderManifests(sampleResult())
	require.NoError(t, err)
	assertGolden(t, "manifests.yaml", got)

	got2, err := importer.RenderManifests(sampleResult())
	require.NoError(t, err)
	require.Equal(t, string(got), string(got2))
}

func TestRenderSummaryGoldens(t *testing.T) {
	out := importer.Summarize(sampleResult(), "prod-eu")
	require.Equal(t, 2, out.Summary.Topics)
	require.Equal(t, 2, out.Summary.TopicAccessRules) // 1 producer + 1 consumer
	require.Equal(t, 1, out.Summary.AccessPolicies)
	require.Equal(t, 1, out.Summary.PolicyRules)

	for _, tc := range []struct {
		format, golden string
	}{
		{"human", "summary.human"},
		{"yaml", "summary.yaml"},
		{"json", "summary.json"},
	} {
		got, err := importer.RenderSummary(out, tc.format)
		require.NoError(t, err)
		assertGolden(t, tc.golden, got)
		got2, err := importer.RenderSummary(out, tc.format)
		require.NoError(t, err)
		require.Equal(t, string(got), string(got2))
	}
}

// sampleResultWithRecordNames returns a Result that extends sampleResult with
// two RecordName-strategy subjects for the RecordName section golden tests.
func sampleResultWithRecordNames() importer.Result {
	res := sampleResult()
	res.RecordNameSubjects = []importer.RecordNameSubject{
		{Subject: "com.example.events.OrderPlaced", RecordName: "com.example.events.OrderPlaced", SchemaType: "AVRO"},
		{Subject: "com.example.payments.PaymentSettled", RecordName: "com.example.payments.PaymentSettled", SchemaType: "AVRO"},
	}
	return res
}

func TestRenderSummaryRecordNameGoldens(t *testing.T) {
	out := importer.Summarize(sampleResultWithRecordNames(), "prod-eu")
	require.Equal(t, 2, out.Summary.RecordNameSubjects)
	require.Equal(t, 2, len(out.RecordNameSubjects))
	require.Equal(t, "com.example.events.OrderPlaced", out.RecordNameSubjects[0].Subject)
	require.Equal(t, "com.example.payments.PaymentSettled", out.RecordNameSubjects[1].Subject)

	for _, tc := range []struct {
		format, golden string
	}{
		{"human", "summary-recordname.human"},
		{"yaml", "summary-recordname.yaml"},
		{"json", "summary-recordname.json"},
	} {
		got, err := importer.RenderSummary(out, tc.format)
		require.NoError(t, err)
		assertGolden(t, tc.golden, got)
		// determinism: render twice, expect identical output
		got2, err := importer.RenderSummary(out, tc.format)
		require.NoError(t, err)
		require.Equal(t, string(got), string(got2))
	}
}

func TestRenderSummaryUnknownFormatErrors(t *testing.T) {
	out := importer.Summarize(sampleResult(), "prod-eu")
	_, err := importer.RenderSummary(out, "xml")
	require.Error(t, err)
}

// TestRenderSummarySchemasSkipped verifies that when SchemasSkipped is true:
//   - human format prints "Schemas: skipped (--skip-schemas)" instead of a count
//   - json format includes "schemasSkipped": true
//   - yaml format includes "schemasSkipped: true"
func TestRenderSummarySchemasSkipped(t *testing.T) {
	out := importer.Summarize(sampleResult(), "prod-eu")
	out.SchemasSkipped = true

	// human
	human, err := importer.RenderSummary(out, "human")
	require.NoError(t, err)
	require.Contains(t, string(human), "Schemas: skipped (--skip-schemas)")
	require.NotContains(t, string(human), "Schemas: 0")

	// json
	j, err := importer.RenderSummary(out, "json")
	require.NoError(t, err)
	require.Contains(t, string(j), `"schemasSkipped": true`)

	// yaml
	y, err := importer.RenderSummary(out, "yaml")
	require.NoError(t, err)
	require.Contains(t, string(y), "schemasSkipped: true")
}

// TestRenderSummarySchemasSkippedFalseOmitted verifies that when SchemasSkipped
// is false (default), neither json nor yaml output includes the field (omitempty).
func TestRenderSummarySchemasSkippedFalseOmitted(t *testing.T) {
	out := importer.Summarize(sampleResult(), "prod-eu")
	// SchemasSkipped defaults to false

	j, err := importer.RenderSummary(out, "json")
	require.NoError(t, err)
	require.NotContains(t, string(j), "schemasSkipped")

	y, err := importer.RenderSummary(out, "yaml")
	require.NoError(t, err)
	require.NotContains(t, string(y), "schemasSkipped")
}

func TestWriteToDirFresh(t *testing.T) {
	dir := t.TempDir()
	outcome, err := importer.WriteToDir(sampleResult(), dir, "never")
	require.NoError(t, err)
	require.Empty(t, outcome.Skipped)
	require.Equal(t, []string{
		filepath.Join(dir, "analytics", "topics", "events.yaml"),
		filepath.Join(dir, "payments", "topics", "orders.yaml"),
		filepath.Join(dir, "platform", "access", "imported-user-admin.yaml"),
	}, outcome.Written)

	// Read one topic back and confirm it parses to the right kind/name.
	data, err := os.ReadFile(filepath.Join(dir, "payments", "topics", "orders.yaml"))
	require.NoError(t, err)
	var tp v1alpha1.KafkaTopic
	require.NoError(t, yaml.Unmarshal(data, &tp))
	require.Equal(t, "KafkaTopic", tp.Kind)
	require.Equal(t, "orders", tp.Name)
	require.Equal(t, "payments", tp.Namespace)

	// And the policy.
	pdata, err := os.ReadFile(filepath.Join(dir, "platform", "access", "imported-user-admin.yaml"))
	require.NoError(t, err)
	var pol v1alpha1.KafkaAccessPolicy
	require.NoError(t, yaml.Unmarshal(pdata, &pol))
	require.Equal(t, "KafkaAccessPolicy", pol.Kind)
	require.Equal(t, "imported-user-admin", pol.Name)
}

func TestWriteToDirNeverSkips(t *testing.T) {
	dir := t.TempDir()
	_, err := importer.WriteToDir(sampleResult(), dir, "never")
	require.NoError(t, err)

	// Capture content before re-run.
	path := filepath.Join(dir, "payments", "topics", "orders.yaml")
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	outcome, err := importer.WriteToDir(sampleResult(), dir, "never")
	require.NoError(t, err)
	require.Empty(t, outcome.Written)
	require.Equal(t, []string{
		filepath.Join(dir, "analytics", "topics", "events.yaml"),
		filepath.Join(dir, "payments", "topics", "orders.yaml"),
		filepath.Join(dir, "platform", "access", "imported-user-admin.yaml"),
	}, outcome.Skipped)

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after)) // unchanged
}

func TestWriteToDirChanged(t *testing.T) {
	dir := t.TempDir()
	_, err := importer.WriteToDir(sampleResult(), dir, "never")
	require.NoError(t, err)

	// Tamper one file.
	path := filepath.Join(dir, "payments", "topics", "orders.yaml")
	require.NoError(t, os.WriteFile(path, []byte("tampered\n"), 0o644))

	outcome, err := importer.WriteToDir(sampleResult(), dir, "changed")
	require.NoError(t, err)
	require.Equal(t, []string{path}, outcome.Written)
	require.Equal(t, []string{
		filepath.Join(dir, "analytics", "topics", "events.yaml"),
		filepath.Join(dir, "platform", "access", "imported-user-admin.yaml"),
	}, outcome.Skipped)

	// Tampered file was restored to canonical content.
	restored, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotEqual(t, "tampered\n", string(restored))
	var tp v1alpha1.KafkaTopic
	require.NoError(t, yaml.Unmarshal(restored, &tp))
	require.Equal(t, "orders", tp.Name)
}

func TestWriteToDirAlways(t *testing.T) {
	dir := t.TempDir()
	_, err := importer.WriteToDir(sampleResult(), dir, "never")
	require.NoError(t, err)

	outcome, err := importer.WriteToDir(sampleResult(), dir, "always")
	require.NoError(t, err)
	require.Empty(t, outcome.Skipped)
	require.Equal(t, []string{
		filepath.Join(dir, "analytics", "topics", "events.yaml"),
		filepath.Join(dir, "payments", "topics", "orders.yaml"),
		filepath.Join(dir, "platform", "access", "imported-user-admin.yaml"),
	}, outcome.Written)
}

func TestWriteToDirUnknownOverwriteErrors(t *testing.T) {
	dir := t.TempDir()
	_, err := importer.WriteToDir(sampleResult(), dir, "sometimes")
	require.Error(t, err)
}

func TestWriteToDirWritesSchemaFiles(t *testing.T) {
	res := importer.Result{
		SchemaFiles: []importer.SchemaFile{
			{Namespace: "payments", MetaName: "payments.orders", BaseName: "payments.orders-value", Ext: "avsc", Content: "AVRO-BODY"},
		},
	}
	dir := t.TempDir()
	outcome, err := importer.WriteToDir(res, dir, "never")
	require.NoError(t, err)

	p := filepath.Join(dir, "payments", "schemas", "payments.orders-value.avsc")
	require.FileExists(t, p)
	got, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, "AVRO-BODY", string(got))
	require.Contains(t, outcome.Written, p)

	// overwrite "never": existing schema file is skipped.
	outcome2, err := importer.WriteToDir(res, dir, "never")
	require.NoError(t, err)
	require.Contains(t, outcome2.Skipped, p)
	require.NotContains(t, outcome2.Written, p)

	// overwrite "always": rewritten even though unchanged.
	outcome3, err := importer.WriteToDir(res, dir, "always")
	require.NoError(t, err)
	require.Contains(t, outcome3.Written, p)
}

func TestWriteToDirRoleBindingsPath(t *testing.T) {
	res := importer.Result{
		RoleBindings: []*v1alpha1.KafkaRoleBinding{
			{
				TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaRoleBinding"},
				ObjectMeta: metav1.ObjectMeta{Name: "imported-rb-svc-systemadmin", Namespace: "platform"},
				Spec:       v1alpha1.KafkaRoleBindingSpec{ClusterRef: v1alpha1.ClusterRef{Name: "prod-eu"}, Principal: "User:svc", Role: "SystemAdmin"},
			},
		},
	}
	dir := t.TempDir()
	outcome, err := importer.WriteToDir(res, dir, "never")
	require.NoError(t, err)
	require.Len(t, outcome.Written, 1)

	want := filepath.Join(dir, "platform", "rolebindings", "imported-rb-svc-systemadmin.yaml")
	require.Equal(t, want, outcome.Written[0])
	require.FileExists(t, want)

	data, err := os.ReadFile(want)
	require.NoError(t, err)
	var rb v1alpha1.KafkaRoleBinding
	require.NoError(t, yaml.Unmarshal(data, &rb))
	require.Equal(t, "KafkaRoleBinding", rb.Kind)
	require.Equal(t, "imported-rb-svc-systemadmin", rb.Name)
}

func TestRenderManifestsIncludesRoleBindings(t *testing.T) {
	res := importer.Result{
		RoleBindings: []*v1alpha1.KafkaRoleBinding{
			{TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaRoleBinding"},
				ObjectMeta: metav1.ObjectMeta{Name: "imported-rb-x", Namespace: "team-a"},
				Spec:       v1alpha1.KafkaRoleBindingSpec{ClusterRef: v1alpha1.ClusterRef{Name: "prod"}, Principal: "User:svc", Role: "SystemAdmin"}},
		},
	}
	out, err := importer.RenderManifests(res)
	require.NoError(t, err)
	require.Contains(t, string(out), "kind: KafkaRoleBinding")
	require.Contains(t, string(out), "imported-rb-x")

	sum := importer.Summarize(res, "prod")
	require.Equal(t, 1, sum.Summary.RoleBindings)
}

func TestWriteToDirUsersPath(t *testing.T) {
	res := importer.Result{
		Users: []*v1alpha1.KafkaUser{
			{
				TypeMeta:   metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaUser"},
				ObjectMeta: metav1.ObjectMeta{Name: "svc-checkout", Namespace: "platform"},
				Spec: v1alpha1.KafkaUserSpec{
					ClusterRef: v1alpha1.ClusterRef{Name: "prod-eu"},
					Username:   "svc-checkout",
					Mechanism:  "SCRAM-SHA-512",
					Password: &v1alpha1.UserPassword{
						ValueFrom: &v1alpha1.ValueSource{Env: "KAFKA_USER_SVC_CHECKOUT_PASSWORD"},
					},
				},
			},
		},
	}
	dir := t.TempDir()
	outcome, err := importer.WriteToDir(res, dir, "never")
	require.NoError(t, err)
	require.Len(t, outcome.Written, 1)

	want := filepath.Join(dir, "platform", "users", "svc-checkout.yaml")
	require.Equal(t, want, outcome.Written[0])
	require.FileExists(t, want)

	data, err := os.ReadFile(want)
	require.NoError(t, err)
	var u v1alpha1.KafkaUser
	require.NoError(t, yaml.Unmarshal(data, &u))
	require.Equal(t, "KafkaUser", u.Kind)
	require.Equal(t, "svc-checkout", u.Name)
	require.Equal(t, "svc-checkout", u.Spec.Username)
	require.Equal(t, "KAFKA_USER_SVC_CHECKOUT_PASSWORD", u.Spec.Password.ValueFrom.Env)
}

func TestRenderManifestsIncludesUsers(t *testing.T) {
	res := importer.Result{
		Users: []*v1alpha1.KafkaUser{
			{TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaUser"},
				ObjectMeta: metav1.ObjectMeta{Name: "svc-checkout", Namespace: "team-a"},
				Spec: v1alpha1.KafkaUserSpec{
					ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
					Username:   "svc-checkout",
					Mechanism:  "SCRAM-SHA-512",
					Password:   &v1alpha1.UserPassword{ValueFrom: &v1alpha1.ValueSource{Env: "KAFKA_USER_SVC_CHECKOUT_PASSWORD"}},
				}},
		},
	}
	out, err := importer.RenderManifests(res)
	require.NoError(t, err)
	require.Contains(t, string(out), "kind: KafkaUser")
	require.Contains(t, string(out), "svc-checkout")

	sum := importer.Summarize(res, "prod")
	require.Equal(t, 1, sum.Summary.Users)
}

// TestRenderSummaryUsersSkipped verifies the --skip-users summary presentation
// mirrors --skip-schemas: human shows "skipped (--skip-users)" instead of a
// count, json/yaml carry usersSkipped: true.
func TestRenderSummaryUsersSkipped(t *testing.T) {
	out := importer.Summarize(sampleResult(), "prod-eu")
	out.UsersSkipped = true

	human, err := importer.RenderSummary(out, "human")
	require.NoError(t, err)
	require.Contains(t, string(human), "Users: skipped (--skip-users)")
	require.NotContains(t, string(human), "Users: 0")

	j, err := importer.RenderSummary(out, "json")
	require.NoError(t, err)
	require.Contains(t, string(j), `"usersSkipped": true`)

	y, err := importer.RenderSummary(out, "yaml")
	require.NoError(t, err)
	require.Contains(t, string(y), "usersSkipped: true")
}

// TestRenderSummaryUsersSkippedFalseOmitted mirrors
// TestRenderSummarySchemasSkippedFalseOmitted for the Users field.
func TestRenderSummaryUsersSkippedFalseOmitted(t *testing.T) {
	out := importer.Summarize(sampleResult(), "prod-eu")

	j, err := importer.RenderSummary(out, "json")
	require.NoError(t, err)
	require.NotContains(t, string(j), "usersSkipped")

	y, err := importer.RenderSummary(out, "yaml")
	require.NoError(t, err)
	require.NotContains(t, string(y), "usersSkipped")
}
