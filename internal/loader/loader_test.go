package loader

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadSingleFile(t *testing.T) {
	objs, err := Load(Options{Filenames: []string{"testdata/orders.yaml"}})
	require.NoError(t, err)
	require.Len(t, objs, 1)
	require.Equal(t, "KafkaTopic", objs[0].Kind)
}

func TestLoadDirectoryNonRecursive(t *testing.T) {
	objs, err := Load(Options{Filenames: []string{"testdata/dir"}})
	require.NoError(t, err)
	require.Len(t, objs, 2) // only top-level files, NOT testdata/dir/nested/deep.yaml
}

func TestLoadDirectoryRecursive(t *testing.T) {
	objs, err := Load(Options{Filenames: []string{"testdata/dir"}, Recursive: true})
	require.NoError(t, err)
	require.Len(t, objs, 3) // includes nested/deep.yaml
}

func TestLoadMultipleFilenames(t *testing.T) {
	objs, err := Load(Options{Filenames: []string{"testdata/orders.yaml", "testdata/policy.yaml"}})
	require.NoError(t, err)
	require.Len(t, objs, 2)
}

func TestLoadStdin(t *testing.T) {
	doc := `apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaTopic
metadata: { name: t1 }
spec: { clusterRef: { name: c }, partitions: 1 }`
	objs, err := Load(Options{Filenames: []string{"-"}, Stdin: strings.NewReader(doc)})
	require.NoError(t, err)
	require.Len(t, objs, 1)
}

func TestLoadStdinNilReaderErrors(t *testing.T) {
	// I15: "-" with no stdin reader must be a clear error, not a panic from
	// handing a nil reader to the YAML decoder. This is reachable via
	// `--cluster-config-file -` (the pipeline's cluster-config load passes
	// Stdin: nil deliberately) and via `import --cluster-config-file -`.
	_, err := Load(Options{Filenames: []string{"-"}, Stdin: nil})
	require.Error(t, err)
	require.Contains(t, err.Error(), "stdin requested (-) but no stdin available")
}

func TestLoadDuplicateFilenamesDeduped(t *testing.T) {
	// The same path given twice (even spelled differently) must load once:
	// duplicate loads produce duplicate resources and spurious identity-collision
	// validation errors.
	objs, err := Load(Options{Filenames: []string{"testdata/orders.yaml", "./testdata/orders.yaml"}})
	require.NoError(t, err)
	require.Len(t, objs, 1)
}

func TestLoadDirectoryAndFileWithinItDeduped(t *testing.T) {
	// `-f dir -f dir/a.yaml` must load a.yaml once: the directory walk and the
	// direct file argument reach the same file via different path spellings,
	// so dedup must compare absolute paths, not just the literal argument
	// strings (which is all the plain testdata/orders.yaml + ./testdata/orders.yaml
	// case above exercises).
	objs, err := Load(Options{Filenames: []string{"testdata/dir", "testdata/dir/a.yaml"}})
	require.NoError(t, err)
	require.Len(t, objs, 2) // testdata/dir has exactly 2 top-level files (a.yaml, b.yaml)

	// Order reversed must give the same result.
	objs, err = Load(Options{Filenames: []string{"testdata/dir/a.yaml", "testdata/dir"}})
	require.NoError(t, err)
	require.Len(t, objs, 2)
}

func TestLoadStdinReadOnce(t *testing.T) {
	// "-" repeated dedupes too: stdin is a one-shot stream.
	doc := `apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaTopic
metadata: { name: t1 }
spec: { clusterRef: { name: c }, partitions: 1 }`
	objs, err := Load(Options{Filenames: []string{"-", "-"}, Stdin: strings.NewReader(doc)})
	require.NoError(t, err)
	require.Len(t, objs, 1)
}

func TestLoadMultiDocYAML(t *testing.T) {
	objs, err := Load(Options{Filenames: []string{"testdata/multidoc.yaml"}})
	require.NoError(t, err)
	require.Len(t, objs, 2)
}

// TestLoadKafkaQuota verifies that a KafkaQuota manifest is decoded into
// Object.Quota with entity and limits populated.
func TestLoadKafkaQuota(t *testing.T) {
	objs, err := Load(Options{Filenames: []string{"testdata/quota.yaml"}})
	require.NoError(t, err)
	require.Len(t, objs, 1)
	o := objs[0]
	require.Equal(t, "KafkaQuota", o.Kind)
	require.NotNil(t, o.Quota)
	require.Nil(t, o.Topic)
	require.Nil(t, o.Policy)
	require.Nil(t, o.Cluster)
	require.Equal(t, "svc-checkout-quota", o.Quota.Name)
	require.Equal(t, "payments", o.Quota.Namespace)
	require.Equal(t, "prod", o.Quota.Spec.ClusterRef.Name)
	require.Equal(t, "User:svc-checkout", o.Quota.Spec.Entity.User)
	require.NotNil(t, o.Quota.Spec.Limits.ProducerByteRate)
	require.InDelta(t, 1048576.0, *o.Quota.Spec.Limits.ProducerByteRate, 0.001)
	require.NotNil(t, o.Quota.Spec.Limits.ConsumerByteRate)
	require.InDelta(t, 2097152.0, *o.Quota.Spec.Limits.ConsumerByteRate, 0.001)
}

// TestLoadKafkaRoleBinding verifies that a KafkaRoleBinding manifest is decoded
// into Object.RoleBinding with all fields populated.
func TestLoadKafkaRoleBinding(t *testing.T) {
	objs, err := Load(Options{Filenames: []string{"testdata/rolebinding.yaml"}})
	require.NoError(t, err)
	require.Len(t, objs, 1)
	o := objs[0]
	require.Equal(t, "KafkaRoleBinding", o.Kind)
	require.NotNil(t, o.RoleBinding)
	require.Nil(t, o.Topic)
	require.Nil(t, o.Policy)
	require.Nil(t, o.Cluster)
	require.Nil(t, o.Quota)
	require.Equal(t, "alice-developer", o.RoleBinding.Name)
	require.Equal(t, "payments", o.RoleBinding.Namespace)
	require.Equal(t, "prod", o.RoleBinding.Spec.ClusterRef.Name)
	require.Equal(t, "User:alice", o.RoleBinding.Spec.Principal)
	require.Equal(t, "DeveloperRead", o.RoleBinding.Spec.Role)
	require.Equal(t, "kafka", o.RoleBinding.Spec.Scope.Type)
	require.Len(t, o.RoleBinding.Spec.Resources, 1)
	require.Equal(t, "Topic", o.RoleBinding.Spec.Resources[0].Type)
	require.Equal(t, "payments.orders", o.RoleBinding.Spec.Resources[0].Name)
	require.Equal(t, "literal", o.RoleBinding.Spec.Resources[0].PatternType)
}

// TestLoadKafkaUser verifies that a KafkaUser manifest is decoded into
// Object.User with clusterRef, username, mechanism, and password populated.
func TestLoadKafkaUser(t *testing.T) {
	objs, err := Load(Options{Filenames: []string{"testdata/user.yaml"}})
	require.NoError(t, err)
	require.Len(t, objs, 1)
	o := objs[0]
	require.Equal(t, "KafkaUser", o.Kind)
	require.NotNil(t, o.User)
	require.Nil(t, o.Topic)
	require.Nil(t, o.Policy)
	require.Nil(t, o.Cluster)
	require.Nil(t, o.Quota)
	require.Nil(t, o.RoleBinding)
	require.Equal(t, "svc-checkout", o.User.Name)
	require.Equal(t, "payments", o.User.Namespace)
	require.Equal(t, "prod", o.User.Spec.ClusterRef.Name)
	require.Equal(t, "svc-checkout", o.User.Spec.Username)
	require.Equal(t, "SCRAM-SHA-512", o.User.Spec.Mechanism)
	require.NotNil(t, o.User.Spec.Password)
	require.NotNil(t, o.User.Spec.Password.ValueFrom)
	require.NotNil(t, o.User.Spec.Password.ValueFrom.SecretKeyRef)
	require.Equal(t, "svc-checkout-credentials", o.User.Spec.Password.ValueFrom.SecretKeyRef.Name)
	require.Equal(t, "password", o.User.Spec.Password.ValueFrom.SecretKeyRef.Key)
}

// TestLoadKafkaCluster verifies that a valid KafkaCluster manifest still loads.
func TestLoadKafkaCluster(t *testing.T) {
	objs, err := Load(Options{Filenames: []string{"testdata/cluster.yaml"}})
	require.NoError(t, err)
	require.Len(t, objs, 1)
	o := objs[0]
	require.Equal(t, "KafkaCluster", o.Kind)
	require.NotNil(t, o.Cluster)
	require.Equal(t, "prod", o.Cluster.Name)
	require.Equal(t, "broker:9092", o.Cluster.Spec.BootstrapServers)
}

// TestLoadTopicUnknownFieldFails verifies that a typo'd field ("configs:"
// instead of "config:") on a KafkaTopic fails loudly instead of being
// silently dropped by the decoder.
func TestLoadTopicUnknownFieldFails(t *testing.T) {
	_, err := Load(Options{Filenames: []string{"testdata/topic-unknown-field.yaml"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "testdata/topic-unknown-field.yaml")
	require.Contains(t, err.Error(), "configs")
}

// TestLoadClusterUnknownFieldFails verifies that a typo'd field
// ("bootstrapServer:" missing the trailing "s") on a KafkaCluster fails
// loudly instead of being silently dropped by the decoder.
func TestLoadClusterUnknownFieldFails(t *testing.T) {
	_, err := Load(Options{Filenames: []string{"testdata/cluster-unknown-field.yaml"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "testdata/cluster-unknown-field.yaml")
	require.Contains(t, err.Error(), "bootstrapServer")
}

// TestLoadUserUnknownFieldFails verifies that a typo'd field ("usernme:"
// instead of "username:") on a KafkaUser fails loudly instead of being
// silently dropped by the decoder.
func TestLoadUserUnknownFieldFails(t *testing.T) {
	_, err := Load(Options{Filenames: []string{"testdata/user-unknown-field.yaml"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "testdata/user-unknown-field.yaml")
	require.Contains(t, err.Error(), "usernme")
}

// TestLoadMultiDocSecondDocUnknownFieldFails verifies that when a multi-doc
// YAML file has a typo in its second document, the resulting error still
// identifies the offending field (and the shared source file path), even
// though the first document is valid.
func TestLoadMultiDocSecondDocUnknownFieldFails(t *testing.T) {
	_, err := Load(Options{Filenames: []string{"testdata/multidoc-second-unknown-field.yaml"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "testdata/multidoc-second-unknown-field.yaml")
	require.Contains(t, err.Error(), "configs")
}

// TestRepoFixturesLoadStrictly is a regression guard for strict decoding
// (typo'd fields must fail loudly, not be silently dropped): every committed
// manifest of our kinds under scenarios/ and quickstart/ must still load
// cleanly. If this fails, either the loader regressed or a fixture has a
// genuine latent typo that was previously masked by lenient decoding.
func TestRepoFixturesLoadStrictly(t *testing.T) {
	root := filepath.Join("..", "..")
	ourKinds := map[string]bool{
		"KafkaTopic":        true,
		"KafkaAccessPolicy": true,
		"KafkaCluster":      true,
		"KafkaQuota":        true,
		"KafkaRoleBinding":  true,
		"KafkaUser":         true,
	}
	hasOurKind := func(path string) bool {
		b, err := os.ReadFile(path)
		if err != nil {
			return false
		}
		for _, line := range strings.Split(string(b), "\n") {
			trimmed := strings.TrimSpace(line)
			if !strings.HasPrefix(trimmed, "kind:") {
				continue
			}
			val := strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "kind:")), `"'`)
			if ourKinds[val] {
				return true
			}
		}
		return false
	}

	var files []string
	globs := []string{
		"scenarios/*/manifests/*.yaml",
		"scenarios/clusters/*/cluster.yaml",
		"scenarios/clusters/*/k8s-cluster.yaml",
	}
	for _, g := range globs {
		matches, err := filepath.Glob(filepath.Join(root, g))
		require.NoError(t, err)
		files = append(files, matches...)
	}
	quickstartRoot := filepath.Join(root, "quickstart")
	require.NoError(t, filepath.Walk(quickstartRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && (filepath.Ext(path) == ".yaml" || filepath.Ext(path) == ".yml") {
			files = append(files, path)
		}
		return nil
	}))

	checked := 0
	for _, f := range files {
		if !hasOurKind(f) {
			continue
		}
		checked++
		_, err := Load(Options{Filenames: []string{f}})
		require.NoErrorf(t, err, "fixture %s failed to load under strict decoding", f)
	}
	require.NotZero(t, checked, "no fixture files matched our kinds; sweep is a no-op, check globs")
}
