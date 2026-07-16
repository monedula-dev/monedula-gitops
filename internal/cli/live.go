package cli

import (
	"os"
	"path/filepath"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/cluster"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	"github.com/monedula-dev/monedula-gitops/internal/pipeline"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// mockStateAnnotation names a file holding mock live state for a cluster. It is
// a hermetic test seam: when present on the selected KafkaCluster,
// buildLiveClient returns a mock seeded from that file instead of dialing a real
// broker. Production clusters omit it and get the real franz-go client.
const mockStateAnnotation = "gitops.monedula.dev/mock-state-file"

// mockSchemaAnnotation names a file holding mock Schema Registry state for a
// cluster. It is the hermetic test seam mirroring mockStateAnnotation: when
// present on the selected KafkaCluster, buildSchemaClient returns a mock seeded
// from that file instead of dialing a real registry.
const mockSchemaAnnotation = "gitops.monedula.dev/mock-schema-file"

// mockMDSAnnotation names a file holding mock MDS role-binding state for a
// cluster. It is the hermetic test seam mirroring mockStateAnnotation: when
// present on the selected KafkaCluster, buildMDSClient returns a mock seeded
// from that file instead of dialing a real MDS endpoint.
const mockMDSAnnotation = "gitops.monedula.dev/mock-mds-file"

// buildLiveClient resolves the live AdminClient for the selected cluster and a
// cleanup func the caller must defer.
//
// If the selected KafkaCluster carries the mock-state annotation
//
//	metadata.annotations["gitops.monedula.dev/mock-state-file"]
//
// live state is seeded from that file (path resolved relative to the first
// --cluster-config-file argument) and cleanup is a no-op. Otherwise a real
// franz-go client is built from the cluster spec and cleanup closes it.
func buildLiveClient(plan *pipeline.Plan, clusterConfigFiles []string) (kafka.AdminClient, func(), error) {
	noop := func() {}
	if plan.SelectedCluster == "" {
		return nil, noop, &ExitError{Code: 2, Msg: "multiple clusters loaded; use --cluster to select one"}
	}

	cl := plan.Clusters[plan.SelectedCluster]
	var stateFile string
	if cl != nil {
		stateFile = cl.Annotations[mockStateAnnotation]
	}
	if stateFile != "" {
		resolved := filepath.Join(baseDir(clusterConfigFiles), stateFile)
		c, err := mock.FromFile(resolved)
		if err != nil {
			return nil, noop, &ExitError{Code: 2, Msg: err.Error()}
		}
		return c, noop, nil
	}

	client, closeClient, err := cluster.BuildKafkaClient(cl, secrets.FileEnvResolver{BaseDir: baseDir(clusterConfigFiles)})
	if err != nil {
		return nil, noop, &ExitError{Code: 2, Msg: err.Error()}
	}
	return client, closeClient, nil
}

// buildSchemaClient resolves the Schema Registry client for the selected
// cluster, or (nil, nil) when the cluster has no schemaRegistry configured (no
// SR -> empty schema diff, which is fine because validation already gated
// declaring spec.schema against an SR-less cluster).
//
// Hermetic seam: if the selected KafkaCluster carries the mock-schema
// annotation the client is seeded from that file (path resolved like
// mock-state). Otherwise a real confluent client is built from the SR spec,
// resolving basic-auth credentials (if any) via internal/secrets relative to
// the cluster-config directory. Errors are returned plain; the caller wraps
// them as ExitError{2}.
func buildSchemaClient(plan *pipeline.Plan, clusterConfigFiles []string) (schemaregistry.Client, error) {
	if plan.SelectedCluster == "" {
		return nil, &ExitError{Code: 2, Msg: "multiple clusters loaded; use --cluster to select one"}
	}
	cl := plan.Clusters[plan.SelectedCluster]
	if cl == nil || cl.Spec.SchemaRegistry == nil {
		return nil, nil
	}

	base := baseDir(clusterConfigFiles)

	if sf := cl.Annotations[mockSchemaAnnotation]; sf != "" {
		resolved := filepath.Join(base, sf)
		return schemamock.FromFile(resolved)
	}

	return cluster.BuildSchemaClient(cl, secrets.FileEnvResolver{BaseDir: base})
}

// baseDir is the directory mock-state paths resolve against: the first
// --cluster-config-file arg used directly when it is a directory, else its
// parent directory.
func baseDir(clusterConfigFiles []string) string {
	if len(clusterConfigFiles) == 0 {
		return "."
	}
	first := clusterConfigFiles[0]
	if info, err := os.Stat(first); err == nil && info.IsDir() {
		return first
	}
	return filepath.Dir(first)
}

// buildMDSClient resolves the MDS client for the selected cluster, or (nil, nil)
// when the cluster has no authorization.mds configured and no mock annotation.
//
// Hermetic seam: if the selected KafkaCluster carries the mock-mds annotation
// the client is seeded from that file (path resolved like mock-state). Otherwise
// a real MDS client is built from the cluster spec, resolving credentials via
// internal/secrets relative to the cluster-config directory. Errors are returned
// plain; the caller wraps them as ExitError{2}.
func buildMDSClient(plan *pipeline.Plan, clusterConfigFiles []string) (mds.Client, error) {
	if plan.SelectedCluster == "" {
		return nil, &ExitError{Code: 2, Msg: "multiple clusters loaded; use --cluster to select one"}
	}
	cl := plan.Clusters[plan.SelectedCluster]

	base := baseDir(clusterConfigFiles)

	// Hermetic mock seam: annotation takes precedence over real client.
	if cl != nil {
		if mf := cl.Annotations[mockMDSAnnotation]; mf != "" {
			resolved := filepath.Join(base, mf)
			return mdsmock.FromFile(resolved)
		}
	}

	// No mock: build the real client (returns nil when MDS not configured).
	return cluster.BuildMDSClient(cl, secrets.FileEnvResolver{BaseDir: base})
}

// fromMDSRoleBindings converts mds.RoleBinding (wire type) to rbac.RoleBinding
// (engine type) for feeding diff.Live.RoleBindings. Attribution fields
// (Mode, Source*) are left zero — live bindings carry no owner attribution.
func fromMDSRoleBindings(in []mds.RoleBinding) []rbac.RoleBinding {
	out := make([]rbac.RoleBinding, 0, len(in))
	for _, rb := range in {
		b := rbac.RoleBinding{
			Principal: rb.Principal,
			Role:      rb.Role,
			Scope: rbac.Scope{
				Type:         rb.Scope.Type,
				KafkaCluster: rb.Scope.KafkaCluster,
				SubCluster:   rb.Scope.SubCluster,
			},
		}
		if rb.Resource != nil {
			b.Resource = &rbac.ResourcePattern{
				Type:        rb.Resource.Type,
				Name:        rb.Resource.Name,
				PatternType: rb.Resource.PatternType,
			}
		}
		out = append(out, b)
	}
	return out
}

// liveACLs converts the kafka.ACLState slice from an AdminClient into
// access.ACL for feeding diff.Live.ACLs (identical field shape).
func liveACLs(states []kafka.ACLState) []access.ACL {
	out := make([]access.ACL, 0, len(states))
	for _, s := range states {
		out = append(out, access.ACL{
			Principal:    s.Principal,
			Host:         s.Host,
			ResourceType: s.ResourceType,
			ResourceName: s.ResourceName,
			PatternType:  s.PatternType,
			Operation:    s.Operation,
			Permission:   s.Permission,
		})
	}
	return out
}
