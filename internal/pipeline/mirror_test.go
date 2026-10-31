package pipeline

import (
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/diff"
	"github.com/stretchr/testify/require"
)

func TestPipelineDesiredMirrors(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/mirror-orders.yaml"},
		ClusterConfigFiles: []string{"testdata/mirror-clusters.yaml"},
		Cluster:            "dr",
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Len(t, plan.MirrorTopics, 1)
	require.Equal(t, []diff.DesiredMirror{{
		Kind: "KafkaMirrorTopic", Name: "orders-dr", TopicName: "payments.orders",
		TopicNameExplicit: false, LinkName: "prod-to-dr", SourceTopicName: "payments.orders", Mode: "Enforce",
	}}, plan.DesiredMirrors)
	require.Equal(t, "", plan.MirrorTopics[0].Spec.TopicName, "defaulting must not write TopicName")
}

func TestPipelineClusterFilterAppliesToMirrors(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/mirror-orders.yaml"},
		ClusterConfigFiles: []string{"testdata/mirror-clusters.yaml"},
		Cluster:            "other",
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Empty(t, plan.MirrorTopics)
	require.Empty(t, plan.DesiredMirrors)
}
