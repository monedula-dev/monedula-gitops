package mock

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
)

func TestDescribeTopicConfigs(t *testing.T) {
	ctx := context.Background()
	c := New([]kafka.TopicState{
		{
			Name:              "t",
			Partitions:        1,
			ReplicationFactor: 1,
			Config: map[string]string{
				"retention.ms":   "1000",
				"cleanup.policy": "delete",
			},
		},
	}, nil)

	entries, err := c.DescribeTopicConfigs(ctx, "t")
	require.NoError(t, err)
	require.Equal(t, []kafka.ConfigEntry{
		{Name: "cleanup.policy", Value: "delete", Default: false},
		{Name: "retention.ms", Value: "1000", Default: false},
	}, entries)

	_, err = c.DescribeTopicConfigs(ctx, "absent")
	require.Error(t, err)
}
