package mirror

import (
	"context"
	"fmt"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
)

// VerifyTopic classifies one destination topic from fresh reads: both cluster
// IDs (bound with the same rules as NewSnapshot), the exact topic through
// k.GetTopic, and the complete all-links mirror list. It never lists all
// topics or fetches link configs, so its cost does not scale with the
// cluster's topic count. Any read or binding failure returns (Unknown, err);
// Unknown with a nil error means the reads succeeded but the evidence is
// contradictory (e.g. a mirror entry without its Kafka topic).
func VerifyTopic(ctx context.Context, k kafka.AdminClient, r kafkarest.Client, topic string) (Classification, error) {
	aid, aerr := k.ClusterID(ctx)
	rid, rerr := r.ClusterID(ctx)
	if err := bindClusterIDs(aid, aerr, rid, rerr); err != nil {
		return Unknown, err
	}
	t, err := k.GetTopic(ctx, topic)
	if err != nil {
		return Unknown, fmt.Errorf("get topic %q: %w", topic, err)
	}
	mirrors, err := r.ListMirrors(ctx)
	if err != nil {
		return Unknown, fmt.Errorf("list mirrors: %w", err)
	}
	mm, err := indexMirrors(mirrors)
	if err != nil {
		return Unknown, err
	}
	topics := map[string]kafka.TopicState{}
	if t != nil {
		topics[topic] = *t
	}
	s := NewSnapshot(aid, rid, topics, mm, nil)
	if s.Err != nil {
		return Unknown, s.Err
	}
	return s.Classify(topic), nil
}
