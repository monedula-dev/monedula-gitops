package mirror

import (
	"context"
	"fmt"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
)

// ReadTopicSnapshot reads the state needed to classify one destination topic:
// both cluster IDs (bound with the same rules as NewSnapshot), the exact topic
// through k.GetTopic, and the complete all-links mirror list. It never lists
// all topics or fetches links or link configs, so its cost does not scale with
// the cluster's topic count.
//
// The returned snapshot is only valid for classifying topic: its Topics map
// holds at most that one entry, so any other name would classify as Absent
// (or Unknown when it has a mirror entry). Any read or binding failure returns
// a snapshot with Err set, which classifies every name as Unknown.
func ReadTopicSnapshot(ctx context.Context, k kafka.AdminClient, r kafkarest.Client, topic string) Snapshot {
	var s Snapshot
	aid, aerr := k.ClusterID(ctx)
	rid, rerr := r.ClusterID(ctx)
	s.AdminClusterID, s.RESTClusterID = aid, rid
	if err := bindClusterIDs(aid, aerr, rid, rerr); err != nil {
		s.Err = err
		return s
	}
	t, err := k.GetTopic(ctx, topic)
	if err != nil {
		s.Err = fmt.Errorf("get topic %q: %w", topic, err)
		return s
	}
	mirrors, err := r.ListMirrors(ctx)
	if err != nil {
		s.Err = fmt.Errorf("list mirrors: %w", err)
		return s
	}
	mm, err := indexMirrors(mirrors)
	if err != nil {
		s.Err = err
		return s
	}
	topics := map[string]kafka.TopicState{}
	if t != nil {
		topics[topic] = *t
	}
	return NewSnapshot(aid, rid, topics, mm, nil)
}

// VerifyTopic classifies one destination topic from a fresh ReadTopicSnapshot.
// Any read or binding failure returns (Unknown, err); Unknown with a nil error
// means the reads succeeded but the evidence is contradictory (e.g. a mirror
// entry without its Kafka topic).
func VerifyTopic(ctx context.Context, k kafka.AdminClient, r kafkarest.Client, topic string) (Classification, error) {
	s := ReadTopicSnapshot(ctx, k, r, topic)
	if s.Err != nil {
		return Unknown, s.Err
	}
	return s.Classify(topic), nil
}
