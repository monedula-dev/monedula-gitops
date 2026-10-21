package mirror

import (
	"context"
	"errors"
	"fmt"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
)

// Classification is the ownership class of a destination topic.
type Classification string

const (
	Absent              Classification = "Absent"
	Normal              Classification = "Normal"
	Mirror              Classification = "Mirror"
	StoppingMirror      Classification = "StoppingMirror"
	StoppedFormerMirror Classification = "StoppedFormerMirror"
	Unknown             Classification = "Unknown"
)

// ErrClusterIdentityMismatch means the Kafka Admin and REST endpoints could not
// be proven to address the same cluster.
var ErrClusterIdentityMismatch = errors.New("ClusterIdentityMismatch")

// LinkInfo is a cluster link and its configs. ConfigErr records a per-link
// config read failure without invalidating the snapshot.
type LinkInfo struct {
	Link      kafkarest.Link
	Configs   map[string]string
	ConfigErr error
}

// Snapshot is the live destination state. Mirrors is keyed by mirror topic name.
// Err != nil means state is incomplete and every Classify returns Unknown.
type Snapshot struct {
	AdminClusterID, RESTClusterID string
	Topics                        map[string]kafka.TopicState
	Mirrors                       map[string]kafkarest.Mirror
	Links                         map[string]LinkInfo
	Err                           error
}

// Complete reports whether all required reads succeeded.
func (s Snapshot) Complete() bool { return s.Err == nil }

// ReadSnapshot reads Kafka and REST state. A linkName absent from the link list
// gets no LinkInfo entry.
func ReadSnapshot(ctx context.Context, k kafka.AdminClient, r kafkarest.Client, linkNames []string) Snapshot {
	var s Snapshot
	aid, aerr := k.ClusterID(ctx)
	rid, rerr := r.ClusterID(ctx)
	s.AdminClusterID, s.RESTClusterID = aid, rid
	if aerr != nil || rerr != nil || aid == "" || rid == "" || aid != rid {
		s.Err = fmt.Errorf("%w: kafka admin cluster id %q (err: %v), rest cluster id %q (err: %v)",
			ErrClusterIdentityMismatch, aid, aerr, rid, rerr)
		return s
	}
	topics, err := k.ListTopics(ctx)
	if err != nil {
		s.Err = fmt.Errorf("list topics: %w", err)
		return s
	}
	mirrors, err := r.ListMirrors(ctx)
	if err != nil {
		s.Err = fmt.Errorf("list mirrors: %w", err)
		return s
	}
	links, err := r.ListLinks(ctx)
	if err != nil {
		s.Err = fmt.Errorf("list links: %w", err)
		return s
	}
	s.Topics = make(map[string]kafka.TopicState, len(topics))
	for _, t := range topics {
		s.Topics[t.Name] = t
	}
	s.Mirrors = make(map[string]kafkarest.Mirror, len(mirrors))
	for _, m := range mirrors {
		s.Mirrors[m.MirrorTopicName] = m
	}
	byName := make(map[string]kafkarest.Link, len(links))
	for _, l := range links {
		byName[l.Name] = l
	}
	s.Links = map[string]LinkInfo{}
	for _, n := range linkNames {
		l, ok := byName[n]
		if !ok {
			continue
		}
		cfg, cerr := r.LinkConfigs(ctx, n)
		s.Links[n] = LinkInfo{Link: l, Configs: cfg, ConfigErr: cerr}
	}
	return s
}

// Classify returns the ownership class of a destination topic name.
func (s Snapshot) Classify(topic string) Classification {
	if s.Err != nil {
		return Unknown
	}
	_, hasTopic := s.Topics[topic]
	m, hasMirror := s.Mirrors[topic]
	if !hasMirror {
		if hasTopic {
			return Normal
		}
		return Absent
	}
	// Kafka/REST inconsistency: a mirror entry without its topic.
	if !hasTopic {
		return Unknown
	}
	switch ClassifyStatus(m.Status) {
	case StatusHealthy, StatusUnhealthy, StatusProgressing:
		return Mirror
	case StatusStopping:
		return StoppingMirror
	case StatusStopped:
		return StoppedFormerMirror
	}
	return Unknown
}
