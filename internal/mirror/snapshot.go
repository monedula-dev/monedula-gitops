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
	read                          bool // set only by NewSnapshot; zero-value snapshots fail closed
}

// NewSnapshot builds a populated snapshot without I/O, applying the same
// identity rules as ReadSnapshot's success path. Nil maps become empty.
func NewSnapshot(adminClusterID, restClusterID string, topics map[string]kafka.TopicState, mirrors map[string]kafkarest.Mirror, links map[string]LinkInfo) Snapshot {
	s := Snapshot{AdminClusterID: adminClusterID, RESTClusterID: restClusterID}
	if err := bindClusterIDs(adminClusterID, nil, restClusterID, nil); err != nil {
		s.Err = err
		return s
	}
	if topics == nil {
		topics = map[string]kafka.TopicState{}
	}
	if mirrors == nil {
		mirrors = map[string]kafkarest.Mirror{}
	}
	if links == nil {
		links = map[string]LinkInfo{}
	}
	s.Topics, s.Mirrors, s.Links = topics, mirrors, links
	s.read = true
	return s
}

// Complete reports whether all required reads succeeded.
func (s Snapshot) Complete() bool { return s.read && s.Err == nil }

// ReadSnapshot reads Kafka and REST state. A linkName absent from the link list
// gets no LinkInfo entry.
func ReadSnapshot(ctx context.Context, k kafka.AdminClient, r kafkarest.Client, linkNames []string) Snapshot {
	var s Snapshot
	aid, aerr := k.ClusterID(ctx)
	rid, rerr := r.ClusterID(ctx)
	s.AdminClusterID, s.RESTClusterID = aid, rid
	if err := bindClusterIDs(aid, aerr, rid, rerr); err != nil {
		s.Err = err
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
	tm := make(map[string]kafka.TopicState, len(topics))
	for _, t := range topics {
		tm[t.Name] = t
	}
	mm, err := indexMirrors(mirrors)
	if err != nil {
		s.Err = err
		return s
	}
	byName := make(map[string]kafkarest.Link, len(links))
	for _, l := range links {
		if _, dup := byName[l.Name]; dup {
			s.Err = fmt.Errorf("duplicate link name %q in REST response", l.Name)
			return s
		}
		byName[l.Name] = l
	}
	lm := map[string]LinkInfo{}
	for _, n := range linkNames {
		l, ok := byName[n]
		if !ok {
			continue
		}
		cfg, cerr := r.LinkConfigs(ctx, n)
		lm[n] = LinkInfo{Link: l, Configs: cfg, ConfigErr: cerr}
	}
	return NewSnapshot(aid, rid, tm, mm, lm)
}

// bindClusterIDs is the single cluster-identity binding rule: both IDs read
// without error, non-empty and equal. Otherwise it returns an error wrapping
// ErrClusterIdentityMismatch.
func bindClusterIDs(adminID string, adminErr error, restID string, restErr error) error {
	if adminErr == nil && restErr == nil && adminID != "" && restID != "" && adminID == restID {
		return nil
	}
	return fmt.Errorf("%w: kafka admin cluster id %q%s, rest cluster id %q%s",
		ErrClusterIdentityMismatch, adminID, errSuffix(adminErr), restID, errSuffix(restErr))
}

// errSuffix renders a read error for the binding message, or nothing when the
// read succeeded.
func errSuffix(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf(" (err: %v)", err)
}

// indexMirrors keys a complete ListMirrors result by mirror topic name. A
// duplicate name is contradictory evidence and fails the whole read.
func indexMirrors(mirrors []kafkarest.Mirror) (map[string]kafkarest.Mirror, error) {
	mm := make(map[string]kafkarest.Mirror, len(mirrors))
	for _, m := range mirrors {
		if _, dup := mm[m.MirrorTopicName]; dup {
			return nil, fmt.Errorf("duplicate mirror topic name %q in REST response", m.MirrorTopicName)
		}
		mm[m.MirrorTopicName] = m
	}
	return mm, nil
}

// Classify returns the ownership class of a destination topic name.
func (s Snapshot) Classify(topic string) Classification {
	if !s.Complete() {
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
