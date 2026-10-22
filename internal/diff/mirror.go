package diff

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/monedula-dev/monedula-gitops/internal/mirror"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
)

// computeMirrorOps plans declared cluster-link mirror topics against the live
// destination snapshot. Only declared mirrors are considered: undeclared live
// mirrors are never reported or touched. Desired mirrors are processed in
// TopicName order and config keys in sorted order, so output is deterministic.
func computeMirrorOps(desired Desired, live Live) []operations.Operation {
	if len(desired.Mirrors) == 0 {
		return nil
	}
	mirrors := append([]DesiredMirror(nil), desired.Mirrors...)
	sort.SliceStable(mirrors, func(i, j int) bool { return mirrors[i].TopicName < mirrors[j].TopicName })

	var ops []operations.Operation
	for _, dm := range mirrors {
		ops = append(ops, mirrorOpsFor(dm, live.Mirrors)...)
	}
	return ops
}

func newMirrorOp(dm DesiredMirror, a operations.Action, msg string) operations.Operation {
	op := operations.New(a)
	op.Kind, op.Namespace, op.Name, op.Target = dm.Kind, dm.Namespace, dm.Name, dm.TopicName
	op.Mode = dm.Mode
	op.Message = msg
	return op
}

// mirrorOpsFor applies the decision table to one desired mirror; the first
// matching row wins, except that a live mirror in a healthy, progressing or
// unhealthy state continues to the replication-factor and config checks.
func mirrorOpsFor(dm DesiredMirror, s *mirror.Snapshot) []operations.Operation {
	one := func(a operations.Action, format string, args ...any) []operations.Operation {
		return []operations.Operation{newMirrorOp(dm, a, fmt.Sprintf(format, args...))}
	}

	if s == nil {
		return one(operations.StateReadError, "Kafka REST not configured; cannot read mirror state")
	}
	if !s.Complete() {
		reason := "snapshot not read"
		if s.Err != nil {
			reason = s.Err.Error()
		}
		return one(operations.StateReadError, "cannot read mirror state: %s", reason)
	}
	link, ok := s.Links[dm.LinkName]
	if !ok {
		return one(operations.Rejected, "link %q not found on the destination cluster", dm.LinkName)
	}
	if link.ConfigErr != nil {
		return one(operations.StateReadError, "cannot read link %q configs: %v", dm.LinkName, link.ConfigErr)
	}
	if err := mirror.CheckPrefix(link, dm.TopicName, dm.SourceTopicName, dm.TopicNameExplicit); err != nil {
		return one(operations.Rejected, "%s", err.Error())
	}

	keys := make([]string, 0, len(dm.Config))
	for k := range dm.Config {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	class := s.Classify(dm.TopicName)
	switch class {
	case mirror.Absent:
		if !mirror.LinkActive(link) {
			return one(operations.Rejected, "link %q is %s; not creating mirror", dm.LinkName, link.Link.State)
		}
		var rejected []operations.Operation
		for _, k := range keys {
			if msg := linkManagedRejection(link, k); msg != "" {
				op := newMirrorOp(dm, operations.Rejected, msg)
				op.Field, op.To = k, dm.Config[k]
				rejected = append(rejected, op)
			}
		}
		if len(rejected) > 0 {
			return rejected
		}
		op := newMirrorOp(dm, operations.CreateMirrorTopic, "")
		setMirrorIdentity(&op, dm)
		op.ReplicationFactor = dm.ReplicationFactor
		if dm.Config != nil {
			op.Config = make(map[string]string, len(dm.Config))
			for k, v := range dm.Config {
				op.Config[k] = v
			}
		}
		return []operations.Operation{op}
	case mirror.Normal:
		return one(operations.Rejected, "topic exists and is not a mirror")
	case mirror.StoppedFormerMirror:
		m := s.Mirrors[dm.TopicName]
		return []operations.Operation{
			newMirrorOp(dm, operations.Rejected, "topic exists and is not a mirror"),
			newMirrorOp(dm, operations.MirrorTopicPromoted, fmt.Sprintf(
				"mirror of %s on link %s was stopped (status %s); the topic is now a normal topic",
				m.SourceTopicName, m.LinkName, m.Status)),
		}
	case mirror.Mirror, mirror.StoppingMirror:
		// handled below
	default: // mirror.Unknown and anything unexpected
		return one(operations.StateReadError, "cannot classify topic %q: mirror status %q unrecognized or Kafka/REST state inconsistent",
			dm.TopicName, s.Mirrors[dm.TopicName].Status)
	}

	m := s.Mirrors[dm.TopicName]
	if m.LinkName != dm.LinkName || m.SourceTopicName != dm.SourceTopicName {
		return one(operations.Rejected, "mirror of %s on link %s; manifest declares %s on %s",
			m.SourceTopicName, m.LinkName, dm.SourceTopicName, dm.LinkName)
	}
	if class == mirror.StoppingMirror {
		return one(operations.MirrorTopicStopping, "mirror is stopping (status %s)", m.Status)
	}

	var ops []operations.Operation
	switch mirror.ClassifyStatus(m.Status) {
	case mirror.StatusUnhealthy:
		ops = append(ops, newMirrorOp(dm, operations.MirrorTopicUnhealthy,
			fmt.Sprintf("mirror is unhealthy (status %s)", m.Status)))
	case mirror.StatusProgressing:
		ops = append(ops, newMirrorOp(dm, operations.MirrorTopicProgressing,
			fmt.Sprintf("mirror is still being set up (status %s)", m.Status)))
	}

	lt := s.Topics[dm.TopicName]
	if dm.ReplicationFactor > 0 && dm.ReplicationFactor != lt.ReplicationFactor {
		op := newMirrorOp(dm, operations.UnsupportedReplicationFactorChange, fmt.Sprintf(
			"replicationFactor is create-time only for mirror topics: live %d, declared %d",
			lt.ReplicationFactor, dm.ReplicationFactor))
		op.From = strconv.Itoa(lt.ReplicationFactor)
		op.To = strconv.Itoa(dm.ReplicationFactor)
		ops = append(ops, op)
	}

	for _, k := range keys {
		want := dm.Config[k]
		got, present := lt.Config[k]
		// A link-managed or undeterminable key is never declarable on a
		// mirror, whether or not its live value currently matches.
		if msg := linkManagedRejection(link, k); msg != "" {
			op := newMirrorOp(dm, operations.Rejected, msg)
			op.Field, op.From, op.To = k, got, want
			ops = append(ops, op)
			continue
		}
		if present && got == want {
			continue
		}
		// From is "" when the key is absent live, like UpdateTopicConfig.
		shown := got
		if !present {
			shown = "<unset>"
		}
		op := newMirrorOp(dm, operations.UpdateMirrorTopicConfig, fmt.Sprintf("config %s: %s -> %s", k, shown, want))
		setMirrorIdentity(&op, dm)
		op.Field, op.From, op.To = k, got, want
		ops = append(ops, op)
	}
	return ops
}

// setMirrorIdentity stamps the mirror identity payload the writer re-verifies
// (link, source, and whether topicName was explicit for CheckPrefix).
func setMirrorIdentity(op *operations.Operation, dm DesiredMirror) {
	op.MirrorLink = dm.LinkName
	op.MirrorSource = dm.SourceTopicName
	op.MirrorNameExplicit = dm.TopicNameExplicit
}

// linkManagedRejection returns the Rejected message for a declared config key
// the link syncs from the source (or whose ownership cannot be determined), or
// "" when the key may be set on the mirror.
func linkManagedRejection(link mirror.LinkInfo, key string) string {
	managed, known := mirror.LinkManaged(link, key)
	if !known {
		return fmt.Sprintf("cannot determine whether %s is link-managed: link %q does not report topic.config.sync.include", key, link.Link.Name)
	}
	if managed {
		return fmt.Sprintf("config %s is link-managed (synced from the source by link %q) and cannot be set on the mirror", key, link.Link.Name)
	}
	return ""
}
