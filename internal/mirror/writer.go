package mirror

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
)

// Writer executes planned mirror writes. Plan time is not write time: each
// write first takes a fresh snapshot and re-checks the same rules the diff
// used (Classify, LinkActive, CheckPrefix, LinkManaged); if the live state no
// longer supports the planned write it returns a "pre-write check: " error
// and writes nothing. POSTs are never retried.
type Writer struct {
	Kafka kafka.AdminClient
	REST  kafkarest.Client
}

func preWrite(format string, args ...any) error {
	return fmt.Errorf("pre-write check: "+format, args...)
}

// freshLink reads a fresh snapshot scoped to op.MirrorLink and returns it with
// the link, failing when the snapshot is incomplete or the link is absent or
// its configs could not be read.
func (w Writer) freshLink(ctx context.Context, op operations.Operation) (Snapshot, LinkInfo, error) {
	s := ReadSnapshot(ctx, w.Kafka, w.REST, []string{op.MirrorLink})
	if !s.Complete() {
		return s, LinkInfo{}, preWrite("cannot read mirror state for %s: %w", op.Target, s.Err)
	}
	l, ok := s.Links[op.MirrorLink]
	if !ok {
		return s, LinkInfo{}, preWrite("link %q not found on the destination cluster", op.MirrorLink)
	}
	if l.ConfigErr != nil {
		return s, LinkInfo{}, preWrite("cannot read link %q configs: %w", op.MirrorLink, l.ConfigErr)
	}
	return s, l, nil
}

// checkKey fails unless the link's ownership of key is known and the key is
// not synced from the source by the link.
func checkKey(l LinkInfo, key string) error {
	managed, known := LinkManaged(l, key)
	if !known {
		return preWrite("cannot determine whether %s is link-managed: link %q does not report topic.config.sync.include", key, l.Link.Name)
	}
	if managed {
		return preWrite("config %s is link-managed by link %q", key, l.Link.Name)
	}
	return nil
}

// CreateMirror creates the mirror topic op.Target of op.MirrorSource on
// op.MirrorLink after re-verifying, on a fresh snapshot, that the topic is
// still Absent, the link is ACTIVE, the name satisfies the link prefix, and
// no declared config key is link-managed or undeterminable. A POST error is
// an uncertain outcome: it is returned wrapped and never retried.
func (w Writer) CreateMirror(ctx context.Context, op operations.Operation) error {
	s, l, err := w.freshLink(ctx, op)
	if err != nil {
		return err
	}
	if c := s.Classify(op.Target); c != Absent {
		return preWrite("topic %s is now %s, not Absent", op.Target, c)
	}
	if !LinkActive(l) {
		return preWrite("link %q is %s, not ACTIVE", op.MirrorLink, l.Link.State)
	}
	if err := CheckPrefix(l, op.Target, op.MirrorSource, op.MirrorNameExplicit); err != nil {
		return preWrite("%w", err)
	}
	keys := make([]string, 0, len(op.Config))
	for k := range op.Config {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := checkKey(l, k); err != nil {
			return err
		}
	}
	req := kafkarest.CreateMirrorRequest{
		LinkName:          op.MirrorLink,
		SourceTopicName:   op.MirrorSource,
		MirrorTopicName:   op.Target,
		ReplicationFactor: op.ReplicationFactor,
		Configs:           op.Config,
	}
	if err := w.REST.CreateMirror(ctx, req); err != nil {
		var se *kafkarest.StatusError
		if errors.As(err, &se) && se.Code >= 400 && se.Code < 500 {
			return fmt.Errorf("create mirror %s rejected by Kafka REST (%d): %w", op.Target, se.Code, err)
		}
		return fmt.Errorf("create mirror %s: outcome unknown, re-run diff before retrying: %w", op.Target, err)
	}
	return nil
}

// AlterMirrorConfig sets op.Field=op.To on the mirror topic op.Target after
// re-verifying, on a fresh snapshot, that it is still a live mirror of
// op.MirrorSource on op.MirrorLink and that the key is not link-managed.
func (w Writer) AlterMirrorConfig(ctx context.Context, op operations.Operation) error {
	s, l, err := w.freshLink(ctx, op)
	if err != nil {
		return err
	}
	if c := s.Classify(op.Target); c != Mirror {
		return preWrite("topic %s is now %s, not a live mirror", op.Target, c)
	}
	m := s.Mirrors[op.Target]
	if m.LinkName != op.MirrorLink || m.SourceTopicName != op.MirrorSource {
		return preWrite("topic %s is a mirror of %s on link %s; planned %s on %s",
			op.Target, m.SourceTopicName, m.LinkName, op.MirrorSource, op.MirrorLink)
	}
	if err := checkKey(l, op.Field); err != nil {
		return err
	}
	if err := w.REST.AlterTopicConfigs(ctx, op.Target, map[string]string{op.Field: op.To}); err != nil {
		return fmt.Errorf("alter mirror %s config %s: %w", op.Target, op.Field, err)
	}
	return nil
}
