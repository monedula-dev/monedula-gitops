package e2e

import (
	"context"
	"fmt"

	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
)

// MirrorExpect asserts a mirror topic exists on the live cluster and, for each
// non-empty field, matches. Name is the mirror topic name on the destination.
type MirrorExpect struct {
	Name        string `json:"name"`
	LinkName    string `json:"linkName,omitempty"`
	SourceTopic string `json:"sourceTopic,omitempty"`
	Status      string `json:"status,omitempty"`
}

// MirrorProber lists mirror topics through Kafka REST. kafkarest.Client
// satisfies it.
type MirrorProber interface {
	ListMirrors(ctx context.Context) ([]kafkarest.Mirror, error)
}

// CheckMirrors asserts the wanted mirrors exist (subset-matched) and the absent
// names are not listed. A read error fails every expectation.
func CheckMirrors(ctx context.Context, p MirrorProber, want []MirrorExpect, absent []string) Report {
	var rep Report
	live, err := p.ListMirrors(ctx)
	if err != nil {
		for _, w := range want {
			rep.Add(CheckResult{Name: "mirror " + w.Name, Detail: fmt.Sprintf("listing mirrors: %v", err)})
		}
		for _, a := range absent {
			rep.Add(CheckResult{Name: "mirror " + a + " absent", Detail: fmt.Sprintf("listing mirrors: %v", err)})
		}
		return rep
	}
	byName := make(map[string]kafkarest.Mirror, len(live))
	for _, m := range live {
		byName[m.MirrorTopicName] = m
	}
	for _, w := range want {
		name := "mirror " + w.Name
		m, ok := byName[w.Name]
		if !ok {
			rep.Add(CheckResult{Name: name, Detail: "mirror topic not found"})
			continue
		}
		var diffs []string
		if w.LinkName != "" && m.LinkName != w.LinkName {
			diffs = append(diffs, fmt.Sprintf("linkName: want %q, live %q", w.LinkName, m.LinkName))
		}
		if w.SourceTopic != "" && m.SourceTopicName != w.SourceTopic {
			diffs = append(diffs, fmt.Sprintf("sourceTopic: want %q, live %q", w.SourceTopic, m.SourceTopicName))
		}
		if w.Status != "" && m.Status != w.Status {
			diffs = append(diffs, fmt.Sprintf("status: want %q, live %q", w.Status, m.Status))
		}
		if len(diffs) > 0 {
			rep.Add(CheckResult{Name: name, Detail: fmt.Sprint(diffs)})
			continue
		}
		rep.Add(CheckResult{Name: name, Pass: true})
	}
	for _, a := range absent {
		name := "mirror " + a + " absent"
		if _, ok := byName[a]; ok {
			rep.Add(CheckResult{Name: name, Detail: "mirror topic is present"})
			continue
		}
		rep.Add(CheckResult{Name: name, Pass: true})
	}
	return rep
}
