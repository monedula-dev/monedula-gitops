package e2e

import (
	"context"
	"fmt"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
)

// LiveProber reads real cluster state for the liveState surface (topics, ACLs,
// quotas, SCRAM users). Backed by kafka.AdminClient in production and
// kafka/mock in tests.
type LiveProber interface {
	// TopicConfig returns whether the topic exists and, if so, its full config
	// map. exists=false means the topic is absent (not an error).
	TopicConfig(ctx context.Context, name string) (exists bool, config map[string]string, err error)
	ACLs(ctx context.Context) ([]kafka.ACLState, error)
	Quotas(ctx context.Context) ([]kafka.QuotaState, error)
	// Users returns the observable SCRAM credential identities for the given
	// usernames (all credentialed users when usernames is empty). Mirrors
	// kafka.AdminClient.ListScramCredentials.
	Users(ctx context.Context, usernames ...string) ([]kafka.ScramCredential, error)
}

// kafkaProber adapts a kafka.AdminClient to LiveProber.
type kafkaProber struct{ admin kafka.AdminClient }

// NewKafkaProber wraps an AdminClient as a LiveProber.
func NewKafkaProber(admin kafka.AdminClient) LiveProber { return &kafkaProber{admin: admin} }

func (p *kafkaProber) ACLs(ctx context.Context) ([]kafka.ACLState, error) {
	return p.admin.ListACLs(ctx)
}

func (p *kafkaProber) Quotas(ctx context.Context) ([]kafka.QuotaState, error) {
	return p.admin.ListQuotas(ctx)
}

func (p *kafkaProber) Users(ctx context.Context, usernames ...string) ([]kafka.ScramCredential, error) {
	return p.admin.ListScramCredentials(ctx, usernames...)
}

func (p *kafkaProber) TopicConfig(ctx context.Context, name string) (bool, map[string]string, error) {
	ts, err := p.admin.GetTopic(ctx, name)
	if err != nil {
		return false, nil, err
	}
	if ts == nil {
		return false, nil, nil
	}
	entries, err := p.admin.DescribeTopicConfigs(ctx, name)
	if err != nil {
		return true, nil, err
	}
	cfg := make(map[string]string, len(entries))
	for _, e := range entries {
		cfg[e.Name] = e.Value
	}
	return true, cfg, nil
}

// CheckLiveState asserts every topic in ls exists with the expected config
// subset (only the listed config keys are checked). A probe error is recorded
// as a failed check (not a panic).
func CheckLiveState(ctx context.Context, p LiveProber, ls LiveState) Report {
	var rep Report
	for _, te := range ls.Topics {
		exists, cfg, err := p.TopicConfig(ctx, te.Name)
		if err != nil {
			rep.Add(CheckResult{Name: fmt.Sprintf("topic %s", te.Name), Pass: false,
				Detail: fmt.Sprintf("probe error: %v", err)})
			continue
		}
		if !exists {
			rep.Add(CheckResult{Name: fmt.Sprintf("topic %s exists", te.Name), Pass: false,
				Detail: fmt.Sprintf("topic %q not found on cluster", te.Name)})
			continue
		}
		rep.Add(CheckResult{Name: fmt.Sprintf("topic %s exists", te.Name), Pass: true})
		for k, want := range te.Config {
			got, ok := cfg[k]
			pass := ok && got == want
			detail := ""
			if !pass {
				detail = fmt.Sprintf("expected %q, got %q (present=%v)", want, got, ok)
			}
			rep.Add(CheckResult{
				Name:   fmt.Sprintf("topic %s config %s=%s", te.Name, k, want),
				Pass:   pass,
				Detail: detail,
			})
		}
	}
	if len(ls.ACLs) > 0 {
		live, err := p.ACLs(ctx)
		if err != nil {
			rep.Add(CheckResult{Name: "acls", Pass: false, Detail: fmt.Sprintf("probe error: %v", err)})
		} else {
			for _, want := range ls.ACLs {
				rep.Add(checkOneACL(want, live))
			}
		}
	}
	if len(ls.Quotas) > 0 {
		live, err := p.Quotas(ctx)
		if err != nil {
			rep.Add(CheckResult{Name: "quotas", Pass: false, Detail: fmt.Sprintf("probe error: %v", err)})
		} else {
			for _, want := range ls.Quotas {
				rep.Add(checkOneQuota(want, live))
			}
		}
	}
	if len(ls.Users) > 0 {
		usernames := make([]string, 0, len(ls.Users))
		for _, u := range ls.Users {
			usernames = append(usernames, u.Username)
		}
		live, err := p.Users(ctx, usernames...)
		if err != nil {
			rep.Add(CheckResult{Name: "users", Pass: false, Detail: fmt.Sprintf("probe error: %v", err)})
		} else {
			for _, want := range ls.Users {
				rep.Add(checkOneUser(want, live))
			}
		}
	}
	if ls.Absent != nil {
		for _, name := range ls.Absent.Topics {
			exists, _, err := p.TopicConfig(ctx, name)
			if err != nil {
				rep.Add(CheckResult{Name: fmt.Sprintf("topic %s absent", name), Pass: false,
					Detail: fmt.Sprintf("probe error: %v", err)})
				continue
			}
			if exists {
				rep.Add(CheckResult{Name: fmt.Sprintf("topic %s absent", name), Pass: false,
					Detail: fmt.Sprintf("topic %q still present on cluster", name)})
				continue
			}
			rep.Add(CheckResult{Name: fmt.Sprintf("topic %s absent", name), Pass: true})
		}
		for _, want := range ls.Absent.Users {
			live, err := p.Users(ctx, want.Username)
			name := fmt.Sprintf("user %s/%s absent", want.Username, want.Mechanism)
			if err != nil {
				rep.Add(CheckResult{Name: name, Pass: false, Detail: fmt.Sprintf("probe error: %v", err)})
				continue
			}
			found := false
			for _, c := range live {
				if c.User == want.Username && c.Mechanism == want.Mechanism {
					found = true
					break
				}
			}
			if found {
				rep.Add(CheckResult{Name: name, Pass: false,
					Detail: fmt.Sprintf("credential (%s, %s) still present on cluster", want.Username, want.Mechanism)})
				continue
			}
			rep.Add(CheckResult{Name: name, Pass: true})
		}
	}
	return rep
}

// aclDefault applies the terse-YAML defaults (patternType=literal, permission=Allow, host=*).
func aclDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func checkOneACL(want ACLExpect, live []kafka.ACLState) CheckResult {
	pt := aclDefault(want.PatternType, "literal")
	perm := aclDefault(want.Permission, "Allow")
	host := aclDefault(want.Host, "*")
	name := fmt.Sprintf("acl %s %s %s:%s (%s)", want.Principal, want.Operation, want.ResourceType, want.ResourceName, perm)
	for _, a := range live {
		if a.Principal == want.Principal && a.Operation == want.Operation &&
			a.ResourceType == want.ResourceType && a.ResourceName == want.ResourceName &&
			a.PatternType == pt && a.Permission == perm && a.Host == host {
			return CheckResult{Name: name, Pass: true}
		}
	}
	return CheckResult{Name: name, Pass: false,
		Detail: fmt.Sprintf("no live ACL matched principal=%s op=%s %s:%s pattern=%s perm=%s host=%s",
			want.Principal, want.Operation, want.ResourceType, want.ResourceName, pt, perm, host)}
}

// quotaEntityMatches reports whether a live entity matches the expected one-of
// user/clientId/ip (or the default entity when all are empty).
func quotaEntityMatches(want QuotaEntityExpect, live []kafka.QuotaEntityComponent) bool {
	for _, c := range live {
		var wantName string
		switch c.Type {
		case "user":
			wantName = want.User
		case "client-id":
			wantName = want.ClientID
		case "ip":
			wantName = want.IP
		default:
			continue
		}
		if wantName == "" {
			continue
		}
		if c.Name != nil && *c.Name == wantName {
			return true
		}
	}
	return false
}

// checkOneUser asserts a live SCRAM credential exists matching want's
// (Username, Mechanism), and — when want.Iterations is non-zero — that its
// iteration count also matches. Iterations == 0 means "don't care" (mirrors
// the KafkaUser spec's own nil-Iterations-means-unset convention).
func checkOneUser(want UserExpect, live []kafka.ScramCredential) CheckResult {
	name := fmt.Sprintf("user %s/%s", want.Username, want.Mechanism)
	for _, c := range live {
		if c.User != want.Username || c.Mechanism != want.Mechanism {
			continue
		}
		if want.Iterations != 0 && c.Iterations != want.Iterations {
			return CheckResult{Name: name, Pass: false,
				Detail: fmt.Sprintf("credential found but iterations differ: want %d, live %d", want.Iterations, c.Iterations)}
		}
		return CheckResult{Name: name, Pass: true}
	}
	return CheckResult{Name: name, Pass: false,
		Detail: fmt.Sprintf("no live SCRAM credential matched username=%s mechanism=%s", want.Username, want.Mechanism)}
}

func checkOneQuota(want QuotaExpect, live []kafka.QuotaState) CheckResult {
	label := want.Entity.User + want.Entity.ClientID + want.Entity.IP
	name := fmt.Sprintf("quota %s", label)
	for _, q := range live {
		if !quotaEntityMatches(want.Entity, q.Entity) {
			continue
		}
		allOK := true
		for k, v := range want.Limits {
			if got, ok := q.Limits[k]; !ok || got != v {
				allOK = false
				break
			}
		}
		if allOK {
			return CheckResult{Name: name, Pass: true}
		}
		return CheckResult{Name: name, Pass: false,
			Detail: fmt.Sprintf("entity matched but limits differ: want %v, live %v", want.Limits, q.Limits)}
	}
	return CheckResult{Name: name, Pass: false, Detail: fmt.Sprintf("no live quota entity matched %s", label)}
}
