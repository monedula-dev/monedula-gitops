// Package quota compiles KafkaQuota resources into Kafka client-quota entities
// and values, and computes the entity identity used for uniqueness checks.
package quota

import (
	"sort"
	"strings"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

func strptr(s string) *string { return &s }

// Component is one entity component: Type "user", "client-id", or "ip"; Name nil means
// the per-type default (Kafka null name).
type Component struct {
	Type string
	Name *string
}

// Entity is a normalized set of components.
type Entity []Component

// Key is a deterministic identity string: components sorted by type, a nil name
// rendered as <default>, joined with "|".
func (e Entity) Key() string {
	cp := append(Entity(nil), e...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Type < cp[j].Type })
	parts := make([]string, 0, len(cp))
	for _, c := range cp {
		name := "<default>"
		if c.Name != nil {
			name = *c.Name
		}
		parts = append(parts, c.Type+"="+name)
	}
	return strings.Join(parts, "|")
}

// Desired is a compiled quota: its entity, the limit values to set, and the
// owning resource's reconciliation mode (spec §16).
type Desired struct {
	Entity Entity
	Limits map[string]float64
	Mode   string
}

// Kafka quota config keys.
const (
	keyProducerByteRate       = "producer_byte_rate"
	keyConsumerByteRate       = "consumer_byte_rate"
	keyRequestPercentage      = "request_percentage"
	keyControllerMutationRate = "controller_mutation_rate"
	keyConnectionCreationRate = "connection_creation_rate"
)

// Compile builds the entity components and limit map from a KafkaQuota. The
// "User:" prefix on a user name is stripped for Kafka's quota API.
func Compile(q *v1alpha1.KafkaQuota) Desired {
	var e Entity
	switch {
	case q.Spec.Entity.User != "":
		e = append(e, Component{Type: "user", Name: strptr(strings.TrimPrefix(q.Spec.Entity.User, "User:"))})
	case q.Spec.Entity.UserDefault:
		e = append(e, Component{Type: "user", Name: nil})
	}
	switch {
	case q.Spec.Entity.ClientId != "":
		e = append(e, Component{Type: "client-id", Name: strptr(q.Spec.Entity.ClientId)})
	case q.Spec.Entity.ClientIdDefault:
		e = append(e, Component{Type: "client-id", Name: nil})
	}
	switch {
	case q.Spec.Entity.Ip != "":
		e = append(e, Component{Type: "ip", Name: strptr(q.Spec.Entity.Ip)})
	case q.Spec.Entity.IpDefault:
		e = append(e, Component{Type: "ip", Name: nil})
	}
	limits := map[string]float64{}
	if v := q.Spec.Limits.ProducerByteRate; v != nil {
		limits[keyProducerByteRate] = *v
	}
	if v := q.Spec.Limits.ConsumerByteRate; v != nil {
		limits[keyConsumerByteRate] = *v
	}
	if v := q.Spec.Limits.RequestPercentage; v != nil {
		limits[keyRequestPercentage] = *v
	}
	if v := q.Spec.Limits.ControllerMutationRate; v != nil {
		limits[keyControllerMutationRate] = *v
	}
	if v := q.Spec.Limits.ConnectionCreationRate; v != nil {
		limits[keyConnectionCreationRate] = *v
	}
	mode := ""
	if q.Spec.Reconciliation != nil {
		mode = q.Spec.Reconciliation.Mode
	}
	return Desired{Entity: e, Limits: limits, Mode: mode}
}
