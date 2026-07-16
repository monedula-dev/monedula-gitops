// Package defaulting applies defaults to loaded resources so downstream code
// (validation, compilation) never has to special-case omitted fields.
package defaulting

import "github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"

// placementConstraintsKey is the Confluent topic config that drives replica
// placement; it is mutually exclusive with an explicit replication factor.
const placementConstraintsKey = "confluent.placement.constraints"

// Topic applies defaults to a KafkaTopic. clusterDefaults may be nil.
func Topic(tp *v1alpha1.KafkaTopic, clusterDefaults *v1alpha1.ClusterDefaults) {
	if tp.Spec.TopicName == "" {
		tp.Spec.TopicName = tp.Name
	}
	if tp.Spec.DeletionPolicy == "" {
		if clusterDefaults != nil && clusterDefaults.TopicDeletionPolicy != "" {
			tp.Spec.DeletionPolicy = clusterDefaults.TopicDeletionPolicy
		} else {
			tp.Spec.DeletionPolicy = "Orphan"
		}
	}
	if tp.Spec.Reconciliation == nil {
		tp.Spec.Reconciliation = &v1alpha1.Reconciliation{}
	}
	if tp.Spec.Reconciliation.Mode == "" {
		tp.Spec.Reconciliation.Mode = "Enforce"
	}
	// Apply the cluster-level default replication factor ONLY when the topic both
	// omits its own and does not configure Confluent replica-placement
	// constraints. Replication factor is mutually exclusive with placement
	// constraints, so a placement-constrained topic must keep RF unset (the broker
	// derives replication from the constraint). This guarantees there is always a
	// way to NOT set RF: omit spec.replicationFactor (and, if a cluster default
	// exists, declare a placement constraint or simply leave it -- an omitted RF
	// is sent to Kafka as -1, "use the broker default / placement").
	_, hasPlacement := tp.Spec.Config[placementConstraintsKey]
	if tp.Spec.ReplicationFactor == nil && !hasPlacement && clusterDefaults != nil && clusterDefaults.ReplicationFactor != nil {
		// Allocate a new int and take its address; never alias the cluster's
		// pointer, otherwise mutating one topic could affect another.
		rf := *clusterDefaults.ReplicationFactor
		tp.Spec.ReplicationFactor = &rf
	}
}

// Policy applies defaults to a KafkaAccessPolicy.
func Policy(pol *v1alpha1.KafkaAccessPolicy) {
	if pol.Spec.DeletionPolicy == "" {
		pol.Spec.DeletionPolicy = "Delete"
	}
	if pol.Spec.Reconciliation == nil {
		pol.Spec.Reconciliation = &v1alpha1.Reconciliation{}
	}
	if pol.Spec.Reconciliation.Mode == "" {
		pol.Spec.Reconciliation.Mode = "Enforce"
	}
	for i := range pol.Spec.Rules {
		if pol.Spec.Rules[i].Permission == "" {
			pol.Spec.Rules[i].Permission = "Allow"
		}
		if pol.Spec.Rules[i].Host == "" {
			pol.Spec.Rules[i].Host = "*"
		}
		if pol.Spec.Rules[i].Resource.PatternType == "" {
			pol.Spec.Rules[i].Resource.PatternType = "literal"
		}
	}
}

// Cluster applies defaults to a KafkaCluster. No-op in v0.1; cluster defaulting
// is reserved for later. Kept so callers have a uniform API.
func Cluster(cl *v1alpha1.KafkaCluster) {
}

// User applies defaults to a KafkaUser.
func User(u *v1alpha1.KafkaUser) {
	if u.Spec.Username == "" {
		u.Spec.Username = u.Name
	}
	if u.Spec.Mechanism == "" {
		u.Spec.Mechanism = "SCRAM-SHA-512"
	}
	if u.Spec.DeletionPolicy == "" {
		u.Spec.DeletionPolicy = "Delete"
	}
}

// RoleBinding applies defaults to a KafkaRoleBinding. DeletionPolicy defaults
// to Delete: mirroring KafkaAccessPolicy and KafkaUser, the compiled MDS role
// bindings are this CR's entire reason to exist, so removing the CR removes
// them by default. Set deletionPolicy: Orphan explicitly to keep them.
func RoleBinding(rb *v1alpha1.KafkaRoleBinding) {
	if rb.Spec.DeletionPolicy == "" {
		rb.Spec.DeletionPolicy = "Delete"
	}
}
