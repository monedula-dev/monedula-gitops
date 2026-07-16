package v1alpha1

// This method lives in its own file, NOT in types.go, deliberately: types.go
// carries controller-gen CEL markers with a gofmt doc-comment reformatting
// hazard (see the MAINTAINER NOTE there), so it is edited as rarely as
// possible.

// ResolvedTopicName returns the effective Kafka topic name: spec.topicName
// when set, else metadata.name (the spec §4 default). It is THE single
// resolution of topic identity — compilers (access/rbac), the pipeline,
// validation, the importer, the admission webhook, and the controllers'
// duplicate-identity gate must all resolve the name through it so they can
// never disagree.
func (t *KafkaTopic) ResolvedTopicName() string {
	if t.Spec.TopicName != "" {
		return t.Spec.TopicName
	}
	return t.Name
}
