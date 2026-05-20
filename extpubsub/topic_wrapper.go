package extpubsub

import (
	"gocloud.dev/pubsub"
)

// Topic wraps *pubsub.Topic.
type Topic struct {
	*pubsub.Topic
}

// WrapTopic wraps an existing *pubsub.Topic.
func WrapTopic(t *pubsub.Topic) *Topic {
	return &Topic{Topic: t}
}
