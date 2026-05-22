package extpubsub

import (
	"gocloud.dev/blob"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/driver"
)

// NewTopic creates a new *pubsub.Topic that offloads large messages to the bucket.
// It wraps the provided base driver.
func NewTopic(base driver.Topic, bucket *blob.Bucket, opts Options) *pubsub.Topic {
	opts.SetDefaults()
	d := newTopic(base, bucket, opts)
	return pubsub.NewTopic(d, nil)
}

// NewSubscription creates a new *pubsub.Subscription that unrolls messages from the bucket.
// It wraps the provided base driver.
func NewSubscription(base driver.Subscription, bucket *blob.Bucket, opts Options) *pubsub.Subscription {
	opts.SetDefaults()
	d := newSubscription(base, bucket, opts)
	return pubsub.NewSubscription(d, nil, nil)
}
