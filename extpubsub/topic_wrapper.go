package extpubsub

import (
	"context"

	"gocloud.dev/blob"
	"gocloud.dev/pubsub"
)

// Topic wraps *pubsub.Topic to offload large messages to a blob bucket,
// sending a lightweight control message in their place.
//
// Unlike the driver-level NewTopic, which offloads a whole driver batch into a
// single blob, Topic operates at the *pubsub.Topic level and therefore offloads
// one message at a time: each Send writes one blob and emits one control message.
type Topic struct {
	*pubsub.Topic

	bucket *blob.Bucket
	opts   Options
}

// WrapTopic wraps an existing *pubsub.Topic so that messages whose body is at
// least opts.MinSize bytes are offloaded to b and replaced with a control
// message. Messages smaller than opts.MinSize are sent through unchanged.
// It assumes the receiving side uses the same bucket and options.
func WrapTopic(t *pubsub.Topic, b *blob.Bucket, opts Options) *Topic {
	opts.SetDefaults()
	return &Topic{
		Topic:  t,
		bucket: b,
		opts:   opts,
	}
}

// Send offloads the message to the bucket and publishes a control message,
// or passes the message through unchanged when it is below opts.MinSize.
func (t *Topic) Send(ctx context.Context, m *pubsub.Message) error {
	if t.opts.MinSize > 0 && len(m.Body) < t.opts.MinSize {
		return t.Topic.Send(ctx, m)
	}

	metadata, err := offload(ctx, t.bucket, t.opts, []*Message{{Body: m.Body, Metadata: m.Metadata}})
	if err != nil {
		return err
	}

	return t.Topic.Send(ctx, &pubsub.Message{Metadata: metadata})
}
