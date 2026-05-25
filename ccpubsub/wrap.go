package ccpubsub

import (
	"context"

	"gocloud.dev/blob"
	"gocloud.dev/pubsub"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

type wrappedSub struct {
	sub        *pubsub.Subscription
	bucket     *blob.Bucket
	opts       claimcheck.Options
	ackDeletes bool
	ackContext context.Context
}

// SubscriptionOptions configures the claim-check receive wrapper. The embedded
// claimcheck.Options controls metadata prefix, serialization, and verification.
type SubscriptionOptions struct {
	claimcheck.Options

	// AckDeletes makes Batch.Ack also best-effort delete the offloaded blob
	// (ack first, then delete). Inline batches delete nothing.
	AckDeletes bool

	// AckContext is the context used for the background blob deletion
	// when AckDeletes is true and Batch.Ack() is called.
	// If nil, context.Background() is used.
	AckContext context.Context
}

// WrapSubscription adapts any gocloud *pubsub.Subscription into a claim-check
// Subscription. It assumes the receiving side uses the same bucket and options
// the producer offloaded with.
func WrapSubscription(s *pubsub.Subscription, b *blob.Bucket, opts SubscriptionOptions) Subscription {
	opts.SetDefaults()
	return &wrappedSub{sub: s, bucket: b, opts: opts.Options, ackDeletes: opts.AckDeletes, ackContext: opts.AckContext}
}

func (w *wrappedSub) Receive(ctx context.Context) (*Batch, error) {
	m, err := w.sub.Receive(ctx)
	if err != nil {
		return nil, err
	}
	cm, ok := claimcheck.ParseControlMessage(m.Metadata, w.opts.MetadataPrefix)
	if !ok && claimcheck.HasControlMessageMetadata(m.Metadata, w.opts.MetadataPrefix) {
		return nil, ErrCorruptControlMessage
	}
	return newBatch(cm, m.Body, m.Metadata, w.bucket, w.opts, m.Ack, m.Nack, w.ackDeletes, w.ackContext), nil
}

func (w *wrappedSub) Shutdown(ctx context.Context) error {
	return w.sub.Shutdown(ctx)
}
