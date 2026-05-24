package ccpubsub

import (
	"context"

	"gocloud.dev/blob"
	"gocloud.dev/pubsub"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

type wrappedSub struct {
	sub    *pubsub.Subscription
	bucket *blob.Bucket
	opts   claimcheck.Options
}

// WrapSubscription adapts any gocloud *pubsub.Subscription into a claim-check
// Subscription. It assumes the receiving side uses the same bucket and options
// the producer offloaded with.
func WrapSubscription(s *pubsub.Subscription, b *blob.Bucket, opts claimcheck.Options) Subscription {
	opts.SetDefaults()
	return &wrappedSub{sub: s, bucket: b, opts: opts}
}

func (w *wrappedSub) Receive(ctx context.Context) (*Batch, error) {
	m, err := w.sub.Receive(ctx)
	if err != nil {
		return nil, err
	}
	cm, _ := claimcheck.ParseControlMessage(m.Metadata, w.opts.MetadataPrefix)
	return newBatch(cm, m.Body, m.Metadata, w.bucket, w.opts, m.Ack, m.Nack), nil
}

func (w *wrappedSub) Shutdown(ctx context.Context) error {
	return w.sub.Shutdown(ctx)
}
