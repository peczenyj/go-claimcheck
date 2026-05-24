package ccpubsub

import (
	"context"
	"io"

	"gocloud.dev/blob"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

// Batch is one received unit: either an offloaded blob (described by a control
// message) or an inline message that was not offloaded. Ack/Nack apply to the
// whole unit.
type Batch struct {
	cm         claimcheck.ControlMessage
	inlineBody []byte
	inlineMeta map[string]string
	bucket     *blob.Bucket
	opts       claimcheck.Options
	ack        func()
	nack       func()
}

func newBatch(
	cm claimcheck.ControlMessage,
	inlineBody []byte,
	inlineMeta map[string]string,
	bucket *blob.Bucket,
	opts claimcheck.Options,
	ack, nack func(),
) *Batch {
	return &Batch{
		cm: cm, inlineBody: inlineBody, inlineMeta: inlineMeta,
		bucket: bucket, opts: opts, ack: ack, nack: nack,
	}
}

// ControlMessage returns the parsed control message. Its Key is empty for an
// inline (non-offloaded) batch.
func (b *Batch) ControlMessage() claimcheck.ControlMessage { return b.cm }

// Offloaded reports whether this batch points at a blob.
func (b *Batch) Offloaded() bool { return b.cm.Key != "" }

// Read decodes all messages in the batch.
func (b *Batch) Read(ctx context.Context) ([]*claimcheck.Message, error) {
	if !b.Offloaded() {
		return []*claimcheck.Message{{Body: b.inlineBody, Metadata: b.inlineMeta}}, nil
	}
	return claimcheck.Read(ctx, b.bucket, b.cm, b.opts)
}

// Open returns a streaming decoder over the blob for bounded-memory reads, plus
// an io.Closer the caller MUST close. It returns ErrInlineBatch for an inline batch.
func (b *Batch) Open(ctx context.Context) (claimcheck.Decoder, io.Closer, error) {
	if !b.Offloaded() {
		return nil, nil, ErrInlineBatch
	}
	return claimcheck.Open(ctx, b.bucket, b.cm, b.opts)
}

// Delete removes the offloaded blob. It is a no-op for an inline batch.
func (b *Batch) Delete(ctx context.Context) error {
	if !b.Offloaded() {
		return nil
	}
	return claimcheck.Delete(ctx, b.bucket, b.cm)
}

// Ack acknowledges the whole batch.
func (b *Batch) Ack() {
	if b.ack != nil {
		b.ack()
	}
}

// Nack requests redelivery of the whole batch.
func (b *Batch) Nack() {
	if b.nack != nil {
		b.nack()
	}
}
