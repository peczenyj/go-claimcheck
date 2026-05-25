package ccpubsub

import (
	"context"
	"io"
	"time"

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
	ackDeletes bool
	ackContext context.Context
}

func newBatch(
	cm claimcheck.ControlMessage,
	inlineBody []byte,
	inlineMeta map[string]string,
	bucket *blob.Bucket,
	opts claimcheck.Options,
	ack, nack func(),
	ackDeletes bool,
	ackContext context.Context,
) *Batch {
	return &Batch{
		cm: cm, inlineBody: inlineBody, inlineMeta: inlineMeta,
		bucket: bucket, opts: opts, ack: ack, nack: nack, ackDeletes: ackDeletes, ackContext: ackContext,
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
		start := time.Now()
		msgs := []*claimcheck.Message{{Body: b.inlineBody, Metadata: b.inlineMeta}}
		b.opts.Observer.ReadDone(ctx, claimcheck.ReadInfo{
			MsgCount:  1,
			Bytes:     int64(len(b.inlineBody)),
			Inline:    true,
			StartTime: start,
			Duration:  time.Since(start),
		})
		return msgs, nil
	}
	return claimcheck.Read(ctx, b.bucket, b.cm, b.opts)
}

// Open returns a streaming decoder over the blob for bounded-memory reads, plus
// an io.Closer the caller MUST close.
func (b *Batch) Open(ctx context.Context) (claimcheck.Decoder, io.Closer, error) {
	if !b.Offloaded() {
		dec := &inlineDecoder{msg: &claimcheck.Message{Body: b.inlineBody, Metadata: b.inlineMeta}}
		closer := &inlineCloser{
			ctx:      ctx,
			observer: b.opts.Observer,
			bytes:    int64(len(b.inlineBody)),
			start:    time.Now(),
		}
		return dec, closer, nil
	}
	return claimcheck.Open(ctx, b.bucket, b.cm, b.opts)
}

type inlineDecoder struct {
	msg  *claimcheck.Message
	done bool
}

func (d *inlineDecoder) Decode(buf []*claimcheck.Message) (int, error) {
	if d.done {
		return 0, io.EOF
	}
	if len(buf) > 0 {
		buf[0] = d.msg
		d.done = true
		return 1, nil
	}
	return 0, nil
}

type inlineCloser struct {
	ctx      context.Context
	observer claimcheck.Observer
	bytes    int64
	start    time.Time
	fired    bool
}

func (c *inlineCloser) Close() error {
	if !c.fired {
		c.fired = true
		c.observer.ReadDone(c.ctx, claimcheck.ReadInfo{
			MsgCount:  1,
			Bytes:     c.bytes,
			Inline:    true,
			StartTime: c.start,
			Duration:  time.Since(c.start),
		})
	}
	return nil
}

// Delete removes the offloaded blob. It is a no-op for an inline batch.
func (b *Batch) Delete(ctx context.Context) error {
	if !b.Offloaded() {
		return nil
	}
	return claimcheck.Delete(ctx, b.bucket, b.cm)
}

// Ack acknowledges the whole batch. When the subscription was created with
// AckDeletes, it acks first and then best-effort deletes the offloaded blob
// (errors ignored; a bucket lifecycle policy is the backstop). The delete uses
// the configured AckContext, or context.Background() if nil. Inline batches delete nothing.
func (b *Batch) Ack() {
	if b.ack != nil {
		b.ack()
	}
	if b.ackDeletes {
		ctx := b.ackContext
		if ctx == nil {
			ctx = context.Background()
		}
		_ = b.Delete(ctx)
	}
}

// AckAndDelete acknowledges the batch first and then deletes the offloaded blob,
// returning any delete error. Use when you want explicit ctx/error control.
// Delete is a no-op for an inline batch.
func (b *Batch) AckAndDelete(ctx context.Context) error {
	if b.ack != nil {
		b.ack()
	}
	return b.Delete(ctx)
}

// Nack requests redelivery of the whole batch.
func (b *Batch) Nack() {
	if b.nack != nil {
		b.nack()
	}
}
