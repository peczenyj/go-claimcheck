package ccpubsub

import (
	"context"
	"sync"

	"gocloud.dev/blob"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

// MemSubscription is an in-memory Subscription for tests and examples. Enqueue
// received messages with Push; Receive returns them as Batches.
type MemSubscription struct {
	bucket *blob.Bucket
	opts   claimcheck.Options
	ch     chan memItem

	mu     sync.Mutex
	closed bool
}

type memItem struct {
	metadata map[string]string
	body     []byte
}

// NewMemSubscription creates an in-memory subscription buffering up to size items.
func NewMemSubscription(bucket *blob.Bucket, opts claimcheck.Options, size int) *MemSubscription {
	opts.SetDefaults()
	return &MemSubscription{bucket: bucket, opts: opts, ch: make(chan memItem, size)}
}

// Push enqueues an incoming message. metadata carries control-message fields
// (use ControlMessage.ToMetadata) for an offloaded message; body is the inline
// payload for a non-offloaded message (nil when offloaded).
func (m *MemSubscription) Push(metadata map[string]string, body []byte) {
	m.ch <- memItem{metadata: metadata, body: body}
}

// Receive returns the next pushed item as a Batch.
func (m *MemSubscription) Receive(ctx context.Context) (*Batch, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case it, ok := <-m.ch:
		if !ok {
			return nil, ErrSubscriptionClosed
		}
		cm, _ := claimcheck.ParseControlMessage(it.metadata, m.opts.MetadataPrefix)
		return newBatch(cm, it.body, it.metadata, m.bucket, m.opts, func() {}, func() {}, false), nil
	}
}

// Shutdown closes the subscription. Subsequent Receive calls return ErrSubscriptionClosed.
func (m *MemSubscription) Shutdown(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.ch)
	}
	return nil
}
