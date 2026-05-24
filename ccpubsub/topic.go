package ccpubsub

import (
	"context"
	"errors"
	"sync"
	"time"

	claimcheck "github.com/peczenyj/go-claimcheck"
	"gocloud.dev/blob"
	"gocloud.dev/pubsub"
)

// ErrTopicClosed is returned by Send after the topic is shut down.
var ErrTopicClosed = errors.New("ccpubsub: topic closed")

// Topic is the claim-check producer contract. Send buffers messages; a buffered
// batch is offloaded to one blob and published as a single control message when
// a threshold is reached, on Flush, or on Shutdown.
type Topic interface {
	// Send buffers a message for offloading.
	Send(ctx context.Context, m *claimcheck.Message) error
	// Flush offloads and publishes any buffered messages immediately.
	Flush(ctx context.Context) error
	// Shutdown flushes remaining messages and shuts down the underlying topic.
	Shutdown(ctx context.Context) error
}

// TopicOptions configures the buffering send wrapper. The embedded
// claimcheck.Options controls serialization, blob naming, and metadata prefix.
type TopicOptions struct {
	claimcheck.Options

	// MaxMessages flushes when the buffer reaches this many messages (0 = off).
	MaxMessages int
	// MaxBytes flushes when buffered message bodies reach this many bytes (0 = off).
	MaxBytes int
	// FlushInterval periodically flushes a non-empty buffer (0 = off).
	FlushInterval time.Duration
}

type bufTopic struct {
	topic       *pubsub.Topic
	bucket      *blob.Bucket
	opts        claimcheck.Options
	maxMessages int
	maxBytes    int

	mu       sync.Mutex
	buf      []*claimcheck.Message
	bufBytes int
	closed   bool

	stop chan struct{}
	wg   sync.WaitGroup
}

// WrapTopic wraps a gocloud *pubsub.Topic with claim-check buffering offload.
func WrapTopic(t *pubsub.Topic, b *blob.Bucket, topts TopicOptions) Topic {
	opts := topts.Options
	opts.SetDefaults()
	bt := &bufTopic{
		topic:       t,
		bucket:      b,
		opts:        opts,
		maxMessages: topts.MaxMessages,
		maxBytes:    topts.MaxBytes,
	}
	if topts.FlushInterval > 0 {
		bt.stop = make(chan struct{})
		bt.wg.Add(1)
		go bt.flushLoop(topts.FlushInterval)
	}
	return bt
}

func (t *bufTopic) Send(ctx context.Context, m *claimcheck.Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrTopicClosed
	}
	t.buf = append(t.buf, m)
	t.bufBytes += len(m.Body)
	if (t.maxMessages > 0 && len(t.buf) >= t.maxMessages) ||
		(t.maxBytes > 0 && t.bufBytes >= t.maxBytes) {
		return t.flushLocked(ctx)
	}
	return nil
}

func (t *bufTopic) Flush(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.flushLocked(ctx)
}

// flushLocked offloads the buffer and publishes one control message. The buffer
// is retained on error so messages are not lost. Callers must hold t.mu.
func (t *bufTopic) flushLocked(ctx context.Context) error {
	if len(t.buf) == 0 {
		return nil
	}
	cm, err := claimcheck.Offload(ctx, t.bucket, t.opts, t.buf)
	if err != nil {
		return err
	}
	if err := t.topic.Send(ctx, &pubsub.Message{Metadata: cm.ToMetadata(t.opts.MetadataPrefix)}); err != nil {
		return err
	}
	t.buf = nil
	t.bufBytes = 0
	return nil
}

func (t *bufTopic) flushLoop(interval time.Duration) {
	defer t.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-ticker.C:
			t.mu.Lock()
			_ = t.flushLocked(context.Background())
			t.mu.Unlock()
		}
	}
}

func (t *bufTopic) Shutdown(ctx context.Context) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.mu.Unlock()

	if t.stop != nil {
		close(t.stop)
		t.wg.Wait()
	}

	t.mu.Lock()
	err := t.flushLocked(ctx)
	t.mu.Unlock()
	if err != nil {
		return err
	}
	return t.topic.Shutdown(ctx)
}
