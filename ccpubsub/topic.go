package ccpubsub

import (
	"context"
	"errors"
	"sync"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/pubsub"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

// ErrTopicClosed is returned by Send after the topic is shut down.
var ErrTopicClosed = errors.New("ccpubsub: topic closed")

// DefaultMaxBytes bounds the buffered-message bytes when a WrapTopic is created
// with no flush trigger at all (MaxMessages, MaxBytes, and FlushInterval all
// zero). It exists so a zero-config producer cannot buffer without limit; once
// buffered bodies reach it, the batch is flushed.
const DefaultMaxBytes = 1 << 20 // 1 MiB

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
//
// If none of MaxMessages, MaxBytes, or FlushInterval is set, the buffer is
// flushed only on an explicit Flush/Shutdown and is capped at DefaultMaxBytes
// so it cannot grow without limit.
type TopicOptions struct {
	claimcheck.Options

	// MaxMessages flushes when the buffer reaches this many messages (0 = off).
	MaxMessages int
	// MaxBytes flushes when buffered message bodies reach this many bytes (0 = off).
	MaxBytes int
	// FlushInterval periodically flushes a non-empty buffer (0 = off).
	FlushInterval time.Duration
	// FlushTimeout bounds how long a single periodic (FlushInterval) flush may
	// take, via a context deadline. 0 (the default) means no deadline: the
	// periodic flush runs to completion. It is independent of FlushInterval.
	FlushTimeout time.Duration
}

type bufTopic struct {
	topic       *pubsub.Topic
	bucket      *blob.Bucket
	opts        claimcheck.Options
	maxMessages int
	maxBytes    int

	flushTimeout time.Duration

	mu       sync.Mutex
	buf      []*claimcheck.Message
	bufBytes int
	closed   bool

	flushMu sync.Mutex

	stop chan struct{}
	wg   sync.WaitGroup
}

// WrapTopic wraps a gocloud *pubsub.Topic with claim-check buffering offload.
func WrapTopic(t *pubsub.Topic, b *blob.Bucket, topts TopicOptions) Topic {
	opts := topts.Options
	opts.SetDefaults()
	maxMessages, maxBytes := topts.MaxMessages, topts.MaxBytes
	// With no flush trigger configured at all, bound the buffer so a zero-config
	// producer cannot grow it without limit.
	if maxMessages == 0 && maxBytes == 0 && topts.FlushInterval == 0 {
		maxBytes = DefaultMaxBytes
	}
	bt := &bufTopic{
		topic:        t,
		bucket:       b,
		opts:         opts,
		maxMessages:  maxMessages,
		maxBytes:     maxBytes,
		flushTimeout: topts.FlushTimeout,
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
	if t.closed {
		t.mu.Unlock()
		return ErrTopicClosed
	}
	if t.opts.MinSize > 0 && len(m.Body) < t.opts.MinSize {
		t.mu.Unlock()
		return t.topic.Send(ctx, &pubsub.Message{Body: m.Body, Metadata: m.Metadata})
	}
	t.buf = append(t.buf, m)
	t.bufBytes += len(m.Body)
	needsFlush := (t.maxMessages > 0 && len(t.buf) >= t.maxMessages) ||
		(t.maxBytes > 0 && t.bufBytes >= t.maxBytes)
	t.mu.Unlock()

	if needsFlush {
		return t.Flush(ctx)
	}
	return nil
}

func (t *bufTopic) Flush(ctx context.Context) error {
	t.flushMu.Lock()
	defer t.flushMu.Unlock()

	t.mu.Lock()
	if len(t.buf) == 0 {
		t.mu.Unlock()
		return nil
	}
	buf := t.buf
	t.buf = nil
	t.bufBytes = 0
	t.mu.Unlock()

	cm, err := claimcheck.Offload(ctx, t.bucket, t.opts, buf)
	if err != nil {
		t.restoreBuffer(buf)
		return err
	}
	if err := t.topic.Send(ctx, &pubsub.Message{Metadata: cm.ToMetadata(t.opts.MetadataPrefix)}); err != nil {
		err = errors.Join(err, claimcheck.Delete(ctx, t.bucket, cm))
		t.restoreBuffer(buf)
		return err
	}
	return nil
}

func (t *bufTopic) restoreBuffer(failedBuf []*claimcheck.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(failedBuf, t.buf...)
	t.bufBytes = 0
	for _, m := range t.buf {
		t.bufBytes += len(m.Body)
	}
}

// flushContext returns the context for a periodic flush. It is bounded by
// FlushTimeout when set, and otherwise has no deadline — the cadence
// (FlushInterval) must not double as the flush timeout.
func (t *bufTopic) flushContext() (context.Context, context.CancelFunc) {
	if t.flushTimeout > 0 {
		return context.WithTimeout(context.Background(), t.flushTimeout)
	}
	return context.Background(), func() {}
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
			ctx, cancel := t.flushContext()
			_ = t.Flush(ctx)
			cancel()
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

	flushErr := t.Flush(ctx)
	shutErr := t.topic.Shutdown(ctx)
	return errors.Join(flushErr, shutErr)
}
