package extpubsub

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"gocloud.dev/blob"
	"gocloud.dev/pubsub"
)

// Subscription wraps *pubsub.Subscription to provide explicit batch handling.
type Subscription struct {
	*pubsub.Subscription
	bucket *blob.Bucket
	opts   Options
}

// WrapSubscription wraps an existing *pubsub.Subscription.
// It assumes the underlying subscription was created with the same bucket and options.
func WrapSubscription(s *pubsub.Subscription, b *blob.Bucket, opts Options) *Subscription {
	opts.SetDefaults()
	return &Subscription{
		Subscription: s,
		bucket:       b,
		opts:         opts,
	}
}

// Batch represents a collection of messages offloaded to a blob.
type Batch struct {
	URL             string
	MessageCount    int
	ContentType     string
	ContentEncoding string
	FileSize        int64
	Checksum        string
	
	// Original is the control message received from the queue.
	Original *pubsub.Message

	bucket *blob.Bucket
	opts   Options
}

// ReceiveBatch waits for a message. If it's a control message, it returns a Batch.
// If it's a normal message, it returns a Batch with a single message (Original).
func (s *Subscription) ReceiveBatch(ctx context.Context) (*Batch, error) {
	m, err := s.Subscription.Receive(ctx)
	if err != nil {
		return nil, err
	}

	prefix := s.opts.MetadataPrefix
	if m.Metadata[prefix+"v"] == "1" {
		count, _ := strconv.Atoi(m.Metadata[prefix+"msg_count"])
		size, _ := strconv.ParseInt(m.Metadata[prefix+"file_size"], 10, 64)
		
		return &Batch{
			URL:             m.Metadata[prefix+"url"],
			MessageCount:    count,
			ContentType:     m.Metadata[prefix+"content_type"],
			ContentEncoding: m.Metadata[prefix+"content_encoding"],
			FileSize:        size,
			Checksum:        m.Metadata[prefix+"checksum"],
			Original:        m,
			bucket:          s.bucket,
			opts:            s.opts,
		}, nil
	}

	// Normal message treated as a batch of 1 (without blob)
	return &Batch{
		MessageCount: 1,
		Original:     m,
		bucket:       s.bucket,
		opts:         s.opts,
	}, nil
}

// Unroll downloads and decodes all messages in the batch.
func (b *Batch) Unroll(ctx context.Context) ([]*Message, error) {
	if b.URL == "" {
		// Normal message
		return []*Message{{Body: b.Original.Body, Metadata: b.Original.Metadata}}, nil
	}

	r, err := b.Reader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return b.opts.Serializer.Decode(r)
}

// Reader returns an io.ReadCloser for the raw (transformed) blob data.
func (b *Batch) Reader(ctx context.Context) (io.ReadCloser, error) {
	if b.URL == "" {
		return nil, fmt.Errorf("extpubsub: batch is not a blob")
	}

	r, err := b.bucket.NewReader(ctx, b.URL, nil)
	if err != nil {
		return nil, err
	}

	tr, err := b.opts.Transformer.WrapReader(r)
	if err != nil {
		_ = r.Close()
		return nil, err
	}

	return &readCloser{Reader: tr, closer: r}, nil
}

type readCloser struct {
	io.Reader
	closer io.Closer
}

func (rc *readCloser) Close() error {
	err1 := rc.Reader.(io.Closer).Close()
	err2 := rc.closer.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

// Ack acknowledges the control message.
func (b *Batch) Ack() {
	b.Original.Ack()
}

// Nack negatively acknowledges the control message.
func (b *Batch) Nack() {
	b.Original.Nack()
}
