package extpubsub

import (
	"context"
	"fmt"
	"sync"

	"gocloud.dev/blob"
	"gocloud.dev/pubsub/driver"
)

type subscription struct {
	driver.Subscription
	bucket *blob.Bucket
	opts   Options

	mu     sync.Mutex
	buffer []*driver.Message
}

// newSubscription creates a new driver.Subscription that unrolls messages from blobs.
func newSubscription(base driver.Subscription, bucket *blob.Bucket, opts Options) driver.Subscription {
	return &subscription{
		Subscription: base,
		bucket:       bucket,
		opts:         opts,
	}
}

func (s *subscription) ReceiveBatch(ctx context.Context, maxMessages int) ([]*driver.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. If we have buffered messages, return them
	if len(s.buffer) > 0 {
		n := maxMessages
		if len(s.buffer) < n {
			n = len(s.buffer)
		}
		res := s.buffer[:n]
		s.buffer = s.buffer[n:]
		return res, nil
	}

	// 2. Receive from underlying subscription
	msgs, err := s.Subscription.ReceiveBatch(ctx, maxMessages)
	if err != nil {
		return nil, err
	}

	var finalMsgs []*driver.Message
	prefix := s.opts.MetadataPrefix

	for _, m := range msgs {
		if !s.opts.DisableTransparentUnrolling && m.Metadata[prefix+"v"] == "1" {
			// This is a control message, unroll it
			unrolled, err := s.unroll(ctx, m)
			if err != nil {
				return nil, fmt.Errorf("extpubsub: failed to unroll message: %w", err)
			}
			finalMsgs = append(finalMsgs, unrolled...)
		} else {
			finalMsgs = append(finalMsgs, m)
		}
	}

	// 3. If we got more than maxMessages, buffer the rest
	if len(finalMsgs) > maxMessages {
		s.buffer = append(s.buffer, finalMsgs[maxMessages:]...)
		return finalMsgs[:maxMessages], nil
	}

	return finalMsgs, nil
}

func (s *subscription) unroll(ctx context.Context, m *driver.Message) ([]*driver.Message, error) {
	prefix := s.opts.MetadataPrefix
	blobName := m.Metadata[prefix+"url"]

	// 1. Read from blob
	r, err := s.bucket.NewReader(ctx, blobName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create blob reader: %w", err)
	}
	defer func() { _ = r.Close() }()

	tr, err := s.opts.Transformer.WrapReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to wrap reader: %w", err)
	}
	defer func() { _ = tr.Close() }()

	// 2. Decode messages
	extMsgs, err := s.opts.Serializer.Decode(tr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode messages: %w", err)
	}

	// 3. Convert back to driver.Message
	// Note: We might want to preserve the AckID of the control message for ALL unrolled messages.
	// But in Go CDK, AckID is usually tied to the specific message.
	// If the user Acks ONE of the unrolled messages, should we Ack the whole blob?
	// Standard Extended Client behavior: Ack the control message when all unrolled messages are handled?
	// For simplicity now, we attach the same AckID to all.
	res := make([]*driver.Message, len(extMsgs))
	for i, em := range extMsgs {
		res[i] = &driver.Message{
			Body:     em.Body,
			Metadata: em.Metadata,
			AckID:    m.AckID, // shared AckID
		}
	}

	return res, nil
}

func (s *subscription) Close() error {
	return s.Subscription.Close()
}
