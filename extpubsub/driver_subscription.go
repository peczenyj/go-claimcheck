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
	// 1. If we have buffered messages, return them
	s.mu.Lock()
	if len(s.buffer) > 0 {
		n := maxMessages
		if len(s.buffer) < n {
			n = len(s.buffer)
		}
		res := s.buffer[:n]
		s.buffer = s.buffer[n:]
		s.mu.Unlock()
		return res, nil
	}
	s.mu.Unlock()

	// 2. Receive from underlying subscription (without lock)
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(finalMsgs) > maxMessages {
		s.buffer = append(s.buffer, finalMsgs[maxMessages:]...)
		return finalMsgs[:maxMessages], nil
	}

	return finalMsgs, nil
}

// ackTracker tracks the status of a set of messages unrolled from a single blob.
// It ensures the underlying control message is only acknowledged when all unrolled messages are handled.
type ackTracker struct {
	mu      sync.Mutex
	baseID  driver.AckID
	pending int
	nacked  bool
}

// syntheticAckID is a wrapper around the tracker used as an AckID for unrolled messages.
type syntheticAckID struct {
	tracker *ackTracker
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
	// We track acks so that the underlying control message is only acked
	// when ALL unrolled messages have been acked by the user.
	tracker := &ackTracker{
		baseID:  m.AckID,
		pending: len(extMsgs),
	}

	res := make([]*driver.Message, len(extMsgs))
	for i, em := range extMsgs {
		res[i] = &driver.Message{
			Body:     em.Body,
			Metadata: em.Metadata,
			AckID:    syntheticAckID{tracker: tracker},
		}
	}

	return res, nil
}

func (s *subscription) SendAcks(ctx context.Context, ackIDs []driver.AckID) error {
	var baseAcks []driver.AckID
	for _, id := range ackIDs {
		if syn, ok := id.(syntheticAckID); ok {
			syn.tracker.mu.Lock()
			syn.tracker.pending--
			if syn.tracker.pending == 0 && !syn.tracker.nacked {
				baseAcks = append(baseAcks, syn.tracker.baseID)
			}
			syn.tracker.mu.Unlock()
		} else {
			baseAcks = append(baseAcks, id)
		}
	}

	if len(baseAcks) > 0 {
		return s.Subscription.SendAcks(ctx, baseAcks)
	}
	return nil
}

func (s *subscription) SendNacks(ctx context.Context, ackIDs []driver.AckID) error {
	var baseNacks []driver.AckID
	for _, id := range ackIDs {
		if syn, ok := id.(syntheticAckID); ok {
			syn.tracker.mu.Lock()
			if !syn.tracker.nacked {
				syn.tracker.nacked = true
				baseNacks = append(baseNacks, syn.tracker.baseID)
			}
			syn.tracker.mu.Unlock()
		} else {
			baseNacks = append(baseNacks, id)
		}
	}

	if len(baseNacks) > 0 {
		return s.Subscription.SendNacks(ctx, baseNacks)
	}
	return nil
}

func (s *subscription) Close() error {
	return s.Subscription.Close()
}
