// Package ccpubsub provides claim-check pubsub wrappers built on the core
// claimcheck package. The receive side is a Subscription whose Ack/Nack operate
// on a whole offloaded blob (the unit of delivery), adapted from any gocloud
// *pubsub.Subscription via WrapSubscription.
package ccpubsub

import (
	"context"
	"errors"
)

// ErrSubscriptionClosed is returned by Receive after the subscription is shut down.
var ErrSubscriptionClosed = errors.New("ccpubsub: subscription closed")

// ErrInlineBatch is returned by Batch.Open when the batch is an inline
// (non-offloaded) message and therefore has no blob to stream.
var ErrInlineBatch = errors.New("ccpubsub: batch is inline, not offloaded")

// Subscription is the claim-check consumer contract. Unlike a gocloud
// pubsub.Subscription, Ack/Nack apply to the whole received blob, not per message.
type Subscription interface {
	// Receive returns the next batch: one offloaded blob, or an inline message.
	Receive(ctx context.Context) (*Batch, error)
	// Shutdown stops the subscription and releases resources.
	Shutdown(ctx context.Context) error
}
