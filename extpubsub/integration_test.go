package extpubsub

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/driver"
)

func TestIntegration_Transparent(t *testing.T) {
	ctx := context.Background()

	// 1. Setup base memDriver and memblob
	drv := &memDriver{}
	bucket := memblob.OpenBucket(nil)

	// 2. Setup Extended Topic and Subscription (Layer 1)
	opts := Options{
		Transformer: NewGzipTransformer(),
	}
	opts.SetDefaults()
	extSub := NewSubscription(drv, bucket, opts)

	// 3. Send messages as a single batch using the driver to ensure they are in one blob
	driverTopic := newTopic(drv, bucket, opts)
	msgs := []*driver.Message{
		{Body: []byte("msg 1"), Metadata: map[string]string{}},
		{Body: []byte("msg 2"), Metadata: map[string]string{}},
	}
	err := driverTopic.SendBatch(ctx, msgs)
	assert.NoError(t, err)

	// Wait a bit for Send to complete
	time.Sleep(10 * time.Millisecond)

	// Inject a custom AckID in the underlying memDriver
	drv.mu.Lock()
	if len(drv.msgs) == 1 {
		drv.msgs[0].AckID = "custom-ack-id"
	}
	drv.mu.Unlock()

	// 4. Receive and verify (Transparent)
	m1, err := extSub.Receive(ctx)
	assert.NoError(t, err)
	assert.Equal(t, msgs[0].Body, m1.Body)

	m2, err := extSub.Receive(ctx)
	assert.NoError(t, err)
	assert.Equal(t, msgs[1].Body, m2.Body)

	// Verify partial acks
	m1.Ack()
	time.Sleep(10 * time.Millisecond)
	drv.mu.Lock()
	assert.Empty(t, drv.acks, "Should not ack underlying message until all are acked")
	drv.mu.Unlock()

	m2.Ack()
	time.Sleep(10 * time.Millisecond)
	drv.mu.Lock()
	assert.Len(t, drv.acks, 1, "Should ack underlying message once all are acked")
	assert.Equal(t, "custom-ack-id", drv.acks[0])
	drv.mu.Unlock()
}

func TestIntegration_Transparent_Nack(t *testing.T) {
	ctx := context.Background()
	drv := &memDriver{}
	bucket := memblob.OpenBucket(nil)
	opts := Options{}
	extTopic := NewTopic(drv, bucket, opts)
	extSub := NewSubscription(drv, bucket, opts)

	err := extTopic.Send(ctx, &pubsub.Message{Body: []byte("msg 1")})
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)
	drv.mu.Lock()
	if len(drv.msgs) > 0 {
		drv.msgs[0].AckID = "custom-nack-id"
	}
	drv.mu.Unlock()

	m1, err := extSub.Receive(ctx)
	assert.NoError(t, err)

	if m1.Nackable() {
		m1.Nack()
		time.Sleep(10 * time.Millisecond)
		drv.mu.Lock()
		assert.Len(t, drv.nacks, 1)
		assert.Equal(t, "custom-nack-id", drv.nacks[0])
		drv.mu.Unlock()
	}
}

func TestIntegration_ExplicitBatch(t *testing.T) {
	ctx := context.Background()

	// 1. Setup
	drv := &memDriver{}
	bucket := memblob.OpenBucket(nil)

	// 2. Setup Extended Topic and Wrapper Subscription (Layer 2)
	opts := Options{
		DisableTransparentUnrolling: true,
		InjectBlobMetadata:          true,
	}
	// We use the driver to create the public topic/sub
	psTopic := NewTopic(drv, bucket, opts)
	psSub := NewSubscription(drv, bucket, opts)

	wrapperSub := WrapSubscription(psSub, bucket, opts)

	// 3. Send data
	err := psTopic.Send(ctx, &pubsub.Message{Body: []byte("batched data")})
	assert.NoError(t, err)

	// 4. Receive Batch
	batch, err := wrapperSub.ReceiveBatch(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, batch.MessageCount)
	assert.NotEmpty(t, batch.URL)
	assert.Equal(t, "application/x-ndjson", batch.ContentType)

	// 5. Unroll
	msgs, err := batch.Unroll(ctx)
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)
	assert.Equal(t, []byte("batched data"), msgs[0].Body)

	batch.Ack()
}

func TestIntegration_ExplicitBatch_Nack(t *testing.T) {
	ctx := context.Background()
	drv := &memDriver{}
	bucket := memblob.OpenBucket(nil)
	opts := Options{DisableTransparentUnrolling: true}
	psTopic := NewTopic(drv, bucket, opts)
	psSub := NewSubscription(drv, bucket, opts)
	wrapperSub := WrapSubscription(psSub, bucket, opts)

	err := psTopic.Send(ctx, &pubsub.Message{Body: []byte("batched data")})
	assert.NoError(t, err)

	batch, err := wrapperSub.ReceiveBatch(ctx)
	assert.NoError(t, err)

	if batch.Original.Nackable() {
		batch.Nack()
	}
}
