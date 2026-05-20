package extpubsub

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub"
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
	extTopic := NewTopic(drv, bucket, opts)
	extSub := NewSubscription(drv, bucket, opts)

	// 3. Send messages
	msgs := []*pubsub.Message{
		{Body: []byte("msg 1")},
		{Body: []byte("msg 2")},
	}
	for _, m := range msgs {
		err := extTopic.Send(ctx, m)
		assert.NoError(t, err)
	}

	// 4. Receive and verify (Transparent)
	for i := 0; i < 2; i++ {
		m, err := extSub.Receive(ctx)
		assert.NoError(t, err)
		assert.Equal(t, msgs[i].Body, m.Body)
		m.Ack()
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
