package extpubsub_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/driver"

	"github.com/peczenyj/go-claimcheck/extpubsub"
)

func TestIntegration_Transparent(t *testing.T) {
	ctx := context.Background()

	// 1. Setup base MemDriver and memblob
	drv := &extpubsub.MemDriver{}
	bucket := memblob.OpenBucket(nil)

	// 2. Setup Extended Topic and Subscription (Layer 1)
	opts := extpubsub.Options{
		Transformer: extpubsub.NewGzipTransformer(),
	}
	extTopic := extpubsub.NewTopic(drv, bucket, opts)
	extSub := extpubsub.NewSubscription(drv, bucket, opts)

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

func TestIntegration_Transparent_PartialAck(t *testing.T) {
	ctx := context.Background()
	drv := &extpubsub.MemDriver{}
	bucket := memblob.OpenBucket(nil)
	opts := extpubsub.Options{}
	opts.SetDefaults()

	// 1. Manually create a blob with 2 messages
	blobName := "multi-msg-blob"
	w, _ := bucket.NewWriter(ctx, blobName, nil)
	_ = opts.Serializer.Encode(w, []*extpubsub.Message{
		{Body: []byte("part 1")},
		{Body: []byte("part 2")},
	})
	_ = w.Close()

	// 2. Inject a control message into the driver
	drv.AddMessages(&driver.Message{
		Metadata: map[string]string{
			"extpubsub_v":   "1",
			"extpubsub_url": blobName,
		},
		AckID: "base-ack",
	})

	extSub := extpubsub.NewSubscription(drv, bucket, opts)

	// 3. Receive first message and Ack it
	m1, err := extSub.Receive(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []byte("part 1"), m1.Body)
	m1.Ack()

	// Wait for processing
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, drv.Acks(), "Base message should not be acked yet")

	// 4. Receive second message and Ack it
	m2, err := extSub.Receive(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []byte("part 2"), m2.Body)
	m2.Ack()

	// Wait for processing
	time.Sleep(50 * time.Millisecond)
	assert.Len(t, drv.Acks(), 1, "Base message should be acked now")
	assert.Equal(t, "base-ack", drv.Acks()[0])
}

func TestIntegration_Transparent_Nack(t *testing.T) {
	ctx := context.Background()
	drv := &extpubsub.MemDriver{}
	bucket := memblob.OpenBucket(nil)
	opts := extpubsub.Options{}
	opts.SetDefaults()

	// 1. Manually create a blob
	blobName := "nack-blob"
	w, _ := bucket.NewWriter(ctx, blobName, nil)
	_ = opts.Serializer.Encode(w, []*extpubsub.Message{{Body: []byte("nack me")}})
	_ = w.Close()

	// 2. Inject control message
	drv.AddMessages(&driver.Message{
		Metadata: map[string]string{
			"extpubsub_v":   "1",
			"extpubsub_url": blobName,
		},
		AckID: "nack-ack-id",
	})

	extSub := extpubsub.NewSubscription(drv, bucket, opts)

	m, err := extSub.Receive(ctx)
	assert.NoError(t, err)

	if m.Nackable() {
		m.Nack()
		time.Sleep(50 * time.Millisecond)
		assert.Len(t, drv.Nacks(), 1)
		assert.Equal(t, "nack-ack-id", drv.Nacks()[0])
	}
}

func TestIntegration_ExplicitBatch(t *testing.T) {
	ctx := context.Background()

	// 1. Setup
	drv := &extpubsub.MemDriver{}
	bucket := memblob.OpenBucket(nil)

	// 2. Setup Extended Topic and Wrapper Subscription (Layer 2)
	opts := extpubsub.Options{
		DisableTransparentUnrolling: true,
		InjectBlobMetadata:          true,
	}
	psTopic := extpubsub.NewTopic(drv, bucket, opts)
	psSub := extpubsub.NewSubscription(drv, bucket, opts)

	wrapperSub := extpubsub.WrapSubscription(psSub, bucket, opts)

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
