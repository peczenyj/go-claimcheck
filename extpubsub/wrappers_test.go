package extpubsub_test

import (
	"context"
	"testing"
	"time"

	"github.com/peczenyj/go-claimcheck/extpubsub"
	"github.com/stretchr/testify/assert"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub/driver"
)

func TestWrappers(t *testing.T) {
	ctx := context.Background()
	drv := &extpubsub.MemDriver{}
	bucket := memblob.OpenBucket(nil)
	opts := extpubsub.Options{}

	topic := extpubsub.NewTopic(drv, bucket, opts)
	sub := extpubsub.NewSubscription(drv, bucket, opts)

	t.Run("WrapTopic", func(t *testing.T) {
		wt := extpubsub.WrapTopic(topic)
		assert.NotNil(t, wt)
		assert.Equal(t, topic, wt.Topic)
	})

	t.Run("WrapSubscription", func(t *testing.T) {
		ws := extpubsub.WrapSubscription(sub, bucket, opts)
		assert.NotNil(t, ws)
		assert.Equal(t, sub, ws.Subscription)
	})

	t.Run("Close", func(t *testing.T) {
		err := topic.Shutdown(ctx)
		assert.NoError(t, err)
		assert.True(t, drv.IsClosed())

		drv.Reset()
		err = sub.Shutdown(ctx)
		assert.NoError(t, err)
		assert.True(t, drv.IsClosed())
	})
}

func TestSubscription_Nack(t *testing.T) {
	ctx := context.Background()
	drv := &extpubsub.MemDriver{}
	bucket := memblob.OpenBucket(nil)
	opts := extpubsub.Options{}
	opts.SetDefaults()

	// 1. Create a blob with one message
	blobName := "test-blob"
	w, _ := bucket.NewWriter(ctx, blobName, nil)
	_ = opts.Serializer.Encode(w, []*extpubsub.Message{{Body: []byte("inner")}})
	_ = w.Close()

	sub := extpubsub.NewSubscription(drv, bucket, opts)

	// 2. Inject a control message directly into the driver
	drv.AddMessages(&driver.Message{
		Metadata: map[string]string{
			"extpubsub_v":   "1",
			"extpubsub_url": blobName,
		},
		AckID: "base-ack",
	})

	// 3. Receive and Nack
	m, err := sub.Receive(ctx)
	assert.NoError(t, err)

	if m.Nackable() {
		m.Nack()
		// Wait for nack to be processed (Go CDK might batch acks/nacks)
		time.Sleep(100 * time.Millisecond)
		assert.Len(t, drv.Nacks(), 1)
		assert.Equal(t, "base-ack", drv.Nacks()[0])
	}
}
