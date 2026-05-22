package extpubsub

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub/driver"
)

func TestWrappers(t *testing.T) {
	ctx := context.Background()
	drv := &memDriver{}
	bucket := memblob.OpenBucket(nil)
	opts := Options{}

	topic := NewTopic(drv, bucket, opts)
	sub := NewSubscription(drv, bucket, opts)

	t.Run("WrapTopic", func(t *testing.T) {
		wt := WrapTopic(topic)
		assert.NotNil(t, wt)
		assert.Equal(t, topic, wt.Topic)
	})

	t.Run("WrapSubscription", func(t *testing.T) {
		ws := WrapSubscription(sub, bucket, opts)
		assert.NotNil(t, ws)
		assert.Equal(t, sub, ws.Subscription)
	})

	t.Run("Close", func(t *testing.T) {
		err := topic.Shutdown(ctx)
		assert.NoError(t, err)
		assert.True(t, drv.closed)

		// Reset for sub
		drv.closed = false
		err = sub.Shutdown(ctx)
		assert.NoError(t, err)
		assert.True(t, drv.closed)
	})
}

func TestSubscription_Nack(t *testing.T) {
	ctx := context.Background()
	drv := &memDriver{}
	bucket := memblob.OpenBucket(nil)
	opts := Options{}
	opts.SetDefaults()

	// 1. Create a blob with one message
	blobName := "test-blob"
	w, _ := bucket.NewWriter(ctx, blobName, nil)
	_ = opts.Serializer.Encode(w, []*Message{{Body: []byte("inner")}})
	_ = w.Close()

	sub := NewSubscription(drv, bucket, opts)

	// 2. Inject a control message
	drv.mu.Lock()
	drv.msgs = append(drv.msgs, &driver.Message{
		Metadata: map[string]string{
			"extpubsub_v":   "1",
			"extpubsub_url": blobName,
		},
		AckID: "base-ack",
	})
	drv.mu.Unlock()

	// 3. Receive and Nack
	m, err := sub.Receive(ctx)
	assert.NoError(t, err)

	if m.Nackable() {
		m.Nack()
		// Wait for nack to be processed (Go CDK might batch acks/nacks)
		time.Sleep(100 * time.Millisecond)
		drv.mu.Lock()
		defer drv.mu.Unlock()
		assert.Len(t, drv.nacks, 1)
		assert.Equal(t, "base-ack", drv.nacks[0])
	}
}
