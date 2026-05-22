package extpubsub

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub"
)

func TestTopicThreshold(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	t.Run("BelowThresholdSentDirectly", func(t *testing.T) {
		mem := &MemDriver{}
		opts := Options{
			MinSize: 100,
		}
		topic := NewTopic(mem, bucket, opts)

		err := topic.Send(ctx, &pubsub.Message{Body: []byte("short")})
		require.NoError(t, err)

		msgs := mem.Messages()
		require.Len(t, msgs, 1)
		assert.Equal(t, []byte("short"), msgs[0].Body)
		assert.Empty(t, msgs[0].Metadata["extpubsub_v"], "Message should not have been offloaded")
	})

	t.Run("AboveThresholdOffloaded", func(t *testing.T) {
		mem := &MemDriver{}
		opts := Options{
			MinSize: 5,
		}
		topic := NewTopic(mem, bucket, opts)

		err := topic.Send(ctx, &pubsub.Message{Body: []byte("long message")})
		require.NoError(t, err)

		msgs := mem.Messages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "1", msgs[0].Metadata["extpubsub_v"], "Message should have been offloaded")
		assert.NotEmpty(t, msgs[0].Metadata["extpubsub_url"])
	})

	t.Run("ZeroThresholdAlwaysOffloaded", func(t *testing.T) {
		mem := &MemDriver{}
		opts := Options{
			MinSize: 0,
		}
		topic := NewTopic(mem, bucket, opts)

		err := topic.Send(ctx, &pubsub.Message{Body: []byte("any")})
		require.NoError(t, err)

		msgs := mem.Messages()
		require.Len(t, msgs, 1)
		assert.Equal(t, "1", msgs[0].Metadata["extpubsub_v"], "Message should have been offloaded")
	})
}
