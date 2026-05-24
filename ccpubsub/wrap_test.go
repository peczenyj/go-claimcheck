package ccpubsub_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/mempubsub"

	claimcheck "github.com/peczenyj/go-claimcheck"
	"github.com/peczenyj/go-claimcheck/ccpubsub"
)

func TestWrapSubscription_OffloadedRoundTrip(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_"}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })

	cm, err := claimcheck.Offload(ctx, bucket, opts, []*claimcheck.Message{{Body: []byte("hello")}})
	require.NoError(t, err)
	require.NoError(t, topic.Send(ctx, &pubsub.Message{Metadata: cm.ToMetadata("cc_")}))

	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)
	require.True(t, batch.Offloaded())

	msgs, err := batch.Read(ctx)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "hello", string(msgs[0].Body))
	batch.Ack()
}

func TestWrapSubscription_InlinePassthrough(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })

	require.NoError(t, topic.Send(ctx, &pubsub.Message{Body: []byte("plain")}))

	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: claimcheck.Options{MetadataPrefix: "cc_"}})
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)
	require.False(t, batch.Offloaded())

	msgs, err := batch.Read(ctx)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "plain", string(msgs[0].Body))
	batch.Ack()
}

func TestWrapSubscription_CoverageGaps(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_"}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)

	cm, err := claimcheck.Offload(ctx, bucket, opts, []*claimcheck.Message{{Body: []byte("hello")}})
	require.NoError(t, err)
	require.NoError(t, topic.Send(ctx, &pubsub.Message{Metadata: cm.ToMetadata("cc_")}))

	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)
	require.True(t, batch.Offloaded())

	// Test ControlMessage()
	require.Equal(t, cm.Key, batch.ControlMessage().Key)

	// Test Nack()
	batch.Nack()

	// Ensure the message is still there after a Nack
	batch2, err := sub.Receive(ctx)
	require.NoError(t, err)
	require.Equal(t, cm.Key, batch2.ControlMessage().Key)
	batch2.Ack()

	// Test Shutdown()
	require.NoError(t, sub.Shutdown(ctx))
	require.NoError(t, topic.Shutdown(ctx))
}
