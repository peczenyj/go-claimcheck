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

func TestAckDeletes_AckRemovesBlob(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_", KeyFunc: func([]*claimcheck.Message) string { return "blob-1" }}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })

	cm, err := claimcheck.Offload(ctx, bucket, opts, []*claimcheck.Message{{Body: []byte("hello")}})
	require.NoError(t, err)
	require.NoError(t, topic.Send(ctx, &pubsub.Message{Metadata: cm.ToMetadata("cc_")}))

	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts, AckDeletes: true})
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)
	_, err = batch.Read(ctx) // incidental: Read is not a precondition for Ack
	require.NoError(t, err)

	batch.Ack()

	exists, err := bucket.Exists(ctx, "blob-1")
	require.NoError(t, err)
	require.False(t, exists, "AckDeletes should have removed the blob")
}

func TestAckDeletes_DisabledKeepsBlob(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_", KeyFunc: func([]*claimcheck.Message) string { return "blob-2" }}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })

	cm, err := claimcheck.Offload(ctx, bucket, opts, []*claimcheck.Message{{Body: []byte("hello")}})
	require.NoError(t, err)
	require.NoError(t, topic.Send(ctx, &pubsub.Message{Metadata: cm.ToMetadata("cc_")}))

	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts}) // AckDeletes false
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)
	batch.Ack()

	exists, err := bucket.Exists(ctx, "blob-2")
	require.NoError(t, err)
	require.True(t, exists, "default Ack must not delete the blob")
}

func TestAckAndDelete_RemovesBlobAndReturnsNil(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_", KeyFunc: func([]*claimcheck.Message) string { return "blob-3" }}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })

	cm, err := claimcheck.Offload(ctx, bucket, opts, []*claimcheck.Message{{Body: []byte("hello")}})
	require.NoError(t, err)
	require.NoError(t, topic.Send(ctx, &pubsub.Message{Metadata: cm.ToMetadata("cc_")}))

	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)

	require.NoError(t, batch.AckAndDelete(ctx))

	exists, err := bucket.Exists(ctx, "blob-3")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestAckAndDelete_InlineIsNoOp(t *testing.T) {
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

	require.NoError(t, batch.AckAndDelete(ctx)) // no blob to delete
}

// TestRoundTrip_ProduceConsumeAckDeletes exercises the full v0.3.0 retention
// story end to end: WrapTopic offloads a message to a blob, WrapSubscription
// receives it, and ack-deletes removes the blob — leaving an empty bucket.
func TestRoundTrip_ProduceConsumeAckDeletes(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_"}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })

	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: opts})
	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("payload")}))
	require.NoError(t, wt.Flush(ctx))
	require.Equal(t, 1, countBlobs(t, ctx, bucket), "offload should have written one blob")

	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts, AckDeletes: true})
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)
	require.True(t, batch.Offloaded())

	msgs, err := batch.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), msgs[0].Body)

	batch.Ack()
	require.Equal(t, 0, countBlobs(t, ctx, bucket), "ack-deletes should have removed the blob")
}
