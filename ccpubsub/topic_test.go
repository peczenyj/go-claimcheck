package ccpubsub_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub/mempubsub"

	claimcheck "github.com/peczenyj/go-claimcheck"
	"github.com/peczenyj/go-claimcheck/ccpubsub"
)

// receiveOne wraps gsub and returns the bodies of the next batch.
func receiveOne(t *testing.T, ctx context.Context, sub ccpubsub.Subscription) []string {
	t.Helper()
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	batch, err := sub.Receive(rctx)
	require.NoError(t, err)
	msgs, err := batch.Read(ctx)
	require.NoError(t, err)
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = string(m.Body)
	}
	batch.Ack()
	return out
}

func TestWrapTopic_CountThresholdFlush(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_"}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })
	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})

	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: opts, MaxMessages: 2})
	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("a")}))
	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("b")})) // triggers flush

	require.Equal(t, []string{"a", "b"}, receiveOne(t, ctx, sub))
}

func TestWrapTopic_ByteThresholdFlush(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_"}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })
	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})

	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: opts, MaxBytes: 3})
	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("hello")})) // 5 >= 3 → flush

	require.Equal(t, []string{"hello"}, receiveOne(t, ctx, sub))
}

func TestWrapTopic_ManualFlush(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_"}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })
	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})

	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: opts})
	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("x")}))
	require.NoError(t, wt.Flush(ctx))

	require.Equal(t, []string{"x"}, receiveOne(t, ctx, sub))
}

func TestWrapTopic_ShutdownFlushes(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_"}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = gsub.Shutdown(ctx) })
	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})

	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: opts})
	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("y")}))
	require.NoError(t, wt.Shutdown(ctx))

	require.Equal(t, []string{"y"}, receiveOne(t, ctx, sub))
}

func TestWrapTopic_SendAfterShutdown(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	topic := mempubsub.NewTopic()
	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: claimcheck.Options{MetadataPrefix: "cc_"}})
	require.NoError(t, wt.Shutdown(ctx))

	err := wt.Send(ctx, &claimcheck.Message{Body: []byte("late")})
	require.ErrorIs(t, err, ccpubsub.ErrTopicClosed)
}

func TestWrapTopic_FlushInterval(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_"}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })
	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})

	// No count/byte threshold — only the timer can flush.
	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{
		Options:       opts,
		FlushInterval: 10 * time.Millisecond,
	})
	t.Cleanup(func() { _ = wt.Shutdown(ctx) })

	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("timed")}))

	// The background timer should flush within receiveOne's 2s timeout.
	require.Equal(t, []string{"timed"}, receiveOne(t, ctx, sub))
}

func countBlobs(t *testing.T, ctx context.Context, b *blob.Bucket) int {
	t.Helper()
	it := b.List(nil)
	n := 0
	for {
		_, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		n++
	}
	return n
}

func TestWrapTopic_PublishFailureDeletesBlob(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	topic := mempubsub.NewTopic()
	require.NoError(t, topic.Shutdown(ctx)) // make Send fail after Offload writes the blob

	cct := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{})
	require.NoError(t, cct.Send(ctx, &claimcheck.Message{Body: []byte("x")})) // buffered, no flush yet

	err := cct.Flush(ctx)
	require.Error(t, err) // publish failed

	require.Equal(t, 0, countBlobs(t, ctx, bucket), "orphaned blob must be cleaned up")
}

func TestWrapTopic_SuccessfulFlushLeavesOneBlob(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	topic := mempubsub.NewTopic()
	t.Cleanup(func() { _ = topic.Shutdown(ctx) })

	cct := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{})
	require.NoError(t, cct.Send(ctx, &claimcheck.Message{Body: []byte("x")}))
	require.NoError(t, cct.Flush(ctx))

	require.Equal(t, 1, countBlobs(t, ctx, bucket))
}
