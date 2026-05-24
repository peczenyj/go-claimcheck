package ccpubsub_test

import (
	"context"
	"testing"
	"time"

	claimcheck "github.com/peczenyj/go-claimcheck"
	"github.com/peczenyj/go-claimcheck/ccpubsub"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub/mempubsub"
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
	sub := ccpubsub.WrapSubscription(gsub, bucket, opts)

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
	sub := ccpubsub.WrapSubscription(gsub, bucket, opts)

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
	sub := ccpubsub.WrapSubscription(gsub, bucket, opts)

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
	sub := ccpubsub.WrapSubscription(gsub, bucket, opts)

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
