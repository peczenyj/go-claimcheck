package ccpubsub_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub/mempubsub"

	claimcheck "github.com/peczenyj/go-claimcheck"
	"github.com/peczenyj/go-claimcheck/ccpubsub"
)

func TestSend_SmallMessageInline(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_", MinSize: 10}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })

	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: opts})
	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("hi")})) // 2 < 10 -> inline now

	require.Equal(t, 0, countBlobs(t, ctx, bucket), "small message must not be offloaded")

	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)
	require.False(t, batch.Offloaded(), "small message should arrive inline")
	msgs, err := batch.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("hi"), msgs[0].Body)
	batch.Ack()
}

func TestSend_LargeMessageOffloaded(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_", MinSize: 4}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })

	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: opts})
	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("hello")})) // 5 >= 4 -> buffered
	require.NoError(t, wt.Flush(ctx))
	require.Equal(t, 1, countBlobs(t, ctx, bucket), "large message must be offloaded")

	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)
	require.True(t, batch.Offloaded(), "large message should arrive offloaded")
	batch.Ack()
}

func TestSend_DefaultMinSizeZeroOffloadsSmall(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_"} // MinSize 0

	topic := mempubsub.NewTopic()
	t.Cleanup(func() { _ = topic.Shutdown(ctx) })

	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: opts})
	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("hi")}))
	require.NoError(t, wt.Flush(ctx))
	require.Equal(t, 1, countBlobs(t, ctx, bucket), "MinSize=0 must offload even small messages")
}

func TestSend_MinSizeBoundary(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		wantInline bool
	}{
		{"below_threshold", "1234", true}, // len 4 < 5 -> inline
		{"at_threshold", "12345", false},  // len 5 >= 5 -> offload
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			bucket := memblob.OpenBucket(nil)
			t.Cleanup(func() { _ = bucket.Close() })
			opts := claimcheck.Options{MetadataPrefix: "cc_", MinSize: 5}

			topic := mempubsub.NewTopic()
			gsub := mempubsub.NewSubscription(topic, time.Second)
			t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })

			wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: opts})
			require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte(tc.body)}))
			require.NoError(t, wt.Flush(ctx)) // harmless no-op if already sent inline

			if tc.wantInline {
				require.Equal(t, 0, countBlobs(t, ctx, bucket))
			} else {
				require.Equal(t, 1, countBlobs(t, ctx, bucket))
			}

			sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})
			batch, err := sub.Receive(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.wantInline, !batch.Offloaded())
			batch.Ack()
		})
	}
}

func TestSend_InlineMetadataRoundTrip(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_", MinSize: 100}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })

	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: opts})
	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("x"), Metadata: map[string]string{"k": "v"}}))

	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)
	require.False(t, batch.Offloaded())
	msgs, err := batch.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "v", msgs[0].Metadata["k"], "inline message metadata should round-trip")
	batch.Ack()
}

func TestSend_InlineAndOffloadedInterleave(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_", MinSize: 5}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })

	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: opts})
	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("big-one")})) // 7 >= 5 -> buffered
	require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte("sm")}))      // 2 < 5 -> inline now
	require.NoError(t, wt.Flush(ctx))                                              // offload buffered big-one
	require.Equal(t, 1, countBlobs(t, ctx, bucket), "exactly one blob (the large message)")

	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})
	offloaded, inline := 0, 0
	for range 2 {
		b, err := sub.Receive(ctx)
		require.NoError(t, err)
		if b.Offloaded() {
			offloaded++
		} else {
			inline++
		}
		_, err = b.Read(ctx)
		require.NoError(t, err)
		b.Ack()
	}
	require.Equal(t, 1, offloaded)
	require.Equal(t, 1, inline)
}
