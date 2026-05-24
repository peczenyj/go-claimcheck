package ccpubsub_test

import (
	"context"
	"testing"
	"time"

	claimcheck "github.com/peczenyj/go-claimcheck"
	"github.com/peczenyj/go-claimcheck/ccpubsub"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/mempubsub"
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

	sub := ccpubsub.WrapSubscription(gsub, bucket, opts)
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

	sub := ccpubsub.WrapSubscription(gsub, bucket, claimcheck.Options{MetadataPrefix: "cc_"})
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)
	require.False(t, batch.Offloaded())

	msgs, err := batch.Read(ctx)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "plain", string(msgs[0].Body))
	batch.Ack()
}
