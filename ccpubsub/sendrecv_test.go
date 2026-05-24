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

func TestSendReceiveEndToEnd(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_", Transformer: claimcheck.NewGzipTransformer()}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = gsub.Shutdown(ctx) })
	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})

	wt := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{Options: opts, MaxMessages: 3})
	for _, b := range []string{"p", "q", "r"} {
		require.NoError(t, wt.Send(ctx, &claimcheck.Message{Body: []byte(b)}))
	}

	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	batch, err := sub.Receive(rctx)
	require.NoError(t, err)
	require.True(t, batch.Offloaded())

	out, err := batch.Read(ctx)
	require.NoError(t, err)
	require.Len(t, out, 3)
	require.Equal(t, "r", string(out[2].Body))

	batch.Ack()
	require.NoError(t, batch.Delete(ctx))
	require.NoError(t, wt.Shutdown(ctx))
}
