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

func TestReceiveEndToEnd(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{
		MetadataPrefix: "cc_",
		Transformer:    claimcheck.NewGzipTransformer(),
	}

	topic := mempubsub.NewTopic()
	gsub := mempubsub.NewSubscription(topic, time.Second)
	t.Cleanup(func() { _ = topic.Shutdown(ctx); _ = gsub.Shutdown(ctx) })

	// Producer: offload a batch, publish only the control message.
	in := []*claimcheck.Message{{Body: []byte("x")}, {Body: []byte("y")}, {Body: []byte("z")}}
	cm, err := claimcheck.Offload(ctx, bucket, opts, in)
	require.NoError(t, err)
	require.NoError(t, topic.Send(ctx, &pubsub.Message{Metadata: cm.ToMetadata("cc_")}))

	// Consumer: wrap, receive, read, ack, clean up.
	sub := ccpubsub.WrapSubscription(gsub, bucket, ccpubsub.SubscriptionOptions{Options: opts})
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)

	out, err := batch.Read(ctx)
	require.NoError(t, err)
	require.Len(t, out, 3)
	require.Equal(t, "z", string(out[2].Body))

	batch.Ack()
	require.NoError(t, batch.Delete(ctx))
	exists, err := bucket.Exists(ctx, cm.Key)
	require.NoError(t, err)
	require.False(t, exists)
}
