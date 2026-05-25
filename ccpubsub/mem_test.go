package ccpubsub_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"

	claimcheck "github.com/peczenyj/go-claimcheck"
	"github.com/peczenyj/go-claimcheck/ccpubsub"
)

func TestMemSubscription_OffloadedRoundTrip(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_"}

	cm, err := claimcheck.Offload(ctx, bucket, opts,
		[]*claimcheck.Message{{Body: []byte("a")}, {Body: []byte("b")}})
	require.NoError(t, err)

	sub := ccpubsub.NewMemSubscription(bucket, opts, 4)
	sub.Push(cm.ToMetadata("cc_"), nil)

	batch, err := sub.Receive(ctx)
	require.NoError(t, err)
	require.True(t, batch.Offloaded())

	msgs, err := batch.Read(ctx)
	require.NoError(t, err)
	require.Len(t, msgs, 2)
	require.Equal(t, "a", string(msgs[0].Body))
	require.Equal(t, "b", string(msgs[1].Body))

	batch.Ack()
	require.NoError(t, batch.Delete(ctx))
	exists, err := bucket.Exists(ctx, cm.Key)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestMemSubscription_InlineMessage(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	sub := ccpubsub.NewMemSubscription(bucket, claimcheck.Options{MetadataPrefix: "cc_"}, 4)
	sub.Push(map[string]string{"app": "x"}, []byte("inline-body"))

	batch, err := sub.Receive(ctx)
	require.NoError(t, err)
	require.False(t, batch.Offloaded())

	msgs, err := batch.Read(ctx)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, "inline-body", string(msgs[0].Body))

	dec, closer, err := batch.Open(ctx)
	require.NoError(t, err)
	defer closer.Close()

	buf := make([]*claimcheck.Message, 1)
	n, err := dec.Decode(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, "inline-body", string(buf[0].Body))

	n, err = dec.Decode(buf)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 0, n)
	require.NoError(t, batch.Delete(ctx)) // no-op for inline
}

func TestMemSubscription_StreamingOpen(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{MetadataPrefix: "cc_"}

	cm, err := claimcheck.Offload(ctx, bucket, opts,
		[]*claimcheck.Message{{Body: []byte("1")}, {Body: []byte("2")}, {Body: []byte("3")}})
	require.NoError(t, err)

	sub := ccpubsub.NewMemSubscription(bucket, opts, 1)
	sub.Push(cm.ToMetadata("cc_"), nil)
	batch, err := sub.Receive(ctx)
	require.NoError(t, err)

	dec, closer, err := batch.Open(ctx)
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()

	buf := make([]*claimcheck.Message, 2)
	got := 0
	for {
		n, err := dec.Decode(buf)
		got += n
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}
	require.Equal(t, 3, got)
}

func TestMemSubscription_ShutdownThenReceive(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	sub := ccpubsub.NewMemSubscription(bucket, claimcheck.Options{}, 1)
	require.NoError(t, sub.Shutdown(context.Background()))
	_, err := sub.Receive(context.Background())
	require.ErrorIs(t, err, ccpubsub.ErrSubscriptionClosed)
}

// https://github.com/peczenyj/go-claimcheck/issues/52
func TestMemSubscription_PushAfterShutdownNoPanic(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	sub := ccpubsub.NewMemSubscription(bucket, claimcheck.Options{}, 1)
	require.NoError(t, sub.Shutdown(context.Background()))

	require.NotPanics(t, func() {
		sub.Push(map[string]string{"k": "v"}, []byte("late"))
	}, "Push after Shutdown must not panic")

	_, err := sub.Receive(context.Background())
	require.ErrorIs(t, err, ccpubsub.ErrSubscriptionClosed)
}
