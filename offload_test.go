package claimcheck_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

func TestOffload(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	msgs := []*claimcheck.Message{{Body: []byte("hello")}, {Body: []byte("world")}}
	cm, err := claimcheck.Offload(context.Background(), bucket, claimcheck.Options{KeyPrefix: "cc/"}, msgs)
	require.NoError(t, err)

	require.Equal(t, claimcheck.Version, cm.Version)
	require.Contains(t, cm.Key, "cc/")
	require.Equal(t, 2, cm.MessageCount)
	require.Equal(t, "application/x-ndjson", cm.ContentType)
	require.Empty(t, cm.ContentEncoding)
	require.Positive(t, cm.FileSize)

	exists, err := bucket.Exists(context.Background(), cm.Key)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestOffloadInjectsCreatedAt(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{InjectBlobMetadata: true, KeyFunc: func([]*claimcheck.Message) string { return "k" }}

	_, err := claimcheck.Offload(ctx, bucket, opts, []*claimcheck.Message{{Body: []byte("x")}})
	require.NoError(t, err)

	attr, err := bucket.Attributes(ctx, "k")
	require.NoError(t, err)

	ca, ok := attr.Metadata["created_at"]
	require.True(t, ok, "created_at missing; metadata=%v", attr.Metadata)
	_, err = time.Parse(time.RFC3339, ca)
	require.NoErrorf(t, err, "created_at not RFC3339: %q", ca)
}

func TestOffloadNoCreatedAtWhenDisabled(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	opts := claimcheck.Options{KeyFunc: func([]*claimcheck.Message) string { return "k" }} // InjectBlobMetadata is false

	_, err := claimcheck.Offload(ctx, bucket, opts, []*claimcheck.Message{{Body: []byte("x")}})
	require.NoError(t, err)

	attr, err := bucket.Attributes(ctx, "k")
	require.NoError(t, err)
	require.Empty(t, attr.Metadata, "no blob metadata should be written when InjectBlobMetadata is false")
}
