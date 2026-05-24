package claimcheck_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

func TestRead_RoundTrip(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	ctx := context.Background()
	opts := claimcheck.Options{}

	in := []*claimcheck.Message{{Body: []byte("one")}, {Body: []byte("two")}}
	cm, err := claimcheck.Offload(ctx, bucket, opts, in)
	require.NoError(t, err)

	out, err := claimcheck.Read(ctx, bucket, cm, opts)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, "one", string(out[0].Body))
	require.Equal(t, "two", string(out[1].Body))
}

func TestRead_MaxBatchSize(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	ctx := context.Background()

	cm, err := claimcheck.Offload(ctx, bucket, claimcheck.Options{}, []*claimcheck.Message{{Body: []byte("a sizable body of bytes")}})
	require.NoError(t, err)

	_, err = claimcheck.Read(ctx, bucket, cm, claimcheck.Options{MaxBatchSize: 4})
	require.ErrorIs(t, err, claimcheck.ErrBatchTooLarge)
}

func TestRead_VerifyChecksum_OK(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	ctx := context.Background()
	opts := claimcheck.Options{VerifyChecksum: true}

	cm, err := claimcheck.Offload(ctx, bucket, opts, []*claimcheck.Message{{Body: []byte("ok")}})
	require.NoError(t, err)

	out, err := claimcheck.Read(ctx, bucket, cm, opts)
	require.NoError(t, err)
	require.Len(t, out, 1)
}

func TestRead_VerifyChecksum_Mismatch(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	ctx := context.Background()
	opts := claimcheck.Options{VerifyChecksum: true}

	cm, err := claimcheck.Offload(ctx, bucket, opts, []*claimcheck.Message{{Body: []byte("ok")}})
	require.NoError(t, err)

	// Corrupt the stored blob in place but keep it valid JSON so it reaches EOF.
	require.NoError(t, bucket.WriteAll(ctx, cm.Key, []byte("{\"Body\":\"dHdv\"}\n"), &blob.WriterOptions{ContentType: cm.ContentType}))

	_, err = claimcheck.Read(ctx, bucket, cm, opts)
	require.ErrorIs(t, err, claimcheck.ErrChecksumMismatch)
}

func TestRead_VerifyChecksum_SkippedWhenAbsent(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	ctx := context.Background()

	cm, err := claimcheck.Offload(ctx, bucket, claimcheck.Options{}, []*claimcheck.Message{{Body: []byte("ok")}})
	require.NoError(t, err)
	cm.Checksum = "" // no MD5 available (e.g. multipart)

	out, err := claimcheck.Read(ctx, bucket, cm, claimcheck.Options{VerifyChecksum: true})
	require.NoError(t, err)
	require.Len(t, out, 1)
}
