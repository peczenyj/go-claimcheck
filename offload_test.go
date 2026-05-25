package claimcheck_test

import (
	"context"
	"errors"
	"io"
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

	_, err = time.Parse(time.RFC3339, cm.CreatedAt)
	require.NoError(t, err, "CreatedAt must be RFC3339")

	exists, err := bucket.Exists(context.Background(), cm.Key)
	require.NoError(t, err)
	require.True(t, exists)
}

type faultyTransformer struct{ claimcheck.NoopTransformer }

func (f faultyTransformer) ContentEncoding() string { return "" }

func (f faultyTransformer) WrapWriter(w io.Writer) (io.WriteCloser, error) {
	return faultyWriteCloser{w}, nil
}

type faultyWriteCloser struct{ io.Writer }

func (f faultyWriteCloser) Write(p []byte) (int, error) { return 0, errors.New("write error") }
func (f faultyWriteCloser) Close() error                { return errors.New("close error") }

func TestOffload_Errors(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })

	// 1. Encode error
	_, err := claimcheck.Offload(ctx, bucket, claimcheck.Options{Transformer: &faultyTransformer{}}, []*claimcheck.Message{{Body: []byte("x")}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "write error")

	// 2. WrapWriter error (using a transformer that returns error immediately)
	_, err = claimcheck.Offload(ctx, bucket, claimcheck.Options{Transformer: &errTransformer{}}, []*claimcheck.Message{{Body: []byte("x")}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrap error")

	// 3. Writer Close error (can't easily trigger bucket.NewWriter Close error with memblob,
	// but we can trigger transformer Close error with faultyTransformer if Encode succeeded but Close failed)
	_, err = claimcheck.Offload(ctx, bucket, claimcheck.Options{Transformer: &closeFaultyTransformer{}}, []*claimcheck.Message{{Body: []byte("x")}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "close error")
}

type closeFaultyTransformer struct{ claimcheck.NoopTransformer }

func (f closeFaultyTransformer) ContentEncoding() string { return "" }

func (f closeFaultyTransformer) WrapWriter(w io.Writer) (io.WriteCloser, error) {
	return closeFaultyWriteCloser{w}, nil
}

type closeFaultyWriteCloser struct{ io.Writer }

func (f closeFaultyWriteCloser) Close() error { return errors.New("close error") }

type errTransformer struct{ claimcheck.NoopTransformer }

func (e errTransformer) ContentEncoding() string { return "" }

func (e errTransformer) WrapWriter(w io.Writer) (io.WriteCloser, error) {
	return nil, errors.New("wrap error")
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
