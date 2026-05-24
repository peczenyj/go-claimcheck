package claimcheck_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

func TestDIYRoundTrip(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	ctx := context.Background()

	opts := claimcheck.Options{
		Serializer:     claimcheck.NewLengthPrefixedSerializer(),
		Transformer:    claimcheck.NewGzipTransformer(),
		KeyPrefix:      "claimcheck/",
		MetadataPrefix: "cc_",
	}

	// Producer side (DIY): offload, then hand the metadata to a transport.
	in := []*claimcheck.Message{{Body: []byte("alpha")}, {Body: []byte("beta")}}
	cm, err := claimcheck.Offload(ctx, bucket, opts, in)
	require.NoError(t, err)
	md := cm.ToMetadata(opts.MetadataPrefix)

	// Consumer side (DIY): parse the metadata, read the blob.
	parsed, ok := claimcheck.ParseControlMessage(md, opts.MetadataPrefix)
	require.True(t, ok)
	require.Equal(t, "gzip", parsed.ContentEncoding)

	out, err := claimcheck.Read(ctx, bucket, parsed, opts)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Equal(t, "alpha", string(out[0].Body))
	require.Equal(t, "beta", string(out[1].Body))
}
