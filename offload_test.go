package claimcheck_test

import (
	"context"
	"testing"

	claimcheck "github.com/peczenyj/go-claimcheck"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
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
