package claimcheck_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

func TestDelete(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	ctx := context.Background()

	cm, err := claimcheck.Offload(ctx, bucket, claimcheck.Options{}, []*claimcheck.Message{{Body: []byte("x")}})
	require.NoError(t, err)

	require.NoError(t, claimcheck.Delete(ctx, bucket, cm))

	exists, err := bucket.Exists(ctx, cm.Key)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestDelete_EmptyKeyIsNoop(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	t.Cleanup(func() { _ = bucket.Close() })
	require.NoError(t, claimcheck.Delete(context.Background(), bucket, claimcheck.ControlMessage{}))
}
