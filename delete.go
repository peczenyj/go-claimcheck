package claimcheck

import (
	"context"

	"gocloud.dev/blob"
)

// Delete removes the blob named by cm. It is a no-op when cm has no key.
func Delete(ctx context.Context, bucket *blob.Bucket, cm ControlMessage) error {
	if cm.Key == "" {
		return nil
	}
	return bucket.Delete(ctx, cm.Key)
}
