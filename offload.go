package claimcheck

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"gocloud.dev/blob"
)

// Offload writes msgs as a single blob to bucket and returns the ControlMessage
// describing it. The blob key is opts.KeyPrefix + opts.KeyFunc(msgs).
func Offload(ctx context.Context, bucket *blob.Bucket, opts Options, msgs []*Message) (cm ControlMessage, err error) {
	opts.SetDefaults()
	start := time.Now()
	key := opts.KeyPrefix + opts.KeyFunc(msgs)

	defer func() {
		opts.Observer.OffloadDone(ctx, OffloadInfo{
			Key:       key,
			MsgCount:  len(msgs),
			Bytes:     cm.FileSize,
			Encoding:  opts.Transformer.ContentEncoding(),
			StartTime: start,
			Duration:  time.Since(start),
			Err:       err,
		})
	}()

	var blobMeta map[string]string
	if opts.InjectBlobMetadata {
		blobMeta = map[string]string{"msg_count": strconv.Itoa(len(msgs))}
	}

	w, err := bucket.NewWriter(ctx, key, &blob.WriterOptions{
		ContentType: opts.Serializer.ContentType(),
		Metadata:    blobMeta,
	})
	if err != nil {
		return ControlMessage{}, fmt.Errorf("claimcheck: create blob writer: %w", err)
	}

	tw, err := opts.Transformer.WrapWriter(w)
	if err != nil {
		_ = w.Close()
		return ControlMessage{}, fmt.Errorf("claimcheck: wrap writer: %w", err)
	}

	if err := opts.Serializer.Encode(tw, msgs); err != nil {
		_ = tw.Close()
		_ = w.Close()
		return ControlMessage{}, fmt.Errorf("claimcheck: encode messages: %w", err)
	}

	if err := tw.Close(); err != nil {
		_ = w.Close()
		return ControlMessage{}, fmt.Errorf("claimcheck: close transformer: %w", err)
	}
	if err := w.Close(); err != nil {
		return ControlMessage{}, fmt.Errorf("claimcheck: close blob writer: %w", err)
	}

	attr, err := bucket.Attributes(ctx, key)
	if err != nil {
		return ControlMessage{}, fmt.Errorf("claimcheck: blob attributes: %w", err)
	}

	return ControlMessage{
		Version:         Version,
		Key:             key,
		MessageCount:    len(msgs),
		ContentType:     opts.Serializer.ContentType(),
		ContentEncoding: opts.Transformer.ContentEncoding(),
		FileSize:        attr.Size,
		Checksum:        fmt.Sprintf("%x", attr.MD5),
	}, nil
}
