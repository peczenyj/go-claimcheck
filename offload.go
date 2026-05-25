package claimcheck

import (
	"context"
	"crypto/md5"
	"fmt"
	"hash"
	"io"
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
		blobMeta = map[string]string{
			"msg_count":  strconv.Itoa(len(msgs)),
			"created_at": time.Now().UTC().Format(time.RFC3339),
		}
	}

	w, err := bucket.NewWriter(ctx, key, &blob.WriterOptions{
		ContentType: opts.Serializer.ContentType(),
		Metadata:    blobMeta,
	})
	if err != nil {
		return ControlMessage{}, fmt.Errorf("claimcheck: create blob writer: %w", err)
	}

	// Track size and MD5 locally to avoid a redundant Attributes network call.
	twrapper := &trackingWriter{w: w, h: md5.New()}

	tw, err := opts.Transformer.WrapWriter(twrapper)
	if err != nil {
		_ = w.Close()
		_ = bucket.Delete(ctx, key)
		return ControlMessage{}, fmt.Errorf("claimcheck: wrap writer: %w", err)
	}

	if err := opts.Serializer.Encode(tw, msgs); err != nil {
		_ = tw.Close()
		_ = w.Close()
		_ = bucket.Delete(ctx, key)
		return ControlMessage{}, fmt.Errorf("claimcheck: encode messages: %w", err)
	}

	if err := tw.Close(); err != nil {
		_ = w.Close()
		_ = bucket.Delete(ctx, key)
		return ControlMessage{}, fmt.Errorf("claimcheck: close transformer: %w", err)
	}
	if err := w.Close(); err != nil {
		_ = bucket.Delete(ctx, key)
		return ControlMessage{}, fmt.Errorf("claimcheck: close blob writer: %w", err)
	}

	return ControlMessage{
		Version:         Version,
		Key:             key,
		MessageCount:    len(msgs),
		ContentType:     opts.Serializer.ContentType(),
		ContentEncoding: opts.Transformer.ContentEncoding(),
		FileSize:        twrapper.n,
		Checksum:        fmt.Sprintf("%x", twrapper.h.Sum(nil)),
	}, nil
}

type trackingWriter struct {
	w io.Writer
	h hash.Hash
	n int64
}

func (t *trackingWriter) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if n > 0 {
		_, _ = t.h.Write(p[:n])
		t.n += int64(n)
	}
	return n, err
}
