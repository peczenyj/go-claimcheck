package extpubsub

import (
	"context"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"gocloud.dev/blob"
	"gocloud.dev/pubsub/driver"
)

// offload writes msgs as a single blob to bucket and returns the control-message
// metadata describing it (version, url, msg_count, content_type,
// content_encoding, file_size, checksum), keyed with opts.MetadataPrefix.
// The blob name is a fresh UUID.
func offload(ctx context.Context, bucket *blob.Bucket, opts Options, msgs []*Message) (map[string]string, error) {
	blobName := uuid.New().String()

	metadata := make(map[string]string)
	if opts.InjectBlobMetadata {
		metadata["msg_count"] = strconv.Itoa(len(msgs))
	}

	w, err := bucket.NewWriter(ctx, blobName, &blob.WriterOptions{
		ContentType: opts.Serializer.ContentType(),
		Metadata:    metadata,
	})
	if err != nil {
		return nil, fmt.Errorf("extpubsub: failed to create blob writer: %w", err)
	}

	tw, err := opts.Transformer.WrapWriter(w)
	if err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("extpubsub: failed to wrap writer: %w", err)
	}

	if err := opts.Serializer.Encode(tw, msgs); err != nil {
		_ = tw.Close()
		_ = w.Close()
		return nil, fmt.Errorf("extpubsub: failed to encode messages: %w", err)
	}

	if err := tw.Close(); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("extpubsub: failed to close transformer: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("extpubsub: failed to close blob writer: %w", err)
	}

	attr, err := bucket.Attributes(ctx, blobName)
	if err != nil {
		return nil, fmt.Errorf("extpubsub: failed to get blob attributes: %w", err)
	}

	prefix := opts.MetadataPrefix
	return map[string]string{
		prefix + "v":                "1",
		prefix + "url":              blobName,
		prefix + "msg_count":        strconv.Itoa(len(msgs)),
		prefix + "content_type":     opts.Serializer.ContentType(),
		prefix + "content_encoding": opts.Transformer.ContentEncoding(),
		prefix + "file_size":        strconv.FormatInt(attr.Size, 10),
		prefix + "checksum":         fmt.Sprintf("%x", attr.MD5),
	}, nil
}

type topic struct {
	driver.Topic
	bucket *blob.Bucket
	opts   Options
}

// newTopic creates a new driver.Topic that offloads messages to a blob.
func newTopic(base driver.Topic, bucket *blob.Bucket, opts Options) driver.Topic {
	return &topic{
		Topic:  base,
		bucket: bucket,
		opts:   opts,
	}
}

func (t *topic) SendBatch(ctx context.Context, msgs []*driver.Message) error {
	// 0. Check if we should offload based on size
	if t.opts.MinSize > 0 {
		var totalSize int
		for _, m := range msgs {
			totalSize += len(m.Body)
		}
		if totalSize < t.opts.MinSize {
			return t.Topic.SendBatch(ctx, msgs)
		}
	}

	// 1. Convert driver.Message to Message for the serializer
	pubMsgs := make([]*Message, len(msgs))
	for i, m := range msgs {
		pubMsgs[i] = &Message{Body: m.Body, Metadata: m.Metadata}
	}

	// 2. Offload to blob and build the control metadata
	metadata, err := offload(ctx, t.bucket, t.opts, pubMsgs)
	if err != nil {
		return err
	}

	// 3. Send the control message
	return t.Topic.SendBatch(ctx, []*driver.Message{{Metadata: metadata}})
}

func (t *topic) Close() error {
	return t.Topic.Close()
}
