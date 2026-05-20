package extpubsub

import (
	"context"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"gocloud.dev/blob"
	"gocloud.dev/pubsub/driver"
)

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
	// 1. Generate unique blob name
	blobName := uuid.New().String()

	// 2. Prepare blob metadata (for the blob service itself)
	metadata := make(map[string]string)
	if t.opts.InjectBlobMetadata {
		metadata["msg_count"] = strconv.Itoa(len(msgs))
	}

	// 3. Write to blob
	w, err := t.bucket.NewWriter(ctx, blobName, &blob.WriterOptions{
		ContentType: t.opts.Serializer.ContentType(),
		Metadata:    metadata,
	})
	if err != nil {
		return fmt.Errorf("extpubsub: failed to create blob writer: %w", err)
	}

	tw, err := t.opts.Transformer.WrapWriter(w)
	if err != nil {
		_ = w.Close()
		return fmt.Errorf("extpubsub: failed to wrap writer: %w", err)
	}

	// Convert driver.Message to Message for the serializer
	pubMsgs := make([]*Message, len(msgs))
	for i, m := range msgs {
		pubMsgs[i] = &Message{
			Body:     m.Body,
			Metadata: m.Metadata,
		}
	}

	if err := t.opts.Serializer.Encode(tw, pubMsgs); err != nil {
		_ = tw.Close()
		_ = w.Close()
		return fmt.Errorf("extpubsub: failed to encode messages: %w", err)
	}

	if err := tw.Close(); err != nil {
		_ = w.Close()
		return fmt.Errorf("extpubsub: failed to close transformer: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("extpubsub: failed to close blob writer: %w", err)
	}

	// 4. Get blob attributes (for size/checksum)
	attr, err := t.bucket.Attributes(ctx, blobName)
	if err != nil {
		return fmt.Errorf("extpubsub: failed to get blob attributes: %w", err)
	}

	// 5. Send Control Message
	prefix := t.opts.MetadataPrefix
	controlMsg := &driver.Message{
		Metadata: map[string]string{
			prefix + "v":                "1",
			prefix + "url":              blobName, // We store the key/name. Receiver must have same bucket context.
			prefix + "msg_count":        strconv.Itoa(len(msgs)),
			prefix + "content_type":     t.opts.Serializer.ContentType(),
			prefix + "content_encoding": t.opts.Transformer.ContentEncoding(),
			prefix + "file_size":        strconv.FormatInt(attr.Size, 10),
			prefix + "checksum":         fmt.Sprintf("%x", attr.MD5),
		},
	}

	return t.Topic.SendBatch(ctx, []*driver.Message{controlMsg})
}

func (t *topic) Close() error {
	return t.Topic.Close()
}
