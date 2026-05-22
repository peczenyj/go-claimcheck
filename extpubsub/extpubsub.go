// Package extpubsub provides a claim-check implementation for Go CDK pubsub.
package extpubsub

import (
	"io"
)

// Message is a serializable representation of a pubsub message.
type Message struct {
	Body     []byte            `json:"body"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// Serializer defines how a slice of messages is encoded into a blob.
type Serializer interface {
	// Encode serializes the messages into the writer.
	Encode(w io.Writer, msgs []*Message) error
	// Decode deserializes messages from the reader.
	Decode(r io.Reader) ([]*Message, error)
	// ContentType returns the MIME type for the serialized format.
	ContentType() string
}

// Transformer defines a middleware for the serialized data (e.g. compression, encryption).
type Transformer interface {
	// WrapWriter wraps the given writer (e.g. with a gzip.Writer).
	WrapWriter(w io.Writer) (io.WriteCloser, error)
	// WrapReader wraps the given reader (e.g. with a gzip.Reader).
	WrapReader(r io.Reader) (io.ReadCloser, error)
	// ContentEncoding returns the string identifying the transformation (e.g. "gzip").
	ContentEncoding() string
}

// Options contains configuration for the extended pubsub topic/subscription.
type Options struct {
	// Serializer is used to encode/decode message batches. Defaults to JSONLines.
	Serializer Serializer
	// Transformer is used to compress/encrypt the blob. Defaults to Noop.
	Transformer Transformer
	// InjectBlobMetadata if true, adds metadata (like message count) to the blob object itself.
	InjectBlobMetadata bool
	// MetadataPrefix is prepended to the metadata keys sent in the control message. Defaults to "extpubsub_".
	MetadataPrefix string
	// DisableTransparentUnrolling if true, the driver will not automatically download and unroll blobs.
	DisableTransparentUnrolling bool
	// MinSize is the threshold in bytes for offloading messages to blob storage.
	// If the total size of a batch of messages is smaller than this, they are sent directly.
	// Defaults to 0 (always offload).
	MinSize int
}

// SetDefaults fills in default values for Options.
func (o *Options) SetDefaults() {
	if o.Serializer == nil {
		o.Serializer = NewJSONLinesSerializer()
	}
	if o.Transformer == nil {
		o.Transformer = NewNoopTransformer()
	}
	if o.MetadataPrefix == "" {
		o.MetadataPrefix = "extpubsub_"
	}
}
