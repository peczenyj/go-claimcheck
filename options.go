package claimcheck

import "github.com/google/uuid"

// Options configures offload, read, and the wrappers built on the core.
type Options struct {
	// Serializer encodes/decodes message batches. Defaults to JSON Lines.
	Serializer Serializer
	// Transformer compresses/transforms the blob bytes. Defaults to Noop.
	Transformer Transformer
	// MetadataPrefix is prepended to control-message metadata keys. Defaults to "claimcheck_".
	MetadataPrefix string
	// KeyPrefix is prepended to every blob key (e.g. "claimcheck/").
	KeyPrefix string
	// KeyFunc returns the unique portion of the blob key. Final key is
	// KeyPrefix + KeyFunc(msgs). Defaults to a random UUID.
	KeyFunc func(msgs []*Message) string
	// MinSize is a byte threshold used by the ccpubsub send wrapper: a message
	// whose Body is smaller than MinSize is published inline (not offloaded to a
	// blob). 0 (the default) offloads every message. It has no effect on the
	// core Offload, which always writes a blob.
	MinSize int
	// InjectBlobMetadata, if true, also writes msg_count to the blob object.
	InjectBlobMetadata bool
	// VerifyChecksum, if true, verifies the blob MD5 against the control
	// message on read (when a hex MD5 is present).
	VerifyChecksum bool
	// AllowMissingChecksum, if true, allows reads to proceed without verification
	// when VerifyChecksum is true but the blob backend does not provide an MD5.
	AllowMissingChecksum bool
	// MaxMessageSize caps a single decoded message's bytes (0 = unlimited).
	MaxMessageSize int
	// MaxDownloadSize caps total stored bytes read from a blob (0 = unlimited).
	MaxDownloadSize int
	// MaxBatchSize is deprecated; use MaxDownloadSize instead.
	MaxBatchSize int
	// Observer receives offload/read notifications. Defaults to NopObserver.
	Observer Observer
}

// SetDefaults fills in default values. Safe to call more than once.
func (o *Options) SetDefaults() {
	if o.Serializer == nil {
		o.Serializer = NewJSONLinesSerializer()
	}
	if o.Transformer == nil {
		o.Transformer = NewNoopTransformer()
	}
	if o.MetadataPrefix == "" {
		o.MetadataPrefix = "claimcheck_"
	}
	if o.KeyFunc == nil {
		o.KeyFunc = func([]*Message) string { return uuid.New().String() }
	}
	if o.Observer == nil {
		o.Observer = NopObserver{}
	}
	if o.MaxDownloadSize == 0 && o.MaxBatchSize > 0 {
		o.MaxDownloadSize = o.MaxBatchSize
	}
}
