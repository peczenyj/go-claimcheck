package claimcheck

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"

	"gocloud.dev/blob"
)

// ErrBatchTooLarge is returned when a blob exceeds the configured MaxBatchSize.
var ErrBatchTooLarge = errors.New("claimcheck: blob exceeds MaxBatchSize")

// ErrChecksumMismatch is returned by a verifying read when the blob MD5 does
// not match the control message checksum.
var ErrChecksumMismatch = errors.New("claimcheck: blob checksum mismatch")

// Open opens the blob named by cm and returns a streaming Decoder over its
// messages plus an io.Closer the caller MUST close when done. opts size caps
// and (when enabled) MD5 verification are applied.
func Open(ctx context.Context, bucket *blob.Bucket, cm ControlMessage, opts Options) (Decoder, io.Closer, error) {
	opts.SetDefaults()

	r, err := bucket.NewReader(ctx, cm.Key, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("claimcheck: open blob: %w", err)
	}

	var raw io.Reader = r
	if opts.MaxBatchSize > 0 {
		raw = &limitedReader{r: raw, remaining: int64(opts.MaxBatchSize)}
	}

	verify := opts.VerifyChecksum && isHexMD5(cm.Checksum)
	var hasher hash.Hash
	if verify {
		hasher = md5.New()
		raw = io.TeeReader(raw, hasher)
	}

	tr, err := opts.Transformer.WrapReader(raw)
	if err != nil {
		_ = r.Close()
		return nil, nil, fmt.Errorf("claimcheck: wrap reader: %w", err)
	}

	closer := &openCloser{transformer: tr, blob: r, hasher: hasher, want: cm.Checksum, verify: verify}
	inner := opts.Serializer.NewDecoder(tr, opts.MaxMessageSize)
	return &verifyingDecoder{Decoder: inner, closer: closer}, closer, nil
}

// Read opens the blob named by cm and decodes all of its messages.
func Read(ctx context.Context, bucket *blob.Bucket, cm ControlMessage, opts Options) ([]*Message, error) {
	dec, closer, err := Open(ctx, bucket, cm, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()

	var msgs []*Message
	buf := make([]*Message, 64)
	for {
		n, derr := dec.Decode(buf)
		msgs = append(msgs, buf[:n]...)
		if errors.Is(derr, io.EOF) {
			return msgs, nil
		}
		if derr != nil {
			return nil, derr
		}
	}
}

type openCloser struct {
	transformer io.ReadCloser
	blob        io.ReadCloser
	hasher      hash.Hash
	want        string
	verify      bool
}

func (c *openCloser) Close() error {
	err1 := c.transformer.Close()
	err2 := c.blob.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (c *openCloser) verifyChecksum() error {
	if !c.verify {
		return nil
	}
	if hex.EncodeToString(c.hasher.Sum(nil)) != c.want {
		return ErrChecksumMismatch
	}
	return nil
}

// verifyingDecoder runs the checksum comparison once the stream reaches EOF —
// the only point at which the full blob has passed through the hasher.
type verifyingDecoder struct {
	Decoder
	closer  *openCloser
	checked bool
}

func (d *verifyingDecoder) Decode(buf []*Message) (int, error) {
	n, err := d.Decoder.Decode(buf)
	if err != nil && !d.checked {
		d.checked = true
		// On any terminal error (EOF or decode failure), verify the checksum
		// first. A checksum mismatch is the more informative error.
		if cerr := d.closer.verifyChecksum(); cerr != nil {
			return n, cerr
		}
	}
	return n, err
}

func isHexMD5(s string) bool {
	if len(s) != 32 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// limitedReader errors (rather than truncating) once total bytes read exceed
// its limit, so an oversized blob is rejected instead of silently cut short.
type limitedReader struct {
	r         io.Reader
	remaining int64
}

func (l *limitedReader) Read(p []byte) (int, error) {
	if l.remaining <= 0 {
		return 0, ErrBatchTooLarge
	}
	if int64(len(p)) > l.remaining {
		p = p[:l.remaining]
	}
	n, err := l.r.Read(p)
	l.remaining -= int64(n)
	return n, err
}
