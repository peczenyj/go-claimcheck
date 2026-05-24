package claimcheck

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"time"

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
	start := time.Now()

	r, err := bucket.NewReader(ctx, cm.Key, nil)
	if err != nil {
		return nil, nil, fireReadErr(ctx, opts, cm, start, fmt.Errorf("claimcheck: open blob: %w", err))
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
		return nil, nil, fireReadErr(ctx, opts, cm, start, fmt.Errorf("claimcheck: wrap reader: %w", err))
	}

	closer := &openCloser{
		transformer: tr, blob: r, hasher: hasher, want: cm.Checksum, verify: verify,
		ctx: ctx, observer: opts.Observer, key: cm.Key, fileSize: cm.FileSize, start: start,
	}
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

// fireReadErr emits a failed-read notification and returns err unchanged so it
// can be used inline on Open's error returns.
func fireReadErr(ctx context.Context, opts Options, cm ControlMessage, start time.Time, err error) error {
	opts.Observer.ReadDone(ctx, ReadInfo{
		Key:       cm.Key,
		Bytes:     cm.FileSize,
		StartTime: start,
		Duration:  time.Since(start),
		Err:       err,
	})
	return err
}

type openCloser struct {
	transformer io.ReadCloser
	blob        io.ReadCloser
	hasher      hash.Hash
	want        string
	verify      bool

	// observability — populated by Open; ReadDone fires once on Close.
	ctx      context.Context
	observer Observer
	key      string
	fileSize int64
	start    time.Time
	msgCount int
	readErr  error
	fired    bool
}

func (c *openCloser) Close() error {
	if !c.fired {
		c.fired = true
		c.observer.ReadDone(c.ctx, ReadInfo{
			Key:       c.key,
			MsgCount:  c.msgCount,
			Bytes:     c.fileSize,
			Inline:    false,
			StartTime: c.start,
			Duration:  time.Since(c.start),
			Err:       c.readErr,
		})
	}
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
	d.closer.msgCount += n
	if err != nil && !d.checked {
		d.checked = true
		// On any terminal error (EOF or decode failure), verify the checksum
		// first. A checksum mismatch is the more informative error.
		if cerr := d.closer.verifyChecksum(); cerr != nil {
			d.closer.readErr = cerr
			return n, cerr
		}
		if !errors.Is(err, io.EOF) {
			d.closer.readErr = err
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
		var peek [1]byte
		n, err := l.r.Read(peek[:])
		if n > 0 || (err != nil && err != io.EOF) {
			return 0, ErrBatchTooLarge
		}
		return 0, io.EOF
	}
	if int64(len(p)) > l.remaining {
		p = p[:l.remaining]
	}
	n, err := l.r.Read(p)
	l.remaining -= int64(n)
	return n, err
}
