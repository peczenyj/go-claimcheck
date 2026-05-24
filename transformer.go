package claimcheck

import (
	"compress/gzip"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Transformer is middleware for the stored blob bytes (e.g. compression).
type Transformer interface {
	WrapWriter(w io.Writer) (io.WriteCloser, error)
	WrapReader(r io.Reader) (io.ReadCloser, error)
	ContentEncoding() string
}

// NoopTransformer passes bytes through unchanged.
type NoopTransformer struct{}

func NewNoopTransformer() *NoopTransformer { return &NoopTransformer{} }

func (t *NoopTransformer) ContentEncoding() string { return "" }

func (t *NoopTransformer) WrapWriter(w io.Writer) (io.WriteCloser, error) {
	return nopWriteCloser{w}, nil
}

func (t *NoopTransformer) WrapReader(r io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(r), nil
}

type nopWriteCloser struct{ io.Writer }

func (n nopWriteCloser) Close() error { return nil }

// GzipTransformer compresses the blob with gzip.
type GzipTransformer struct{}

func NewGzipTransformer() *GzipTransformer { return &GzipTransformer{} }

func (t *GzipTransformer) ContentEncoding() string { return "gzip" }

func (t *GzipTransformer) WrapWriter(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}

func (t *GzipTransformer) WrapReader(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

// ZstdTransformer compresses the blob with zstd.
type ZstdTransformer struct{}

func NewZstdTransformer() *ZstdTransformer { return &ZstdTransformer{} }

func (t *ZstdTransformer) ContentEncoding() string { return "zstd" }

func (t *ZstdTransformer) WrapWriter(w io.Writer) (io.WriteCloser, error) {
	return zstd.NewWriter(w)
}

func (t *ZstdTransformer) WrapReader(r io.Reader) (io.ReadCloser, error) {
	decoder, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}

	return decoder.IOReadCloser(), nil
}
