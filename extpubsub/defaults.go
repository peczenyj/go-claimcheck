package extpubsub

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

// JSONLinesSerializer implements Serializer using NDJSON.
type JSONLinesSerializer struct{}

// NewJSONLinesSerializer creates a new JSONLinesSerializer.
func NewJSONLinesSerializer() *JSONLinesSerializer { return &JSONLinesSerializer{} }

// ContentType returns the MIME type for NDJSON.
func (s *JSONLinesSerializer) ContentType() string { return "application/x-ndjson" }

// Encode serializes messages as JSON lines.
func (s *JSONLinesSerializer) Encode(w io.Writer, msgs []*Message) error {
	enc := json.NewEncoder(w)
	for _, m := range msgs {
		if err := enc.Encode(m); err != nil {
			return err
		}
	}
	return nil
}

// Decode deserializes messages from JSON lines.
func (s *JSONLinesSerializer) Decode(r io.Reader) ([]*Message, error) {
	var msgs []*Message
	dec := json.NewDecoder(r)
	for dec.More() {
		var m Message
		if err := dec.Decode(&m); err != nil {
			return nil, err
		}
		msgs = append(msgs, &m)
	}
	return msgs, nil
}

// LengthPrefixedSerializer implements Serializer using binary length prefixes.
type LengthPrefixedSerializer struct{}

// NewLengthPrefixedSerializer creates a new LengthPrefixedSerializer.
func NewLengthPrefixedSerializer() *LengthPrefixedSerializer { return &LengthPrefixedSerializer{} }

// ContentType returns the MIME type for binary data.
func (s *LengthPrefixedSerializer) ContentType() string { return "application/octet-stream" }

// Encode serializes messages with a 4-byte big-endian length prefix.
func (s *LengthPrefixedSerializer) Encode(w io.Writer, msgs []*Message) error {
	for _, m := range msgs {
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		if err := binary.Write(w, binary.BigEndian, int32(len(data))); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// Decode deserializes messages with length prefixes.
func (s *LengthPrefixedSerializer) Decode(r io.Reader) ([]*Message, error) {
	var msgs []*Message
	for {
		var length int32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if length < 0 {
			return nil, io.ErrUnexpectedEOF
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}
		var m Message
		if err := json.Unmarshal(data, &m); err != nil {
			return nil, err
		}
		msgs = append(msgs, &m)
	}
	return msgs, nil
}

// NoopTransformer does nothing.
type NoopTransformer struct{}

// NewNoopTransformer creates a new NoopTransformer.
func NewNoopTransformer() *NoopTransformer { return &NoopTransformer{} }

// ContentEncoding returns an empty string.
func (t *NoopTransformer) ContentEncoding() string { return "" }

// WrapWriter returns the writer as is.
func (t *NoopTransformer) WrapWriter(w io.Writer) (io.WriteCloser, error) {
	return nopWriteCloser{w}, nil
}

// WrapReader returns the reader as is.
func (t *NoopTransformer) WrapReader(r io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(r), nil
}

type nopWriteCloser struct{ io.Writer }

func (n nopWriteCloser) Close() error { return nil }

// GzipTransformer implements Gzip compression.
type GzipTransformer struct{}

// NewGzipTransformer creates a new GzipTransformer.
func NewGzipTransformer() *GzipTransformer { return &GzipTransformer{} }

// ContentEncoding returns "gzip".
func (t *GzipTransformer) ContentEncoding() string { return "gzip" }

// WrapWriter returns a gzip writer.
func (t *GzipTransformer) WrapWriter(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}

// WrapReader returns a gzip reader.
func (t *GzipTransformer) WrapReader(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}
