package extpubsub

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"io"
)

// JSONLinesSerializer implements Serializer using NDJSON.
type JSONLinesSerializer struct{}

func NewJSONLinesSerializer() *JSONLinesSerializer { return &JSONLinesSerializer{} }

func (s *JSONLinesSerializer) ContentType() string { return "application/x-ndjson" }

func (s *JSONLinesSerializer) Encode(w io.Writer, msgs []*Message) error {
	enc := json.NewEncoder(w)
	for _, m := range msgs {
		if err := enc.Encode(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *JSONLinesSerializer) Decode(r io.Reader) ([]*Message, error) {
	var msgs []*Message
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		var m Message
		if err := json.Unmarshal(scanner.Bytes(), &m); err != nil {
			return nil, err
		}
		msgs = append(msgs, &m)
	}
	return msgs, scanner.Err()
}

// LengthPrefixedSerializer implements Serializer using binary length prefixes.
type LengthPrefixedSerializer struct{}

func NewLengthPrefixedSerializer() *LengthPrefixedSerializer { return &LengthPrefixedSerializer{} }

func (s *LengthPrefixedSerializer) ContentType() string { return "application/octet-stream" }

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

func (s *LengthPrefixedSerializer) Decode(r io.Reader) ([]*Message, error) {
	var msgs []*Message
	for {
		var length int32
		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
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

// GzipTransformer implements Gzip compression.
type GzipTransformer struct{}

func NewGzipTransformer() *GzipTransformer { return &GzipTransformer{} }

func (t *GzipTransformer) ContentEncoding() string { return "gzip" }

func (t *GzipTransformer) WrapWriter(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}

func (t *GzipTransformer) WrapReader(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}
