package claimcheck

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
)

// ErrMessageTooLarge is returned when a message exceeds the configured size.
var ErrMessageTooLarge = errors.New("claimcheck: message exceeds MaxMessageSize")

// Serializer encodes a batch of messages and creates streaming decoders.
type Serializer interface {
	Encode(w io.Writer, msgs []*Message) error
	// NewDecoder returns a stateful decoder reading from r. maxMessageSize is
	// the per-message byte cap (0 = unlimited); serializers that cannot see
	// per-message lengths may ignore it.
	NewDecoder(r io.Reader, maxMessageSize int) Decoder
	ContentType() string
}

// Decoder decodes messages from a stream in caller-bounded chunks.
type Decoder interface {
	// Decode fills buf with up to len(buf) messages and returns the count.
	// It returns io.EOF when the stream is exhausted.
	Decode(buf []*Message) (int, error)
}

// JSONLinesSerializer encodes messages as NDJSON.
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

func (s *JSONLinesSerializer) NewDecoder(r io.Reader, _ int) Decoder {
	return &jsonLinesDecoder{dec: json.NewDecoder(r)}
}

type jsonLinesDecoder struct {
	dec *json.Decoder
}

func (d *jsonLinesDecoder) Decode(buf []*Message) (int, error) {
	n := 0
	for n < len(buf) {
		if !d.dec.More() {
			return n, io.EOF
		}
		var m Message
		if err := d.dec.Decode(&m); err != nil {
			return n, err
		}
		buf[n] = &m
		n++
	}
	return n, nil
}

// LengthPrefixedSerializer encodes messages with a uint32 big-endian length prefix.
type LengthPrefixedSerializer struct{}

func NewLengthPrefixedSerializer() *LengthPrefixedSerializer { return &LengthPrefixedSerializer{} }

func (s *LengthPrefixedSerializer) ContentType() string { return "application/octet-stream" }

func (s *LengthPrefixedSerializer) Encode(w io.Writer, msgs []*Message) error {
	for _, m := range msgs {
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		if uint64(len(data)) > math.MaxUint32 {
			return ErrMessageTooLarge
		}
		if err := binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func (s *LengthPrefixedSerializer) NewDecoder(r io.Reader, maxMessageSize int) Decoder {
	return &lengthPrefixedDecoder{r: r, max: maxMessageSize}
}

type lengthPrefixedDecoder struct {
	r   io.Reader
	max int
}

func (d *lengthPrefixedDecoder) Decode(buf []*Message) (int, error) {
	n := 0
	for n < len(buf) {
		var length uint32
		if err := binary.Read(d.r, binary.BigEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				return n, io.EOF
			}
			return n, err
		}
		if d.max > 0 && int64(length) > int64(d.max) {
			return n, ErrMessageTooLarge
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(d.r, data); err != nil {
			return n, err
		}
		var m Message
		if err := json.Unmarshal(data, &m); err != nil {
			return n, err
		}
		buf[n] = &m
		n++
	}
	return n, nil
}
