package extpubsub

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerializers(t *testing.T) {
	msgs := []*Message{
		{Body: []byte("hello"), Metadata: map[string]string{"foo": "bar"}},
		{Body: []byte("world"), Metadata: map[string]string{"baz": "qux"}},
	}

	serializers := []struct {
		name string
		s    Serializer
	}{
		{"JSONLines", NewJSONLinesSerializer()},
		{"LengthPrefixed", NewLengthPrefixedSerializer()},
	}

	for _, tt := range serializers {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := tt.s.Encode(&buf, msgs)
			assert.NoError(t, err)

			decoded, err := tt.s.Decode(&buf)
			assert.NoError(t, err)
			assert.Equal(t, msgs, decoded)
			assert.NotEmpty(t, tt.s.ContentType())
		})
	}
}

func TestGzipTransformer(t *testing.T) {
	transformer := NewGzipTransformer()
	data := []byte("some data to compress and decompress")

	var buf bytes.Buffer
	w, err := transformer.WrapWriter(&buf)
	assert.NoError(t, err)
	_, err = w.Write(data)
	assert.NoError(t, err)
	err = w.Close()
	assert.NoError(t, err)

	assert.NotEqual(t, data, buf.Bytes()) // Should be compressed

	r, err := transformer.WrapReader(&buf)
	assert.NoError(t, err)
	decoded := new(bytes.Buffer)
	_, err = io.Copy(decoded, r)
	assert.NoError(t, err)
	err = r.Close()
	assert.NoError(t, err)

	assert.Equal(t, data, decoded.Bytes())
	assert.Equal(t, "gzip", transformer.ContentEncoding())
}
