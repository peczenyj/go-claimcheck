package extpubsub_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/peczenyj/go-claimcheck/extpubsub"
)

func TestLengthPrefixedSerializer(t *testing.T) {
	s := extpubsub.NewLengthPrefixedSerializer()

	t.Run("ContentType", func(t *testing.T) {
		assert.Equal(t, "application/octet-stream", s.ContentType())
	})

	t.Run("RoundTrip", func(t *testing.T) {
		msgs := []*extpubsub.Message{
			{Body: []byte("hello"), Metadata: map[string]string{"a": "b"}},
			{Body: []byte("world")},
		}

		var buf bytes.Buffer
		err := s.Encode(&buf, msgs)
		require.NoError(t, err)

		decoded, err := s.Decode(&buf)
		require.NoError(t, err)
		require.Len(t, decoded, len(msgs))
		assert.Equal(t, msgs[0].Body, decoded[0].Body)
		assert.Equal(t, msgs[0].Metadata["a"], decoded[0].Metadata["a"])
		assert.Equal(t, msgs[1].Body, decoded[1].Body)
	})

	t.Run("DecodeEmpty", func(t *testing.T) {
		decoded, err := s.Decode(bytes.NewReader(nil))
		require.NoError(t, err)
		assert.Empty(t, decoded)
	})

	t.Run("DecodeInvalidLength", func(t *testing.T) {
		// 4 bytes for length, but negative
		data := []byte{0xff, 0xff, 0xff, 0xff}
		_, err := s.Decode(bytes.NewReader(data))
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("DecodeShortBody", func(t *testing.T) {
		// Length 10, but only 5 bytes available
		data := []byte{0, 0, 0, 10, 1, 2, 3, 4, 5}
		_, err := s.Decode(bytes.NewReader(data))
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("DecodeMalformedJSON", func(t *testing.T) {
		// Length 5, but body is not JSON
		data := []byte{0, 0, 0, 5, 'n', 'o', 't', 'j', 's'}
		_, err := s.Decode(bytes.NewReader(data))
		assert.Error(t, err)
	})
}

func TestJSONLinesSerializer_Errors(t *testing.T) {
	s := extpubsub.NewJSONLinesSerializer()

	t.Run("DecodeMalformed", func(t *testing.T) {
		data := []byte(`{"body": "YmFzZTY0"}
invalid json
`)
		_, err := s.Decode(bytes.NewReader(data))
		assert.Error(t, err)
	})
}
