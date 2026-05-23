package claimcheck_test

import (
	"bytes"
	"io"
	"testing"

	claimcheck "github.com/peczenyj/go-claimcheck"
	"github.com/stretchr/testify/require"
)

func drain(t *testing.T, dec claimcheck.Decoder, chunk int) []*claimcheck.Message {
	t.Helper()
	var out []*claimcheck.Message
	buf := make([]*claimcheck.Message, chunk)
	for {
		n, err := dec.Decode(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			return out
		}
		require.NoError(t, err)
	}
}

func TestJSONLines_ChunkedRoundTrip(t *testing.T) {
	s := claimcheck.NewJSONLinesSerializer()
	require.Equal(t, "application/x-ndjson", s.ContentType())

	msgs := []*claimcheck.Message{
		{Body: []byte("a")}, {Body: []byte("b")}, {Body: []byte("c")},
	}
	var buf bytes.Buffer
	require.NoError(t, s.Encode(&buf, msgs))

	got := drain(t, s.NewDecoder(&buf, 0), 2)
	require.Len(t, got, 3)
	require.Equal(t, "a", string(got[0].Body))
	require.Equal(t, "c", string(got[2].Body))
}

func TestLengthPrefixed_ChunkedRoundTrip(t *testing.T) {
	s := claimcheck.NewLengthPrefixedSerializer()
	require.Equal(t, "application/octet-stream", s.ContentType())

	msgs := []*claimcheck.Message{{Body: []byte("x")}, {Body: []byte("yy")}}
	var buf bytes.Buffer
	require.NoError(t, s.Encode(&buf, msgs))

	got := drain(t, s.NewDecoder(&buf, 0), 1)
	require.Len(t, got, 2)
	require.Equal(t, "yy", string(got[1].Body))
}

func TestLengthPrefixed_MaxMessageSize(t *testing.T) {
	s := claimcheck.NewLengthPrefixedSerializer()
	var buf bytes.Buffer
	require.NoError(t, s.Encode(&buf, []*claimcheck.Message{{Body: []byte("a long-ish body")}}))

	dec := s.NewDecoder(&buf, 4) // far below the encoded record size
	out := make([]*claimcheck.Message, 1)
	_, err := dec.Decode(out)
	require.ErrorIs(t, err, claimcheck.ErrMessageTooLarge)
}
