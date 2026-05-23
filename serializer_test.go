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
