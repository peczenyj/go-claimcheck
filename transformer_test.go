package claimcheck_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

func TestGzipTransformer_RoundTrip(t *testing.T) {
	tr := claimcheck.NewGzipTransformer()
	require.Equal(t, "gzip", tr.ContentEncoding())

	var buf bytes.Buffer
	w, err := tr.WrapWriter(&buf)
	require.NoError(t, err)
	_, err = w.Write([]byte("payload"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	r, err := tr.WrapReader(&buf)
	require.NoError(t, err)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	require.Equal(t, "payload", string(got))
}

func TestNoopTransformer_RoundTrip(t *testing.T) {
	tr := claimcheck.NewNoopTransformer()
	require.Empty(t, tr.ContentEncoding())

	var buf bytes.Buffer
	w, _ := tr.WrapWriter(&buf)
	_, _ = w.Write([]byte("payload"))
	require.NoError(t, w.Close())

	r, _ := tr.WrapReader(&buf)
	got, _ := io.ReadAll(r)
	require.NoError(t, r.Close())
	require.Equal(t, "payload", string(got))
}
