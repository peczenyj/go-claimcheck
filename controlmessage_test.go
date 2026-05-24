package claimcheck_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

func TestControlMessage_RoundTrip(t *testing.T) {
	cm := claimcheck.ControlMessage{
		Version:         claimcheck.Version,
		Key:             "claimcheck/abc",
		MessageCount:    3,
		ContentType:     "application/x-ndjson",
		ContentEncoding: "gzip",
		FileSize:        42,
		Checksum:        "deadbeef",
	}

	md := cm.ToMetadata("cc_")
	require.Equal(t, "1", md["cc_v"])
	require.Equal(t, "claimcheck/abc", md["cc_key"])
	require.Equal(t, "3", md["cc_msg_count"])

	got, ok := claimcheck.ParseControlMessage(md, "cc_")
	require.True(t, ok)
	require.Equal(t, cm, got)
}

func TestParseControlMessage_NotClaimCheck(t *testing.T) {
	_, ok := claimcheck.ParseControlMessage(map[string]string{"foo": "bar"}, "cc_")
	require.False(t, ok)
}
