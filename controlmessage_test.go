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

// https://github.com/peczenyj/go-claimcheck/issues/55
func TestParseControlMessage_UnsupportedVersion(t *testing.T) {
	cm := claimcheck.ControlMessage{
		Version: "999", Key: "claimcheck/abc", MessageCount: 1,
		ContentType: "application/x-ndjson", FileSize: 10, Checksum: "deadbeef",
	}
	md := cm.ToMetadata("cc_")
	require.Equal(t, "999", md["cc_v"])

	_, ok := claimcheck.ParseControlMessage(md, "cc_")
	require.False(t, ok, "an unsupported envelope version must not parse as valid")
}

// https://github.com/peczenyj/go-claimcheck/issues/48
func TestHasControlMessageMetadata(t *testing.T) {
	valid := claimcheck.ControlMessage{Version: claimcheck.Version, Key: "k", MessageCount: 1}.ToMetadata("cc_")
	require.True(t, claimcheck.HasControlMessageMetadata(valid, "cc_"),
		"metadata carrying a version field is a claim-check envelope")

	// Version field present but the rest is garbage: still an envelope (just a broken one).
	corrupt := map[string]string{"cc_v": claimcheck.Version, "cc_msg_count": "not-a-number"}
	require.True(t, claimcheck.HasControlMessageMetadata(corrupt, "cc_"))

	// No version field under the prefix: a genuine inline message.
	require.False(t, claimcheck.HasControlMessageMetadata(map[string]string{"app": "x"}, "cc_"))
}
