package claimcheck_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

func TestOptions_SetDefaults(t *testing.T) {
	var o claimcheck.Options
	o.SetDefaults()

	require.NotNil(t, o.Serializer)
	require.NotNil(t, o.Transformer)
	require.Equal(t, "claimcheck_", o.MetadataPrefix)
	require.NotNil(t, o.KeyFunc)

	k1 := o.KeyFunc(nil)
	k2 := o.KeyFunc(nil)
	require.NotEmpty(t, k1)
	require.NotEqual(t, k1, k2, "default KeyFunc must yield unique keys")
}

func TestOptions_SetDefaults_PreservesExplicit(t *testing.T) {
	o := claimcheck.Options{MetadataPrefix: "x_", KeyPrefix: "p/"}
	o.SetDefaults()
	require.Equal(t, "x_", o.MetadataPrefix)
	require.Equal(t, "p/", o.KeyPrefix)
}

func TestOptions_MaxDownloadSizeFallback(t *testing.T) {
	// Fallback from deprecated MaxBatchSize
	o := claimcheck.Options{MaxBatchSize: 123}
	o.SetDefaults()
	require.Equal(t, 123, o.MaxDownloadSize)

	// Explicit MaxDownloadSize wins
	o = claimcheck.Options{MaxDownloadSize: 456, MaxBatchSize: 123}
	o.SetDefaults()
	require.Equal(t, 456, o.MaxDownloadSize)
}
