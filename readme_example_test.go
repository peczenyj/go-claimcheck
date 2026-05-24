package claimcheck_test

import (
	"compress/flate"
	"context"
	"fmt"
	"io"
	"log"

	"gocloud.dev/blob/memblob"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

// flateTransformer is a user-defined claimcheck.Transformer backed by the
// standard library DEFLATE codec. Implementing these three methods is all it
// takes to plug in any codec — for example Brotli via
// github.com/andybalholm/brotli.
type flateTransformer struct{}

func (flateTransformer) ContentEncoding() string { return "deflate" }

func (flateTransformer) WrapWriter(w io.Writer) (io.WriteCloser, error) {
	return flate.NewWriter(w, flate.DefaultCompression)
}

func (flateTransformer) WrapReader(r io.Reader) (io.ReadCloser, error) {
	return flate.NewReader(r), nil
}

// Example_customTransformer offloads and reads a batch using the user-defined
// flateTransformer above, demonstrating that compression is fully pluggable.
func Example_customTransformer() {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)

	opts := claimcheck.Options{Transformer: flateTransformer{}}

	cm, err := claimcheck.Offload(ctx, bucket, opts,
		[]*claimcheck.Message{{Body: []byte("compressed with your own codec")}})
	if err != nil {
		log.Fatal(err)
	}

	msgs, err := claimcheck.Read(ctx, bucket, cm, opts)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s (encoding=%s)", msgs[0].Body, cm.ContentEncoding)
	// Output: compressed with your own codec (encoding=deflate)
}

// Example_coreAPI uses the low-level core directly: offload a batch to a blob,
// hand the control-message metadata to any transport, then parse it and read
// the blob back on the other side.
func Example_coreAPI() {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)

	opts := claimcheck.Options{KeyPrefix: "claimcheck/"}

	// Producer: write the batch to a blob, get the control message.
	cm, err := claimcheck.Offload(ctx, bucket, opts,
		[]*claimcheck.Message{{Body: []byte("alpha")}, {Body: []byte("beta")}})
	if err != nil {
		log.Fatal(err)
	}
	metadata := cm.ToMetadata(opts.MetadataPrefix) // attach to your pubsub message

	// Consumer: parse the metadata you received, then read the blob back.
	parsed, ok := claimcheck.ParseControlMessage(metadata, opts.MetadataPrefix)
	if !ok {
		log.Fatal("not a claim-check message")
	}
	msgs, err := claimcheck.Read(ctx, bucket, parsed, opts)
	if err != nil {
		log.Fatal(err)
	}
	_ = claimcheck.Delete(ctx, bucket, parsed) // optional cleanup

	fmt.Printf("%s %s", msgs[0].Body, msgs[1].Body)
	// Output: alpha beta
}
