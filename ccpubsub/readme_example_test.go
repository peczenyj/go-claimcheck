package ccpubsub_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub/mempubsub"

	claimcheck "github.com/peczenyj/go-claimcheck"
	"github.com/peczenyj/go-claimcheck/ccpubsub"
)

// Example_zeroConfiguration is the simplest possible use of the Pub/Sub
// integration layer: wrap a topic and subscription with all defaults (JSON
// Lines, no compression, "claimcheck_" metadata prefix), then send and receive.
func Example_zeroConfiguration() {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	baseTopic := mempubsub.NewTopic()
	baseSub := mempubsub.NewSubscription(baseTopic, time.Second)

	topic := ccpubsub.WrapTopic(baseTopic, bucket, ccpubsub.TopicOptions{})
	sub := ccpubsub.WrapSubscription(baseSub, bucket, ccpubsub.SubscriptionOptions{})

	if err := topic.Send(ctx, &claimcheck.Message{Body: []byte("hello")}); err != nil {
		log.Fatal(err)
	}
	if err := topic.Shutdown(ctx); err != nil { // flushes the buffered batch
		log.Fatal(err)
	}

	batch, err := sub.Receive(ctx)
	if err != nil {
		log.Fatal(err)
	}
	msgs, err := batch.Read(ctx)
	if err != nil {
		log.Fatal(err)
	}
	batch.Ack()

	fmt.Printf("%s", msgs[0].Body)
	// Output: hello
}

// Example_customizingTheIntegrationLayer keeps the same wrappers but tunes the
// options: compress blobs with Zstd, namespace the blob keys, verify checksums
// on read, and flush after 100 buffered messages (or every two seconds).
func Example_customizingTheIntegrationLayer() {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	baseTopic := mempubsub.NewTopic()
	baseSub := mempubsub.NewSubscription(baseTopic, time.Second)

	// Producer and consumer share the same options
	// (see "The bucket-binding contract").
	opts := claimcheck.Options{
		Transformer:    claimcheck.NewZstdTransformer(),
		KeyPrefix:      "claimcheck/",
		VerifyChecksum: true,
	}

	topic := ccpubsub.WrapTopic(baseTopic, bucket, ccpubsub.TopicOptions{
		Options:       opts,
		MaxMessages:   100,
		FlushInterval: 2 * time.Second,
	})
	sub := ccpubsub.WrapSubscription(baseSub, bucket, ccpubsub.SubscriptionOptions{Options: opts})

	for _, body := range []string{"one", "two", "three"} {
		if err := topic.Send(ctx, &claimcheck.Message{Body: []byte(body)}); err != nil {
			log.Fatal(err)
		}
	}
	if err := topic.Shutdown(ctx); err != nil { // flushes the three buffered messages as one blob
		log.Fatal(err)
	}

	batch, err := sub.Receive(ctx)
	if err != nil {
		log.Fatal(err)
	}
	msgs, err := batch.Read(ctx)
	if err != nil {
		log.Fatal(err)
	}
	batch.Ack()

	fmt.Printf("%d messages: %s %s %s", len(msgs), msgs[0].Body, msgs[1].Body, msgs[2].Body)
	// Output: 3 messages: one two three
}
