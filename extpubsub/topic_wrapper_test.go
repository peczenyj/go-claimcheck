package extpubsub_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
	"gocloud.dev/pubsub"

	"github.com/peczenyj/go-claimcheck/extpubsub"
)

// failingSerializer always errors on Encode, to exercise WrapTopic's error path.
type failingSerializer struct{}

func (failingSerializer) Encode(io.Writer, []*extpubsub.Message) error {
	return errors.New("boom")
}

func (failingSerializer) Decode(io.Reader) ([]*extpubsub.Message, error) {
	return nil, errors.New("boom")
}

func (failingSerializer) ContentType() string { return "application/x-fail" }

func TestWrapTopic_OffloadRoundTrip(t *testing.T) {
	ctx := context.Background()
	drv := &extpubsub.MemDriver{}
	bucket := memblob.OpenBucket(nil)
	opts := extpubsub.Options{} // MinSize 0 => always offload

	rawTopic := pubsub.NewTopic(drv, nil)
	extTopic := extpubsub.WrapTopic(rawTopic, bucket, opts)

	require.NoError(t, extTopic.Send(ctx, &pubsub.Message{Body: []byte("hello")}))

	rawSub := pubsub.NewSubscription(drv, nil, nil)
	wrapSub := extpubsub.WrapSubscription(rawSub, bucket, opts)

	batch, err := wrapSub.ReceiveBatch(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch.URL)
	require.Equal(t, 1, batch.MessageCount)

	msgs, err := batch.Unroll(ctx)
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	require.Equal(t, []byte("hello"), msgs[0].Body)
	batch.Ack()
}

func TestWrapTopic_PassthroughBelowThreshold(t *testing.T) {
	ctx := context.Background()
	drv := &extpubsub.MemDriver{}
	bucket := memblob.OpenBucket(nil)
	opts := extpubsub.Options{MinSize: 1024}

	rawTopic := pubsub.NewTopic(drv, nil)
	extTopic := extpubsub.WrapTopic(rawTopic, bucket, opts)

	require.NoError(t, extTopic.Send(ctx, &pubsub.Message{Body: []byte("small")}))

	msgs := drv.Messages()
	require.Len(t, msgs, 1)
	require.Equal(t, []byte("small"), msgs[0].Body)
	require.NotContains(t, msgs[0].Metadata, "extpubsub_v")
}

func TestWrapTopic_OffloadError(t *testing.T) {
	ctx := context.Background()
	drv := &extpubsub.MemDriver{}
	bucket := memblob.OpenBucket(nil)
	opts := extpubsub.Options{Serializer: failingSerializer{}}

	rawTopic := pubsub.NewTopic(drv, nil)
	extTopic := extpubsub.WrapTopic(rawTopic, bucket, opts)

	require.Error(t, extTopic.Send(ctx, &pubsub.Message{Body: []byte("data")}))
}
