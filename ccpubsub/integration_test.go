//go:build integration

// Integration tests for the claim-check round-trip against real infrastructure.
//
// There are three tests:
//
//   - TestIntegrationKafka     — Kafka (Redpanda) for both publish and consume,
//     MinIO for blob storage. Always runs (requires Docker).
//   - TestIntegrationRabbitMQ  — RabbitMQ for both publish and consume, MinIO
//     for blob storage. Always runs (requires Docker).
//   - TestIntegrationExternal  — uses real backends provided via environment
//     variables. Skipped unless CLAIMCHECK_IT_PUBSUB_URL is set.
//
// The external test reads:
//
//	CLAIMCHECK_IT_PUBSUB_URL     pubsub URL used for BOTH publish and consume
//	                             (e.g. rabbit://my-queue, kafka://my-topic, mem://t).
//	                             Required; the test is skipped when empty.
//	CLAIMCHECK_IT_BLOB_URL       blob bucket URL (e.g. s3://bucket?..., file:///p).
//	                             Defaults to mem:// when empty.
//	CLAIMCHECK_IT_MESSAGE_COUNT  number of messages to push (default 1024).
//	                             Only affects TestIntegrationExternal.
//
// CLAIMCHECK_IT_PUBSUB_URL is passed to both pubsub.OpenTopic and
// pubsub.OpenSubscription, so it must be valid as both for the chosen driver.
package ccpubsub_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"
	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/kafkapubsub"
	_ "gocloud.dev/pubsub/mempubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"

	claimcheck "github.com/peczenyj/go-claimcheck"
	"github.com/peczenyj/go-claimcheck/ccpubsub"
)

const (
	defaultMessageCount = 1024
	progressInterval    = 128
	resourceName        = "claimcheck-it"
	receiveTimeout      = 60 * time.Second
)

// TestIntegrationKafka round-trips messages through a Redpanda (Kafka) container
// with MinIO as the blob store.
func TestIntegrationKafka(t *testing.T) {
	blobURL := startMinIO(t)
	topicURL, subURL := startRedpanda(t)
	runRoundTrip(t, topicURL, subURL, blobURL, defaultMessageCount)
}

// TestIntegrationRabbitMQ round-trips messages through a RabbitMQ container with
// MinIO as the blob store.
func TestIntegrationRabbitMQ(t *testing.T) {
	blobURL := startMinIO(t)
	topicURL, subURL := startRabbitMQ(t)
	runRoundTrip(t, topicURL, subURL, blobURL, defaultMessageCount)
}

// TestIntegrationExternal round-trips messages through the real backends named
// by CLAIMCHECK_IT_PUBSUB_URL (publish + consume) and CLAIMCHECK_IT_BLOB_URL
// (blob store, defaulting to mem://). It is skipped when CLAIMCHECK_IT_PUBSUB_URL
// is unset.
func TestIntegrationExternal(t *testing.T) {
	pubsubURL := os.Getenv("CLAIMCHECK_IT_PUBSUB_URL")
	if pubsubURL == "" {
		t.Skip("CLAIMCHECK_IT_PUBSUB_URL not set; skipping external integration test")
	}

	blobURL := os.Getenv("CLAIMCHECK_IT_BLOB_URL")
	if blobURL == "" {
		blobURL = "mem://"
	}

	count := defaultMessageCount
	if v := os.Getenv("CLAIMCHECK_IT_MESSAGE_COUNT"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err)
		count = n
	}

	runRoundTrip(t, pubsubURL, pubsubURL, blobURL, count)
}

// runRoundTrip opens the topic, subscription, and bucket from their URLs, wraps
// the topic/subscription with the claim-check offload (one blob per message via
// MaxMessages: 1), publishes count messages, and verifies they all come back.
func runRoundTrip(t *testing.T, topicURL, subURL, blobURL string, count int) {
	t.Helper()
	ctx := context.Background()

	bucket, err := blob.OpenBucket(ctx, blobURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bucket.Close() })

	topic, err := pubsub.OpenTopic(ctx, topicURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = topic.Shutdown(ctx) })

	sub, err := pubsub.OpenSubscription(ctx, subURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Shutdown(ctx) })

	// MaxMessages: 1 offloads every message to its own blob and publishes one
	// control message per message, mirroring the per-message offload semantics.
	ccTopic := ccpubsub.WrapTopic(topic, bucket, ccpubsub.TopicOptions{MaxMessages: 1})
	ccSub := ccpubsub.WrapSubscription(sub, bucket, claimcheck.Options{})

	publishMessages(t, ccTopic, count)
	consumeMessages(t, ccSub, count)
}

// publishMessages sends count JSON messages through the wrapped topic, logging
// progress every progressInterval messages, then flushes any buffered remainder.
func publishMessages(t *testing.T, ccTopic ccpubsub.Topic, count int) {
	t.Helper()
	ctx := context.Background()

	for i := 0; i < count; i++ {
		body, err := json.Marshal(map[string]any{
			"foo":       "bar",
			"timestamp": time.Now().Unix(),
			"random":    rand.Int64(),
		})
		require.NoError(t, err)
		require.NoError(t, ccTopic.Send(ctx, &claimcheck.Message{Body: body}))

		if n := i + 1; n%progressInterval == 0 || n == count {
			t.Logf("writing %d of %d messages", n, count)
		}
	}

	require.NoError(t, ccTopic.Flush(ctx))
}

// consumeMessages receives and reads batches from the wrapped subscription until
// count messages have been collected, logging progress every progressInterval
// messages and verifying each decodes to the expected shape.
func consumeMessages(t *testing.T, ccSub ccpubsub.Subscription, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), receiveTimeout)
	defer cancel()

	received := 0
	for received < count {
		batch, err := ccSub.Receive(ctx)
		require.NoError(t, err)

		msgs, err := batch.Read(ctx)
		require.NoError(t, err)

		for _, m := range msgs {
			var payload struct {
				Foo       string `json:"foo"`
				Timestamp int64  `json:"timestamp"`
				Random    int64  `json:"random"`
			}
			require.NoError(t, json.Unmarshal(m.Body, &payload))
			require.Equal(t, "bar", payload.Foo)
			received++

			if received%progressInterval == 0 || received == count {
				t.Logf("reading %d of %d messages", received, count)
			}
		}
		batch.Ack()
	}

	require.Equal(t, count, received)
}

// startRedpanda boots a Redpanda (Kafka-compatible) container, sets KAFKA_BROKERS,
// and returns the topic URL and subscription URL.
func startRedpanda(t *testing.T) (topicURL, subURL string) {
	t.Helper()
	ctx := context.Background()

	c, err := redpanda.Run(ctx,
		"redpandadata/redpanda:v24.2.7",
		testcontainers.WithLogger(log.TestLogger(t)),
	)
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)

	broker, err := c.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	t.Setenv("KAFKA_BROKERS", broker)

	// kafkapubsub never declares topology and Redpanda does not reliably
	// auto-create the topic on first produce, so create it up front.
	createKafkaTopic(t, broker, resourceName)

	return "kafka://" + resourceName,
		"kafka://" + resourceName + "-group?topic=" + resourceName + "&offset=oldest"
}

func createKafkaTopic(t *testing.T, broker, topic string) {
	t.Helper()

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0

	admin, err := sarama.NewClusterAdmin([]string{broker}, cfg)
	require.NoError(t, err)
	defer func() { _ = admin.Close() }()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		require.NoError(t, err)
	}
}

// startRabbitMQ boots a RabbitMQ container, sets RABBIT_SERVER_URL, declares a
// fanout exchange + queue + binding (rabbitpubsub does not declare topology),
// and returns the topic (exchange) URL and subscription (queue) URL.
func startRabbitMQ(t *testing.T) (topicURL, subURL string) {
	t.Helper()
	ctx := context.Background()

	c, err := rabbitmq.Run(ctx,
		"rabbitmq:3.13-management-alpine",
		testcontainers.WithLogger(log.TestLogger(t)),
	)
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)

	amqpURL, err := c.AmqpURL(ctx)
	require.NoError(t, err)
	t.Setenv("RABBIT_SERVER_URL", amqpURL)

	declareRabbitTopology(t, amqpURL)

	return "rabbit://" + resourceName, "rabbit://" + resourceName
}

func declareRabbitTopology(t *testing.T, amqpURL string) {
	t.Helper()

	conn, err := amqp091.Dial(amqpURL)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	ch, err := conn.Channel()
	require.NoError(t, err)
	defer func() { _ = ch.Close() }()

	require.NoError(t, ch.ExchangeDeclare(resourceName, "fanout", true, false, false, false, nil))
	_, err = ch.QueueDeclare(resourceName, true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind(resourceName, "", resourceName, false, nil))
}

// startMinIO boots a MinIO container, sets AWS_* env vars, creates the bucket,
// and returns an s3:// URL pointing at it (path-style, plaintext HTTP).
func startMinIO(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	c, err := minio.Run(ctx,
		"minio/minio:RELEASE.2024-12-18T13-15-44Z",
		testcontainers.WithLogger(log.TestLogger(t)),
	)
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)

	endpoint, err := c.ConnectionString(ctx)
	require.NoError(t, err)

	t.Setenv("AWS_ACCESS_KEY_ID", c.Username)
	t.Setenv("AWS_SECRET_ACCESS_KEY", c.Password)
	t.Setenv("AWS_REGION", "us-east-1")

	createS3Bucket(t, endpoint, c.Username, c.Password)

	// The gocloud s3blob opener uses the "endpoint" value verbatim as the AWS
	// SDK endpoint URL, so it must carry an explicit scheme; without one the SDK
	// treats "localhost" as the scheme and drops the host. The value is
	// URL-encoded so the "http://" survives query-string parsing.
	return fmt.Sprintf(
		"s3://%s?region=us-east-1&endpoint=%s&disable_https=true&use_path_style=true",
		resourceName, url.QueryEscape("http://"+endpoint),
	)
}

func createS3Bucket(t *testing.T, endpoint, accessKey, secretKey string) {
	t.Helper()
	ctx := context.Background()

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://" + endpoint)
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(resourceName),
	})
	require.NoError(t, err)
}
