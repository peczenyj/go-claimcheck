//go:build integration

// Integration tests for the claim-check round-trip against real infrastructure.
//
// Each backend is resolved through a Go CDK URL opener and can be substituted
// with a real backend via environment variables; when a variable is unset, a
// testcontainer is started instead.
//
//	CLAIMCHECK_IT_INPUT          topic URL to publish to
//	                             (e.g. kafka://my-topic, rabbit://my-exchange)
//	CLAIMCHECK_IT_OUTPUT         subscription URL to consume from
//	                             (e.g. kafka://my-group?topic=my-topic&offset=oldest,
//	                              rabbit://my-queue)
//	CLAIMCHECK_IT_BLOB_URL       blob bucket URL
//	                             (e.g. s3://bucket?region=..&endpoint=..&use_path_style=true,
//	                              mem://, file:///path)
//	CLAIMCHECK_IT_BROKER         which broker the fallback starts: "kafka" (default) or "rabbitmq"
//	CLAIMCHECK_IT_MESSAGE_COUNT  number of messages to push (default 1000)
//
// INPUT and OUTPUT describe the two ends of ONE broker (one round-trip per run).
// The gocloud openers also read KAFKA_BROKERS / RABBIT_SERVER_URL / AWS_* which
// the container helpers set automatically in fallback mode.
package extpubsub_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"os"
	"strconv"
	"strings"
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
	"gocloud.dev/pubsub"

	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/memblob"
	_ "gocloud.dev/blob/s3blob"
	_ "gocloud.dev/pubsub/kafkapubsub"
	_ "gocloud.dev/pubsub/rabbitpubsub"

	"github.com/peczenyj/go-claimcheck/extpubsub"
)

const (
	defaultMessageCount = 1000
	resourceName        = "claimcheck-it"
	receiveTimeout      = 60 * time.Second
)

func TestIntegration(t *testing.T) {
	ctx := context.Background()

	blobURL := os.Getenv("CLAIMCHECK_IT_BLOB_URL")
	if blobURL == "" {
		blobURL = startMinIO(t)
	}

	inURL := os.Getenv("CLAIMCHECK_IT_INPUT")
	outURL := os.Getenv("CLAIMCHECK_IT_OUTPUT")
	if inURL == "" || outURL == "" {
		switch strings.ToLower(os.Getenv("CLAIMCHECK_IT_BROKER")) {
		case "rabbitmq", "rabbit":
			inURL, outURL = startRabbitMQ(t)
		default:
			inURL, outURL = startRedpanda(t)
		}
	}

	count := defaultMessageCount
	if v := os.Getenv("CLAIMCHECK_IT_MESSAGE_COUNT"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err)
		count = n
	}

	bucket, err := blob.OpenBucket(ctx, blobURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bucket.Close() })

	topic, err := pubsub.OpenTopic(ctx, inURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = topic.Shutdown(ctx) })

	sub, err := pubsub.OpenSubscription(ctx, outURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Shutdown(ctx) })

	opts := extpubsub.Options{MinSize: 1} // force offload of every message
	extTopic := extpubsub.WrapTopic(topic, bucket, opts)
	wrapSub := extpubsub.WrapSubscription(sub, bucket, opts)

	// Publish.
	for i := 0; i < count; i++ {
		body, err := json.Marshal(map[string]any{
			"foo":       "bar",
			"timestamp": time.Now().Unix(),
			"random":    rand.Int64(),
		})
		require.NoError(t, err)
		require.NoError(t, extTopic.Send(ctx, &pubsub.Message{Body: body}))
	}

	// Consume and verify.
	recvCtx, cancel := context.WithTimeout(ctx, receiveTimeout)
	defer cancel()

	received := 0
	for received < count {
		batch, err := wrapSub.ReceiveBatch(recvCtx)
		require.NoError(t, err)

		msgs, err := batch.Unroll(recvCtx)
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
		}
		batch.Ack()
	}

	require.Equal(t, count, received)
}

// startRedpanda boots a Redpanda (Kafka-compatible) container, sets KAFKA_BROKERS,
// and returns the input topic URL and output subscription URL.
func startRedpanda(t *testing.T) (inURL, outURL string) {
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

	// rabbitpubsub/kafkapubsub never declare topology; Redpanda does not
	// reliably auto-create the topic on first produce, so create it up front.
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
// and returns the input exchange URL and output queue URL.
func startRabbitMQ(t *testing.T) (inURL, outURL string) {
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
