# go-claimcheck

[![tag](https://img.shields.io/github/tag/peczenyj/go-claimcheck.svg)](https://github.com/peczenyj/go-claimcheck/releases)
![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.25-%23007d9c)
[![GoDoc](https://pkg.go.dev/badge/github.com/peczenyj/go-claimcheck)](http://pkg.go.dev/github.com/peczenyj/go-claimcheck)
[![ci](https://github.com/peczenyj/go-claimcheck/actions/workflows/ci.yml/badge.svg)](https://github.com/peczenyj/go-claimcheck/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/peczenyj/go-claimcheck/graph/badge.svg?token=9y6f3vGgpr)](https://codecov.io/gh/peczenyj/go-claimcheck)
[![Report card](https://goreportcard.com/badge/github.com/peczenyj/go-claimcheck)](https://goreportcard.com/report/github.com/peczenyj/go-claimcheck)
[![CodeQL](https://github.com/peczenyj/go-claimcheck/actions/workflows/github-code-scanning/codeql/badge.svg)](https://github.com/peczenyj/go-claimcheck/actions/workflows/github-code-scanning/codeql)
[![Dependency Review](https://github.com/peczenyj/go-claimcheck/actions/workflows/dependency-review.yml/badge.svg)](https://github.com/peczenyj/go-claimcheck/actions/workflows/dependency-review.yml)
[![License](https://img.shields.io/github/license/peczenyj/go-claimcheck)](./LICENSE)

Cloud-agnostic Claim Check pattern for Go. Transparently offload large messages to blob storage (S3/GCS/Azure) while sending lightweight pointers via Pub/Sub (Kafka/RabbitMQ/SNS/SQS). 

Powered by [Go CDK](https://gocloud.dev/) for total provider portability.

## Features

- **Transparent Unrolling:** Automatically downloads and decodes offloaded blobs during `Receive`.
- **Explicit Batch Handling:** Optional wrapper to handle raw blob data and metadata manually.
- **Pluggable Serialization:** Built-in support for NDJSON (JSON Lines) and Length-prefixed binary.
- **Data Transformation:** Built-in Gzip compression middleware.
- **Rich Metadata:** Automatically tracks checksums (MD5), file size, and message counts.
- **Provider Agnostic:** Works with any Pub/Sub and Blob storage supported by Go CDK.

## Installation

```bash
go get github.com/peczenyj/go-claimcheck
```

## Quick Start

### 1. Setup Extended Topic

```go
import (
    "github.com/peczenyj/go-claimcheck/extpubsub"
    "gocloud.dev/pubsub/mempubsub"
    "gocloud.dev/blob/memblob"
)

// Initialize base drivers
baseTopic := mempubsub.NewTopic()
bucket := memblob.OpenBucket(nil)

// Wrap with Claim-Check logic
opts := extpubsub.Options{
    MinSize:     1024 * 1024,            // Offload only if > 1MB
    Transformer: extpubsub.NewGzipTransformer(), // Compress blobs
}
topic := extpubsub.NewTopic(baseTopic, bucket, opts)

// Send messages normally
err := topic.Send(ctx, &pubsub.Message{Body: []byte("large payload...")})
```

### 2. Setup Extended Subscription

```go
// Wrap base subscription
baseSub := mempubsub.NewSubscription(baseTopic, 1*time.Minute)
sub := extpubsub.NewSubscription(baseSub, bucket, opts)

// Receive unrolls automatically
m, err := sub.Receive(ctx)
fmt.Printf("Received: %s\n", m.Body)
m.Ack()
```

### 3. Explicit Batch Handling (Advanced)

For manual control or when processing messages in bulk from a single blob:

```go
import "github.com/peczenyj/go-claimcheck/extpubsub"

// Wrap the *pubsub.Subscription for advanced features
extSub := extpubsub.WrapSubscription(sub, bucket, opts)

// Receive the raw batch control message
batch, err := extSub.ReceiveBatch(ctx)
if err != nil { /* ... */ }

fmt.Printf("Batch URL: %s, Messages: %d\n", batch.URL, batch.MessageCount)

// Download and unroll all messages at once
msgs, err := batch.Unroll(ctx)
for _, m := range msgs {
    fmt.Printf("Unrolled Body: %s\n", m.Body)
}

batch.Ack() // Acks the underlying control message
```

## Development

This project uses [Task](https://taskfile.dev/) to manage the development workflow.

### Prerequisites

- Go 1.25+
- [golangci-lint](https://golangci-lint.run/)
- [gotestsum](https://github.com/gotestyourself/gotestsum) (for formatted test output)

### Common Tasks

- **Run Tests:** `task test`
- **Run Linter:** `task lint`
- **Format Code:** `task format`
- **Tidy Modules:** `task tidy`

## Integration Testing

Integration tests live behind the `integration` build tag and exercise the full
claim-check round-trip against real infrastructure. They require Docker.

```bash
task test:integration
```

By default this starts disposable containers (Redpanda for Kafka, MinIO for S3).
Each backend can be independently swapped for a real one via environment
variables — when a variable is set, its container is not started.

| Variable | Purpose | Example |
| :--- | :--- | :--- |
| `CLAIMCHECK_IT_INPUT` | topic URL to publish to | `kafka://my-topic` / `rabbit://my-exchange` |
| `CLAIMCHECK_IT_OUTPUT` | subscription URL to consume from | `kafka://my-group?topic=my-topic&offset=oldest` / `rabbit://my-queue` |
| `CLAIMCHECK_IT_BLOB_URL` | blob bucket URL | `s3://bucket?region=us-east-1&endpoint=...&use_path_style=true` / `mem://` / `file:///tmp/cc` |
| `CLAIMCHECK_IT_BROKER` | broker started in fallback: `kafka` (default) or `rabbitmq` | `rabbitmq` |
| `CLAIMCHECK_IT_MESSAGE_COUNT` | number of messages to push | `1000` (default) |

`CLAIMCHECK_IT_INPUT` and `CLAIMCHECK_IT_OUTPUT` describe the two ends of the
**same** broker (one round-trip per run). The Go CDK URL openers also read their
own variables, which you must set for real backends (and which the containers
set automatically in fallback mode):

- Kafka: `KAFKA_BROKERS` (comma-separated)
- RabbitMQ: `RABBIT_SERVER_URL` (`amqp://...`)
- S3: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, optionally `AWS_ENDPOINT_URL_S3`

Example against a real Kafka + real S3 bucket:

```bash
export CLAIMCHECK_IT_INPUT='kafka://orders'
export CLAIMCHECK_IT_OUTPUT='kafka://claimcheck?topic=orders&offset=oldest'
export KAFKA_BROKERS='broker1:9092,broker2:9092'
export CLAIMCHECK_IT_BLOB_URL='s3://my-bucket?region=eu-west-1'
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
task test:integration
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
