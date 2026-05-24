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
    MinSize:     1024 * 1024,            // Offload only if >= 1MB
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

### 4. Explicit Send-Side Offloading (WrapTopic)

For fine-grained, per-message control on the publish side without the driver-level batching:

```go
import (
    "github.com/peczenyj/go-claimcheck/extpubsub"
    "gocloud.dev/pubsub"
    "gocloud.dev/pubsub/mempubsub"
    "gocloud.dev/blob/memblob"
)

// Initialize base drivers
baseTopic := mempubsub.NewTopic()
bucket := memblob.OpenBucket(nil)

// Wrap with per-message offload logic
opts := extpubsub.Options{
    MinSize: 1024 * 1024, // Offload only if >= 1MB; 0 = always offload
}
topic := extpubsub.WrapTopic(baseTopic, bucket, opts)

// Send — body >= MinSize is written to the bucket; smaller bodies pass through unchanged
err := topic.Send(ctx, &pubsub.Message{Body: []byte("large payload...")})
```

The receiving side uses `WrapSubscription` (or `extpubsub.NewSubscription`) with the same bucket and options to transparently unroll offloaded messages.

### 5. Core API — manual offload/read (`claimcheck` package)

The root `claimcheck` package is the low-level foundation the wrappers build on.
Use it directly when you manage the topic, subscription, and blob bucket
yourself: offload a batch to a blob, send the returned control message's
metadata over any transport, then read it back on the other side.

```go
import (
    claimcheck "github.com/peczenyj/go-claimcheck"
    "gocloud.dev/blob/memblob"
)

bucket := memblob.OpenBucket(nil)
opts := claimcheck.Options{KeyPrefix: "claimcheck/"}

// Producer: write the batch to a blob, get the control message.
cm, err := claimcheck.Offload(ctx, bucket, opts,
    []*claimcheck.Message{{Body: []byte("large payload...")}})
metadata := cm.ToMetadata(opts.MetadataPrefix) // attach to your pubsub message

// Consumer: parse the metadata you received, then read the blob back.
if parsed, ok := claimcheck.ParseControlMessage(metadata, opts.MetadataPrefix); ok {
    msgs, err := claimcheck.Read(ctx, bucket, parsed, opts)
    // ... process msgs ...
    _ = claimcheck.Delete(ctx, bucket, parsed) // optional cleanup
}
```

`Options` supports `KeyPrefix`/`KeyFunc` (blob naming), `Serializer` (JSON Lines
or length-prefixed), `Transformer` (gzip), `VerifyChecksum` (opt-in MD5), and
`MaxMessageSize`/`MaxBatchSize` (decode safety caps). For bounded-memory reads,
use `claimcheck.Open` to stream messages in chunks instead of `claimcheck.Read`.

> **Note:** `claimcheck` is the low-level core of an in-progress API redesign.
> The `extpubsub` wrappers above are being migrated onto it, so the high-level
> API may change before v1.0.

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
claim-check round-trip against real infrastructure.

```bash
task test:integration
```

There are three tests:

- **`TestIntegrationKafka`** — publishes and consumes through a Redpanda (Kafka)
  container, with MinIO as the blob store. Requires Docker.
- **`TestIntegrationRabbitMQ`** — publishes and consumes through a RabbitMQ
  container, with MinIO as the blob store. Requires Docker.
- **`TestIntegrationExternal`** — runs against real backends you provide via
  environment variables. **Skipped unless `CLAIMCHECK_IT_PUBSUB_URL` is set.**

The external test reads:

| Variable | Purpose | Default |
| :--- | :--- | :--- |
| `CLAIMCHECK_IT_PUBSUB_URL` | pubsub URL used for **both** publish and consume (`rabbit://my-queue`, `kafka://my-topic`, `mem://t`, …) | _(required; test skipped if empty)_ |
| `CLAIMCHECK_IT_BLOB_URL` | blob bucket URL (`s3://bucket?region=...&endpoint=...&use_path_style=true`, `file:///tmp/cc`, …) | `mem://` |
| `CLAIMCHECK_IT_MESSAGE_COUNT` | number of messages to push | `1024` |

`CLAIMCHECK_IT_PUBSUB_URL` is passed to both `pubsub.OpenTopic` and
`pubsub.OpenSubscription`, so it must be valid as both for the chosen driver
(e.g. a RabbitMQ exchange/queue with a binding). The Go CDK URL openers read
their own variables, which you set for the real backends:

- Kafka: `KAFKA_BROKERS` (comma-separated)
- RabbitMQ: `RABBIT_SERVER_URL` (`amqp://...`)
- S3: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, optionally `AWS_ENDPOINT_URL_S3`

Example against a real RabbitMQ + real S3 bucket:

```bash
export CLAIMCHECK_IT_PUBSUB_URL='rabbit://claimcheck'
export RABBIT_SERVER_URL='amqp://guest:guest@localhost:5672/'
export CLAIMCHECK_IT_BLOB_URL='s3://my-bucket?region=eu-west-1'
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
task test:integration
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
