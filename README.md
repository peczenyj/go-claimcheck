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

- **Two layers, your choice:** a low-level **core** (`claimcheck`) for full control over offload/read, and **magic wrappers** (`ccpubsub`) that buffer, offload, and unroll for you.
- **Blob-level delivery:** the consumer `Batch` Acks/Nacks a whole offloaded blob — the honest unit of delivery for this pattern.
- **Pluggable Serialization:** built-in NDJSON (JSON Lines) and length-prefixed binary, both streaming for bounded-memory reads.
- **Data Transformation:** built-in Gzip and Zstd compression middleware.
- **Rich Metadata:** tracks checksum (MD5), file size, content type/encoding, and message count on every control message.
- **Provider Agnostic:** works with any Pub/Sub and Blob storage supported by [Go CDK](https://gocloud.dev/).

## Installation

```bash
go get github.com/peczenyj/go-claimcheck
```

## Architecture

The library is two layers. Pick the one that fits how much control you want;
both speak the same on-blob format, so a producer on one layer interoperates
with a consumer on the other.

| Layer | Package | Send | Receive |
| :--- | :--- | :--- | :--- |
| **Core (DIY)** | `claimcheck` | `Offload` a batch to a blob, attach `ControlMessage` metadata to your own pubsub message | `ParseControlMessage`, then `Read` / `Open` the blob |
| **Magic (wrappers)** | `ccpubsub` | `WrapTopic` buffers and offloads for you | `WrapSubscription` returns a `Batch` you `Read` and `Ack` |

The core never imports `gocloud.dev/pubsub`: it deals only in blobs and a
metadata map, so the control message can ride any transport. The `ccpubsub`
wrappers adapt any gocloud `*pubsub.Topic` / `*pubsub.Subscription`.

## Quick Start

### Magic wrappers (`ccpubsub`)

The wrappers do the offloading and unrolling for you. The producer buffers
messages and offloads a batch to one blob — flushed when a threshold is hit, on
`Flush`, or on `Shutdown` — then publishes a single control message. The consumer
receives that control message as a `Batch`, reads the blob back, and Acks the
whole blob.

```go
import (
    "context"
    "time"

    claimcheck "github.com/peczenyj/go-claimcheck"
    "github.com/peczenyj/go-claimcheck/ccpubsub"
    "gocloud.dev/blob/memblob"
    "gocloud.dev/pubsub/mempubsub"
)

ctx := context.Background()
bucket := memblob.OpenBucket(nil)
baseTopic := mempubsub.NewTopic()
baseSub := mempubsub.NewSubscription(baseTopic, time.Second)

// Both sides MUST agree on the bucket, Serializer, Transformer, and
// MetadataPrefix. (See "The bucket-binding contract" below.)
opts := claimcheck.Options{Transformer: claimcheck.NewZstdTransformer()}

// Producer: buffer up to 100 messages per blob (or flush on Shutdown).
topic := ccpubsub.WrapTopic(baseTopic, bucket, ccpubsub.TopicOptions{
    Options:     opts,
    MaxMessages: 100,
})
_ = topic.Send(ctx, &claimcheck.Message{Body: []byte("large payload...")})
_ = topic.Shutdown(ctx) // flushes the buffered batch

// Consumer: receive the batch, read every message, ack the whole blob.
sub := ccpubsub.WrapSubscription(baseSub, bucket, opts)
batch, err := sub.Receive(ctx)
if err != nil { /* ... */ }

msgs, err := batch.Read(ctx) // or batch.Open(ctx) to stream with bounded memory
for _, m := range msgs {
    // ... process m.Body ...
}
batch.Ack()              // acknowledges the whole blob
_ = batch.Delete(ctx)    // optional: remove the blob once consumed
```

`WrapTopic` **always** offloads the buffered batch to a blob; the number of
messages per blob is caller-controlled via `MaxMessages` / `MaxBytes` /
`FlushInterval` and is otherwise unbounded. A `Batch` may also be *inline*
(`batch.Offloaded() == false`) when a received message carries no control-message
metadata — `Read` still returns it, and `Ack`/`Delete` behave sensibly.

### Core API — manual offload/read (`claimcheck`)

Use the core directly when you manage the topic, subscription, and bucket
yourself: offload a batch to a blob, send the returned control message's metadata
over any transport, then read it back on the other side.

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
or length-prefixed), `Transformer` (Gzip or Zstd), `VerifyChecksum` (opt-in MD5),
and `MaxMessageSize`/`MaxBatchSize` (decode safety caps). For bounded-memory
reads, use `claimcheck.Open` to stream messages in chunks instead of
`claimcheck.Read`.

## The bucket-binding contract

The claim check only works when the producer and consumer **independently agree**
on where the blob lives and how it is encoded. There is no negotiation: the
control message carries the blob key and content-type/encoding, but the consumer
must already be configured to reach the same storage and decode the same way.

Both sides must share:

- **the same blob bucket** — the consumer must be able to open the exact bucket
  the producer wrote to (same provider, region, and bucket name/prefix);
- **a compatible `Serializer`** — the consumer must decode what the producer
  encoded (JSON Lines vs. length-prefixed);
- **a compatible `Transformer`** — matching compression (`""`, `gzip`, `zstd`);
- **the same `MetadataPrefix`** — or the consumer will not recognise the control
  message and will treat the delivery as an inline (non-offloaded) batch.

If these drift, the failure mode is silent or late: a mismatched `MetadataPrefix`
makes the consumer ignore the pointer and hand back the raw control message as an
inline `Batch`; a wrong bucket makes `Read` fail to open the blob; a mismatched
serializer or transformer surfaces as a decode error (or, with
`VerifyChecksum`, an `ErrChecksumMismatch`). Treat the bucket + options as a
shared contract you deploy to both sides together.

## Delivery semantics

This library is **at-least-once**, and the unit of delivery is the **whole blob**:

- `WrapSubscription` Acks/Nacks the underlying pubsub message, which points at one
  offloaded blob. `batch.Ack()` acknowledges every message in that blob at once;
  `batch.Nack()` requests redelivery of the entire blob.
- If your consumer crashes after reading a blob but before `Ack`, the broker
  redelivers the same control message and you process the **whole blob again**.
  Make consumers **idempotent** (e.g. dedupe on a message key) — there is no
  partial-blob acknowledgement.
- Offloaded blobs are **not** deleted automatically. Call `batch.Delete(ctx)`
  (or `claimcheck.Delete`) once you have durably processed the batch, or run a
  lifecycle/TTL policy on the bucket to reclaim storage.

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
