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
[![SLSA Level 2](https://img.shields.io/badge/SLSA-Level_2-green)](https://github.com/peczenyj/go-claimcheck/attestations)

Cloud-agnostic Claim Check pattern for Go. Transparently offload large messages to blob storage (S3/GCS/Azure) while sending lightweight pointers via Pub/Sub (Kafka/RabbitMQ/SNS/SQS). 

Powered by [Go CDK](https://gocloud.dev/) for total provider portability.

## Features

- **Two API layers:** a low-level **core** (`claimcheck`) for full control over offload and read, and a **Pub/Sub integration layer** (`ccpubsub`) that handles buffering, offloading, and unrolling automatically.
- **Blob-level delivery:** the consumer `Batch` acknowledges or negatively-acknowledges a whole offloaded blob — the natural unit of delivery for this pattern.
- **Pluggable Serialization:** built-in NDJSON (JSON Lines) and length-prefixed binary, both streaming for bounded-memory reads.
- **Data Transformation:** built-in Gzip and Zstd compression middleware.
- **Rich Metadata:** tracks file size, content type/encoding, and message count on every control message, plus a best-effort MD5 checksum. The checksum is backend-dependent — some stores do not expose an object MD5 (e.g. S3 multipart uploads, GCS composite objects, Azure without Content-MD5); when it is absent, `VerifyChecksum` fails closed with `ErrChecksumUnavailable` rather than reading unverified.
- **Provider Agnostic:** works with any Pub/Sub and Blob storage supported by [Go CDK](https://gocloud.dev/).

## Installation

```bash
go get github.com/peczenyj/go-claimcheck
```

## Architecture

The library is organized in two layers. Choose the one that matches the level of
control you need; both share the same on-blob format, so a producer using one
layer interoperates with a consumer using the other.

| Layer | Package | Send | Receive |
| :--- | :--- | :--- | :--- |
| **Core (low-level)** | `claimcheck` | `Offload` a batch to a blob, attach `ControlMessage` metadata to your own pubsub message | `ParseControlMessage`, then `Read` / `Open` the blob |
| **Pub/Sub integration** | `ccpubsub` | `WrapTopic` buffers and offloads automatically | `WrapSubscription` returns a `Batch` to `Read` and `Ack` |

The core never imports `gocloud.dev/pubsub`: it deals only in blobs and a
metadata map, so the control message can ride any transport. The `ccpubsub`
wrappers adapt any gocloud `*pubsub.Topic` / `*pubsub.Subscription`.

## Quick Start

The examples below build up from the simplest possible use to full manual
control. Every snippet is mirrored by a runnable test in the package, so they
compile and pass as written.

### 1. Zero configuration

Wrap a topic and subscription and start sending. With empty options you get the
defaults — JSON Lines serialization, no compression, and the `claimcheck_`
metadata prefix. The producer buffers messages and offloads them to one blob,
publishing a single control message; the consumer receives that as a `Batch`,
reads the blob back, and acknowledges the whole blob.

```go
import (
    "context"
    "log"
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
msgs, err := batch.Read(ctx) // or batch.Open(ctx) to stream with bounded memory
if err != nil {
    log.Fatal(err)
}
batch.Ack() // acknowledges the whole blob
```

A `Batch` may also be *inline* (`batch.Offloaded() == false`) when a received
message carries no control-message metadata — `Read` still returns the message,
`Ack` acknowledges it, and `Delete` is a no-op (there is no blob to remove).

### 2. Customizing the integration layer

The same wrappers take options. Here we compress blobs with Zstd, namespace the
blob keys, verify checksums on read, and control batching: flush after 100
buffered messages or every two seconds, whichever comes first. `WrapTopic`
**always** offloads the buffered batch — the number of messages per blob is
caller-controlled via `MaxMessages` / `MaxBytes` / `FlushInterval`. If you set
**none** of them the batch is only published on an explicit `Flush`/`Shutdown`,
so the buffer is bounded by `DefaultMaxBytes` (1 MiB) to prevent unbounded
growth; set at least one trigger for predictable flushing. `FlushTimeout`
bounds how long a single periodic flush may take (0 = run to completion, and
independent of `FlushInterval`).

```go
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
```

`Options` also supports `KeyFunc` (custom blob naming), `Serializer` (JSON Lines
or length-prefixed), and `MaxMessageSize` / `MaxBatchSize` (decode safety caps).

### 3. Bring your own codec

Compression and serialization are pluggable interfaces. To add a codec, satisfy
the three-method `Transformer` interface. This example wraps the standard
library's DEFLATE codec; the same shape works for any codec — for instance
Brotli via [`github.com/andybalholm/brotli`](https://github.com/andybalholm/brotli)
(a third-party package, not a dependency of this library).

The same interface is also the right place for **encryption** — an AES-GCM or
NaCl secretbox `Transformer` plugs in exactly like a compression codec.
Encryption is not built in; it is bring-your-own via this interface.

```go
import (
    "compress/flate"
    "io"

    claimcheck "github.com/peczenyj/go-claimcheck"
)

type flateTransformer struct{}

func (flateTransformer) ContentEncoding() string { return "deflate" }

func (flateTransformer) WrapWriter(w io.Writer) (io.WriteCloser, error) {
    return flate.NewWriter(w, flate.DefaultCompression)
}

func (flateTransformer) WrapReader(r io.Reader) (io.ReadCloser, error) {
    return flate.NewReader(r), nil
}

// Use it like any built-in transformer:
opts := claimcheck.Options{Transformer: flateTransformer{}}
```

### 4. Low-level core API (`claimcheck`)

For full manual control, use the core directly: you manage the topic,
subscription, and bucket yourself. `Offload` writes a batch to a blob and returns
a `ControlMessage`; you send its metadata over any transport, then parse it and
`Read` the blob back on the other side. The core never imports
`gocloud.dev/pubsub`.

```go
import (
    "context"
    "log"

    claimcheck "github.com/peczenyj/go-claimcheck"
    "gocloud.dev/blob/memblob"
)

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
```

For bounded-memory reads, use `claimcheck.Open` to stream messages in chunks
instead of `claimcheck.Read`.

## Conditional offload

By default every message produced through `WrapTopic` is offloaded to a blob.
Set `Options.MinSize` to skip the blob for small payloads: a message whose
`Body` is smaller than `MinSize` is published **inline** (a plain Pub/Sub
message carrying the body and metadata), while messages at or above `MinSize`
are buffered and offloaded as usual.

```go
opts := claimcheck.Options{MinSize: 64 * 1024} // offload only payloads >= 64 KiB
topic := ccpubsub.WrapTopic(baseTopic, bucket, ccpubsub.TopicOptions{Options: opts})
```

The consumer needs no special handling — `WrapSubscription` already returns
inline messages as a `Batch` whose `Offloaded()` is `false` and whose `Read`
yields the original message. `MinSize` is `0` by default, preserving the
always-offload behavior.

Inline messages are published immediately and do not pass through the offload
buffer, so a small message may be delivered before previously buffered larger
ones; `WrapTopic` does not guarantee ordering across the inline/offload boundary.

## Observability

Set `Options.Observer` to record offload/read metrics. The `Observer` interface
lives in the core package and pulls in no observability dependency:

```go
type Observer interface {
    OffloadDone(ctx context.Context, info claimcheck.OffloadInfo)
    ReadDone(ctx context.Context, info claimcheck.ReadInfo)
}
```

Embed `claimcheck.NopObserver` so future methods stay non-breaking, and override
the hooks you care about. Each `Info` carries `MsgCount`, `Bytes`, `Duration`,
`Err`, and (for reads) an `Inline` flag distinguishing offloaded blobs from
inline messages — enough for offload/unroll latency histograms, byte counters,
and offloaded-vs-inline ratios. The observer set on `Options` flows through the
`ccpubsub` layer automatically. See `Example_observer` in the godoc.

### Writing an OpenTelemetry adapter

`claimcheck` keeps `go.opentelemetry.io/otel` out of its dependency graph, so an
OTel adapter lives in your own code (or a separate module). Because each `Info`
carries `StartTime` and `Duration`, you can record a correctly-timed span
retroactively:

```go
// Illustrative — not part of the module; you supply meter and tracer.
type otelObserver struct {
    claimcheck.NopObserver
    tracer       trace.Tracer
    offloadBytes metric.Int64Counter
    offloadDur   metric.Int64Histogram
}

func (o otelObserver) OffloadDone(ctx context.Context, info claimcheck.OffloadInfo) {
    o.offloadBytes.Add(ctx, info.Bytes)
    o.offloadDur.Record(ctx, info.Duration.Microseconds())

    _, span := o.tracer.Start(ctx, "claimcheck.offload",
        trace.WithTimestamp(info.StartTime))
    if info.Err != nil {
        span.RecordError(info.Err)
    }
    span.End(trace.WithTimestamp(info.StartTime.Add(info.Duration)))
}
```

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
shared contract you deploy to both sides together. *(Tip: If `InjectBlobMetadata` is used, the consumer can optionally use `bucket.Attributes()` to validate the content-type and encoding before reading, though this incurs an extra network round-trip.)*

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

## Blob retention and cleanup

Delivery is at-least-once and deleting the offloaded blob is the **consumer's**
responsibility. Two things prevent blobs from accumulating:

**1. Ack-deletes (recommended).** Create the subscription with `AckDeletes` so
acking a batch also removes its blob (ack first, then a best-effort delete):

```go
sub := ccpubsub.WrapSubscription(baseSub, bucket, ccpubsub.SubscriptionOptions{
    Options:    claimcheck.Options{MetadataPrefix: "cc_"},
    AckDeletes: true,
})
// batch.Ack() now also deletes the blob.
```

For explicit error handling, call `batch.AckAndDelete(ctx)` instead of `Ack()`.
The ack-then-delete ordering is deliberate: a crash after ack but before delete
leaves an orphan (swept by the lifecycle policy below); a crash before ack means
neither ran, so redelivery still finds the blob present.

**2. A bucket lifecycle policy (backstop).** Set an object-expiration rule on the
bucket, scoped to your `KeyPrefix`, with an age longer than (max broker
retention + worst-case processing time). It acts on the object's native creation
time — no library cooperation needed — and mops up anything ack-deletes misses
(consumers that never delete, or the post-ack crash window).

S3:

```json
{
  "Rules": [{
    "ID": "expire-claimcheck-blobs",
    "Filter": { "Prefix": "claimcheck/" },
    "Status": "Enabled",
    "Expiration": { "Days": 7 }
  }]
}
```

GCS:

```json
{
  "rule": [{
    "action": { "type": "Delete" },
    "condition": { "age": 7, "matchesPrefix": ["claimcheck/"] }
  }]
}
```

The producer also self-cleans: if publishing the control message fails after the
blob is written, the buffering `WrapTopic` deletes the orphaned blob before
returning the error.

## Known limitations

- **Producer I/O is serialized.** `WrapTopic` performs the blob write and the
  control-message publish while holding its internal lock, so concurrent `Send`
  calls block for the duration of a flush. This bounds producer throughput under
  heavy concurrency; see
  [#53](https://github.com/peczenyj/go-claimcheck/issues/53). If you need higher
  concurrency today, run multiple `WrapTopic` instances or shard producers.

## Supply chain security

Every tagged release meets [SLSA](https://slsa.dev) **Build Level 2**: the release
workflow runs on a GitHub-hosted runner and builds a source archive
(`go-claimcheck-<version>.tar.gz`) and a `SHA256SUMS` file, then generates a
Sigstore-signed build-provenance attestation over them using GitHub's
[artifact attestations](https://docs.github.com/actions/security-guides/using-artifact-attestations-to-establish-provenance-for-builds).
Because the provenance is produced and signed by the hosted build platform, it is
authentic and tamper-evident — meeting SLSA Build Level 2 by default.

Verify a downloaded release artifact against its provenance with the GitHub CLI:

```bash
gh attestation verify go-claimcheck-0.6.0.tar.gz --repo peczenyj/go-claimcheck
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
