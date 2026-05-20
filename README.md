# go-claimcheck

[![CI](https://github.com/peczenyj/go-claimcheck/actions/workflows/ci.yml/badge.svg)](https://github.com/peczenyj/go-claimcheck/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/peczenyj/go-claimcheck.svg)](https://pkg.go.dev/github.com/peczenyj/go-claimcheck)

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

## Development

This project uses [Task](https://taskfile.dev/) to manage the development workflow.

### Prerequisites

- Go 1.25+
- [golangci-lint](https://golangci-lint.run/)
- [mockery](https://github.com/vektra/mockery) (for generating mocks)
- [gotestsum](https://github.com/gotestyourself/gotestsum) (for formatted test output)

### Common Tasks

- **Run Tests:** `task test`
- **Run Linter:** `task lint`
- **Format Code:** `task format`
- **Tidy Modules:** `task tidy`
- **Generate Mocks:** `task mock`

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
