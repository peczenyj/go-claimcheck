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
- [gotestsum](https://github.com/gotestyourself/gotestsum) (for formatted test output)

### Common Tasks

- **Run Tests:** `task test`
- **Run Linter:** `task lint`
- **Format Code:** `task format`
- **Tidy Modules:** `task tidy`

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
