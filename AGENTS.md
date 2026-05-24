# Gemini CLI Context: go-claimcheck

This document provides architectural overview, development workflows, and coding conventions for the `go-claimcheck` project.

## Project Overview

`go-claimcheck` is a Go library that implements the **Claim Check** pattern in a cloud-agnostic way. It leverages [Go CDK](https://gocloud.dev/) to transparently offload large message payloads to blob storage (like S3, GCS, or Azure Blob) while sending lightweight pointers (claims) through Pub/Sub systems (like Kafka, SNS/SQS, or RabbitMQ).

The library is two layers: a transport-agnostic **core** (`claimcheck`, repo
root) and **magic wrappers** (`ccpubsub`) that wire it onto Go CDK pubsub.

### Core Package (`claimcheck`, repo root)

The foundation. It deals only in blobs and a metadata map and never imports
`gocloud.dev/pubsub`, so the control message can ride any transport.

- **`Offload`:** serializes a batch of `*Message`, optionally transforms (compresses) it, writes it to a `blob.Bucket`, and returns a `ControlMessage` (blob key, message count, content type/encoding, file size, MD5 checksum).
- **`Read` / `Open`:** read a blob back from its `ControlMessage`. `Read` returns all messages; `Open` returns a streaming `Decoder` for bounded-memory reads. Honors `MaxMessageSize`/`MaxBatchSize` caps and optional MD5 `VerifyChecksum`.
- **`Delete`:** removes an offloaded blob.
- **`ControlMessage` + `ToMetadata`/`ParseControlMessage`:** the pointer envelope, marshalled to/from a `map[string]string` under a configurable `MetadataPrefix`.
- **Serializers:** how message batches are encoded into blobs — JSON Lines or length-prefixed binary; both stream.
- **Transformers:** middleware for blob bytes — Noop, Gzip, or Zstd compression.

### Wrappers Package (`ccpubsub`)

The "magic" layer. Both wrappers adapt existing gocloud objects rather than
implementing gocloud's driver interfaces.

- **`WrapTopic`:** buffers `Send`s and offloads the buffered batch to one blob, publishing a single control message. Flushes on a `MaxMessages`/`MaxBytes`/`FlushInterval` threshold, on explicit `Flush`, or on `Shutdown`.
- **`WrapSubscription`:** adapts any `*pubsub.Subscription` into a claim-check `Subscription` whose `Receive` returns a `Batch`. Ack/Nack apply to the whole offloaded blob (the unit of delivery), not per message.
- **`Batch`:** `Read`/`Open` the blob (or an inline message), `Ack`/`Nack` the whole unit, `Delete` the blob.
- **`MemSubscription`:** an in-memory `Subscription` for tests.

## Building and Running

The project uses [Task](https://taskfile.dev/) for workflow automation. A `Makefile` is also provided as a proxy to `task`.

| Command | Description |
| :--- | :--- |
| `task test` | Runs all tests using `gotestsum`. |
| `task lint` | Executes `golangci-lint` with the project's configuration. |
| `task format` | Formats the codebase using `golangci-lint --fix` (includes `gofumpt`, `goimports`, `gci`). |
| `task tidy` | Runs `go mod tidy` to clean up dependencies. |
| `task changelog` | Generates `CHANGELOG.md` using `git-cliff`. |

### Prerequisites

- Go 1.25+
- `golangci-lint`
- `gotestsum` (optional, for formatted output)
- `task` (Taskfile runner)

## Development Conventions

### Coding Style

- Follow standard Go idioms and effective Go practices.
- **Formatting:** Use `task format` to ensure consistency. The project uses `gofumpt`, `goimports`, and `gci` for imports ordering (Standard -> Default -> Local).
- **Linting:** Strict linting is enforced via `.golangci.yml`. Always run `task lint` before submitting changes. Key linters include `gocritic`, `revive`, `gocyclo`, and `bodyclose`.

### Testing Practices

- Tests are located alongside the source code in `*_test.go` files.
- End-to-end flows with in-memory drivers live in `ccpubsub/sendrecv_test.go` and the runnable examples in `*example_test.go`. Integration tests against real brokers (behind the `integration` build tag) are in `ccpubsub/integration_test.go`.
- Use `testify` for assertions.
- When adding new features, ensure appropriate unit and/or scenario tests are included.
- For components requiring external dependencies (like cloud providers), use the Go CDK `memdriver` or `memblob` for in-memory testing when possible.

### Contribution Guidelines

- Keep the public API clean and consistent with Go CDK patterns.
- Ensure all new public symbols are documented with Go doc comments.
- Update `CHANGELOG.md` if making significant changes (use `task changelog`).
