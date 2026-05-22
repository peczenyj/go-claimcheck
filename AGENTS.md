# Gemini CLI Context: go-claimcheck

This document provides architectural overview, development workflows, and coding conventions for the `go-claimcheck` project.

## Project Overview

`go-claimcheck` is a Go library that implements the **Claim Check** pattern in a cloud-agnostic way. It leverages [Go CDK](https://gocloud.dev/) to transparently offload large message payloads to blob storage (like S3, GCS, or Azure Blob) while sending lightweight pointers (claims) through Pub/Sub systems (like Kafka, SNS/SQS, or RabbitMQ).

### Core Components (package `extpubsub`)

- **Topic Wrapper:** Intercepts `Send` calls. If a message batch exceeds a configurable `MinSize` threshold (in `Options`), it serializes the payload, uploads it to a `blob.Bucket`, and sends a control message with the blob URL and metadata. If below the threshold, messages are sent directly.
- **Subscription Wrapper:** Intercepts `Receive` calls. It detects control messages, automatically downloads the corresponding blob from the `blob.Bucket`, and "unrolls" it back into the original messages.
- **Explicit Batch Wrapper:** A second layer of the API (via `WrapSubscription`) that allows users to receive the raw control message as a `Batch`, providing metadata (URL, checksum, count) and manual `Unroll` capabilities.
- **Serializers:** Define how message batches are encoded into blobs (e.g., JSON Lines).
- **Transformers:** Middleware for blob data, such as Gzip compression.

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
- Integration tests are found in `extpubsub/integration_test.go`.
- Use `testify` for assertions.
- When adding new features, ensure appropriate unit and/or integration tests are included.
- For components requiring external dependencies (like cloud providers), use the Go CDK `memdriver` or `memblob` for in-memory testing when possible.

### Contribution Guidelines

- Keep the public API clean and consistent with Go CDK patterns.
- Ensure all new public symbols are documented with Go doc comments.
- Update `CHANGELOG.md` if making significant changes (use `task changelog`).
