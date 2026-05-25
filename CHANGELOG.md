# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- Adopt the shared Keep a Changelog git-cliff configuration

## [0.6.0] - 2026-05-25

### Bug Fixes

- Avoid redundant Attributes call and prevent orphaned blobs in Offload
- Optimize LengthPrefixedSerializer and fix README example
- Fail closed when VerifyChecksum is set but no usable MD5 is available
- Surface corrupt and unsupported control messages instead of silent inline
- Make MemSubscription.Push safe after Shutdown
- Bound zero-config buffer and decouple periodic flush timeout from interval

### Documentation

- Fix checksum claim, retitle AGENTS.md, note producer concurrency limit

### Security

- Generate SLSA build provenance for releases

## [0.5.0] - 2026-05-25

### Bug Fixes

- Add timeout to background flush to prevent deadlock
- Only verify checksum on EOF to prevent error masking
- Ensure topic shuts down even if flush fails
- Address various edge cases and coverage gaps

### Miscellaneous Tasks

- Update changelog for v0.5.0

## [0.4.0] - 2026-05-24

### Documentation

- Document conditional offload (MinSize) and correct its comment

### Features

- Send sub-MinSize messages inline instead of offloading

### Miscellaneous Tasks

- Update changelog
- Release v0.4.0

### Testing

- Strengthen MinSize=0 back-compat test with round-trip assertion

## [0.3.0] - 2026-05-24

### Bug Fixes

- Delete orphaned blob when publish fails after offload

### Documentation

- Correct unimplemented MinSize and encryption claims
- Document blob retention (ack-deletes + lifecycle policies)

### Features

- Stamp created_at into blob metadata when InjectBlobMetadata is set
- WrapSubscription takes SubscriptionOptions (adds AckDeletes flag)
- Add AckDeletes mode and Batch.AckAndDelete (ack then delete)

### Miscellaneous Tasks

- Update changelog
- Release v0.3.0

### Testing

- Add end-to-end produce-consume-ackdeletes round trip

## [0.2.0] - 2026-05-24

### Documentation

- Add tested slog Observer example
- Document Observer and OpenTelemetry adapter pattern

### Features

- Add Observer hooks for offload and read
- Fire ReadDone for inline ccpubsub batches

### Miscellaneous Tasks

- Update changelog
- Release v0.2.0

## [0.1.0] - 2026-05-24

### Documentation

- Clarify WrapTopic always-offload default and embedding caveat
- Document integration tests and CLAIMCHECK_IT_* env vars
- Add WrapTopic quick-start example and fix MinSize wording
- Add core claimcheck package (DIY offload/read) section
- Rewrite around core + ccpubsub; remove legacy extpubsub
- Replace informal "magic" wording with "Pub/Sub integration layer"
- Restructure Quick Start as progressive, tested examples

### Features

- Make WrapTopic offload large messages per-message
- *(core)* Add Message and ControlMessage envelope
- *(core)* Add Noop and Gzip transformers
- *(core)* Add Serializer interface and JSONLines streaming decoder
- *(core)* Add Options and SetDefaults
- *(core)* Add length-prefixed serializer with uint32 and size guard
- *(core)* Add Offload
- *(core)* Add Open, Read, batch-size limiter, and checksum verification
- *(core)* Add Delete
- *(ccpubsub)* Add Subscription interface, Batch, and in-memory impl
- *(ccpubsub)* Add WrapSubscription adapter over gocloud *pubsub.Subscription
- *(ccpubsub)* Add Topic interface and buffering send wrapper
- *(core)* Add zstd transformer

### Miscellaneous Tasks

- Update changelog
- Update changelog
- Update changelog
- Release v0.1.0

### Refactor

- Extract shared offload helper from topic.SendBatch

### Testing

- Cover functional WrapTopic offload, passthrough, and error path
- Assert failed WrapTopic offload publishes no control message
- Add claim-check integration test over real broker + blob
- Split integration test into Kafka, RabbitMQ, and external cases
- *(core)* Add DIY round-trip example and satisfy lint
- *(ccpubsub)* Add end-to-end receive round-trip and satisfy lint
- *(ccpubsub)* Cover background FlushInterval timer
- *(ccpubsub)* Add end-to-end send/receive round-trip and satisfy lint

### Build

- Add integration-test dependencies
- Add test:integration task

## [0.0.1] - 2026-05-22

### Bug Fixes

- Update golangci-lint config for v2 and fix lint issues
- Use full semver for golangci-lint version in CI
- Resolve testify dependency, mockery config, and improve driver reliability
- *(ci)* Use goinstall mode for golangci-lint
- *(ci)* Restore golangci-lint v2 configuration and align version

### Documentation

- Update README with installation, usage, and contribution guidelines

### Features

- Implement core claim-check pattern with dual-layer API
- *(task)* Add test:race and test:coverage tasks
- *(task)* Add clean task to remove coverage artifacts
- Implement message size threshold and update documentation

### Miscellaneous Tasks

- *(ci)* Fix action version
- *(ci)* Fix action version again
- *(buildfiles)* Create a minimalistic makefile just in case we call it
- *(deps)* Bump google.golang.org/grpc from 1.77.0 to 1.79.3
- Remove mockery
- Update badges
- Add codecov
- Update taskfile
- Update taskfile
- Update release workflow
- Bump version

### Refactor

- Rename integration tests to scenario tests

### Security

- Add explicit permissions to CI workflow

### Styling

- Fix gci formatting in tests
- Fix gci formatting in serializer_test.go

### Testing

- Increase coverage for serializers

### Ci

- *(linter)* Fix golangci configuration

[unreleased]: https://github.com/peczenyj/go-claimcheck/compare/v0.6.0..HEAD
[0.6.0]: https://github.com/peczenyj/go-claimcheck/compare/v0.5.0..v0.6.0
[0.5.0]: https://github.com/peczenyj/go-claimcheck/compare/v0.4.0..v0.5.0
[0.4.0]: https://github.com/peczenyj/go-claimcheck/compare/v0.3.0..v0.4.0
[0.3.0]: https://github.com/peczenyj/go-claimcheck/compare/v0.2.0..v0.3.0
[0.2.0]: https://github.com/peczenyj/go-claimcheck/compare/v0.1.0..v0.2.0
[0.1.0]: https://github.com/peczenyj/go-claimcheck/compare/v0.0.1..v0.1.0
[0.0.1]: https://github.com/peczenyj/go-claimcheck/tree/v0.0.1

<!-- generated by git-cliff -->
