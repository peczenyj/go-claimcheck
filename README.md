# go-claimcheck
Cloud-agnostic Claim Check pattern for Go. Transparently offload large messages to blob storage (S3/GCS) while sending lightweight pointers via Pub/Sub   (Kafka/RabbitMQ). Powered by Go CDK for total provider portability. Features pluggable serialization, compression/encryption, and rich metadata (checksums,   file size, message counts).
