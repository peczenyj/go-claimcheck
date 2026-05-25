# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability within this project, please send an e-mail to **tiago.peczenyj+github@gmail.com**.

All security vulnerabilities will be promptly addressed. We request that you do not report security-related issues through public GitHub issues.

## Build Provenance

Tagged releases meet [SLSA](https://slsa.dev) Build Level 1. Each release ships a
source archive and `SHA256SUMS` with a signed build-provenance attestation,
verifiable with `gh attestation verify <artifact> --repo peczenyj/go-claimcheck`.
