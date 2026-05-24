package claimcheck

import (
	"context"
	"time"
)

// Observer receives fire-after notifications about offload and read operations.
// All methods are called synchronously after the operation completes (success or
// failure). Implementations must not block. Each info carries StartTime and
// Duration so a tracing adapter can create a correctly-timed span retroactively.
type Observer interface {
	// OffloadDone is called once after Offload completes.
	OffloadDone(ctx context.Context, info OffloadInfo)
	// ReadDone is called once after a batch read completes (offloaded or inline).
	ReadDone(ctx context.Context, info ReadInfo)
}

// NopObserver is the default Observer; all methods are no-ops. Embed it in your
// own Observer so new interface methods added later do not break your type.
type NopObserver struct{}

func (NopObserver) OffloadDone(context.Context, OffloadInfo) {}
func (NopObserver) ReadDone(context.Context, ReadInfo)       {}

// OffloadInfo describes a completed Offload.
type OffloadInfo struct {
	Key       string        // blob key written
	MsgCount  int           // number of messages in the batch
	Bytes     int64         // stored blob size; 0 if it failed before the write completed
	Encoding  string        // ContentEncoding ("", "gzip", "zstd", ...)
	StartTime time.Time     // when the offload started
	Duration  time.Duration // total offload time
	Err       error         // non-nil if the offload failed
}

// ReadInfo describes a completed batch read.
type ReadInfo struct {
	Key       string        // blob key; empty for an inline batch
	MsgCount  int           // number of messages decoded
	Bytes     int64         // stored blob size (offloaded) or inline body length
	Inline    bool          // true when the batch had no control message
	StartTime time.Time     // when the read started
	Duration  time.Duration // total read time
	Err       error         // decode or checksum failure; nil on clean EOF
}
