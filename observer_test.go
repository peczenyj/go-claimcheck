package claimcheck

import (
	"context"
	"sync"
	"testing"
	"time"

	"gocloud.dev/blob/memblob"
)

// compile-time assertion that NopObserver satisfies Observer.
var _ Observer = NopObserver{}

func TestNopObserverIsNoOp(t *testing.T) {
	var obs Observer = NopObserver{}
	// Must not panic and must accept fully-populated infos.
	obs.OffloadDone(context.Background(), OffloadInfo{
		Key: "k", MsgCount: 1, Bytes: 10, Encoding: "gzip",
		StartTime: time.Now(), Duration: time.Millisecond,
	})
	obs.ReadDone(context.Background(), ReadInfo{
		Key: "k", MsgCount: 1, Bytes: 10, Inline: false,
		StartTime: time.Now(), Duration: time.Millisecond,
	})
}

// recordingObserver captures the most recent info passed to each hook.
type recordingObserver struct {
	mu       sync.Mutex
	offloads []OffloadInfo
	reads    []ReadInfo
}

func (r *recordingObserver) OffloadDone(_ context.Context, info OffloadInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.offloads = append(r.offloads, info)
}

func (r *recordingObserver) ReadDone(_ context.Context, info ReadInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads = append(r.reads, info)
}

func TestOffloadFiresOffloadDoneOnSuccess(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	rec := &recordingObserver{}
	opts := Options{Observer: rec, KeyFunc: func([]*Message) string { return "fixed-key" }}

	cm, err := Offload(ctx, bucket, opts, []*Message{{Body: []byte("a")}, {Body: []byte("b")}})
	if err != nil {
		t.Fatalf("Offload: %v", err)
	}
	if len(rec.offloads) != 1 {
		t.Fatalf("want 1 OffloadDone, got %d", len(rec.offloads))
	}
	got := rec.offloads[0]
	if got.Key != "fixed-key" || got.MsgCount != 2 || got.Err != nil {
		t.Fatalf("bad info: %+v", got)
	}
	if got.Bytes != cm.FileSize || got.Bytes == 0 {
		t.Fatalf("Bytes=%d, cm.FileSize=%d", got.Bytes, cm.FileSize)
	}
	if got.Duration <= 0 {
		t.Fatalf("Duration not set: %v", got.Duration)
	}
}

func TestOffloadFiresOffloadDoneOnError(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	bucket.Close() // force NewWriter to fail
	rec := &recordingObserver{}
	opts := Options{Observer: rec}

	_, err := Offload(ctx, bucket, opts, []*Message{{Body: []byte("a")}})
	if err == nil {
		t.Fatal("expected Offload to fail on a closed bucket")
	}
	if len(rec.offloads) != 1 {
		t.Fatalf("want 1 OffloadDone, got %d", len(rec.offloads))
	}
	got := rec.offloads[0]
	if got.Err == nil {
		t.Fatal("OffloadInfo.Err should be set on failure")
	}
	if got.Bytes != 0 {
		t.Fatalf("Bytes should be 0 on failure, got %d", got.Bytes)
	}
}

func TestReadFiresReadDone(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	rec := &recordingObserver{}
	opts := Options{Observer: rec, KeyFunc: func([]*Message) string { return "k1" }}

	cm, err := Offload(ctx, bucket, opts, []*Message{{Body: []byte("x")}, {Body: []byte("y")}})
	if err != nil {
		t.Fatalf("Offload: %v", err)
	}

	msgs, err := Read(ctx, bucket, cm, opts)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("want 2 msgs, got %d", len(msgs))
	}
	if len(rec.reads) != 1 {
		t.Fatalf("want 1 ReadDone, got %d", len(rec.reads))
	}
	got := rec.reads[0]
	if got.Key != "k1" || got.MsgCount != 2 || got.Inline || got.Err != nil {
		t.Fatalf("bad ReadInfo: %+v", got)
	}
	if got.Bytes != cm.FileSize {
		t.Fatalf("Bytes=%d, cm.FileSize=%d", got.Bytes, cm.FileSize)
	}
}

func TestReadFiresReadDoneOnChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	rec := &recordingObserver{}
	opts := Options{Observer: rec, VerifyChecksum: true, KeyFunc: func([]*Message) string { return "k2" }}

	cm, err := Offload(ctx, bucket, opts, []*Message{{Body: []byte("x")}})
	if err != nil {
		t.Fatalf("Offload: %v", err)
	}
	cm.Checksum = "00000000000000000000000000000000" // valid hex MD5, wrong value

	_, err = Read(ctx, bucket, cm, opts)
	if err == nil {
		t.Fatal("expected checksum mismatch error")
	}
	if len(rec.reads) != 1 {
		t.Fatalf("want 1 ReadDone, got %d", len(rec.reads))
	}
	if rec.reads[0].Err == nil {
		t.Fatal("ReadInfo.Err should be set on checksum mismatch")
	}
}

func TestOpenFiresReadDoneOnMissingBlob(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()
	rec := &recordingObserver{}
	opts := Options{Observer: rec}

	_, _, err := Open(ctx, bucket, ControlMessage{Key: "does-not-exist"}, opts)
	if err == nil {
		t.Fatal("expected open error for missing blob")
	}
	if len(rec.reads) != 1 {
		t.Fatalf("want 1 ReadDone, got %d", len(rec.reads))
	}
	if rec.reads[0].Err == nil {
		t.Fatal("ReadInfo.Err should be set on open failure")
	}
}
