package ccpubsub

import (
	"context"
	"sync"
	"testing"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

type recObserver struct {
	mu    sync.Mutex
	reads []claimcheck.ReadInfo
}

func (r *recObserver) OffloadDone(context.Context, claimcheck.OffloadInfo) {}
func (r *recObserver) ReadDone(_ context.Context, info claimcheck.ReadInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads = append(r.reads, info)
}

func TestInlineBatchReadFiresReadDone(t *testing.T) {
	ctx := context.Background()
	rec := &recObserver{}
	opts := claimcheck.Options{Observer: rec}
	opts.SetDefaults()

	// Inline batch: empty control message, body present.
	b := newBatch(claimcheck.ControlMessage{}, []byte("hello"), nil, nil, opts, nil, nil, false)

	msgs, err := b.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Body) != "hello" {
		t.Fatalf("bad msgs: %+v", msgs)
	}
	if len(rec.reads) != 1 {
		t.Fatalf("want 1 ReadDone, got %d", len(rec.reads))
	}
	got := rec.reads[0]
	if !got.Inline || got.MsgCount != 1 || got.Bytes != int64(len("hello")) || got.Err != nil {
		t.Fatalf("bad inline ReadInfo: %+v", got)
	}
}
