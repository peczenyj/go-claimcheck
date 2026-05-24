package claimcheck_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"gocloud.dev/blob/memblob"

	claimcheck "github.com/peczenyj/go-claimcheck"
)

// slogObserver logs offload/read events. It embeds NopObserver so that any
// future Observer methods default to no-ops without breaking this type.
type slogObserver struct {
	claimcheck.NopObserver
	log *slog.Logger
}

func (o slogObserver) OffloadDone(_ context.Context, info claimcheck.OffloadInfo) {
	o.log.Info("offload", "msgs", info.MsgCount, "encoding", info.Encoding, "err", info.Err)
}

func (o slogObserver) ReadDone(_ context.Context, info claimcheck.ReadInfo) {
	o.log.Info("read", "msgs", info.MsgCount, "inline", info.Inline, "err", info.Err)
}

func Example_observer() {
	// Strip the volatile time attribute so the output is deterministic.
	dropTime := func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return a
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{ReplaceAttr: dropTime}))
	obs := slogObserver{log: logger}

	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	opts := claimcheck.Options{
		Observer: obs,
		KeyFunc:  func([]*claimcheck.Message) string { return "example-key" },
	}

	cm, err := claimcheck.Offload(ctx, bucket, opts, []*claimcheck.Message{
		{Body: []byte("alpha")},
		{Body: []byte("beta")},
	})
	if err != nil {
		fmt.Println("offload error:", err)
		return
	}

	if _, err := claimcheck.Read(ctx, bucket, cm, opts); err != nil {
		fmt.Println("read error:", err)
		return
	}

	// Output:
	// level=INFO msg=offload msgs=2 encoding="" err=<nil>
	// level=INFO msg=read msgs=2 inline=false err=<nil>
}
