// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package glue

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/fatih/color"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/vbauerster/mpb/v7"
	"github.com/vbauerster/mpb/v7/decor"
	"golang.org/x/term"
)

type pbProgress struct {
	bar      *mpb.Bar
	progress *mpb.Progress
	ops      ConsoleOperations
}

// Inc increases the progress. This method must be goroutine-safe, and can
// be called from any goroutine.
func (p pbProgress) Inc() {
	p.bar.Increment()
}

// IncBy increases the progress by n.
func (p pbProgress) IncBy(n int64) {
	p.bar.IncrBy(int(n))
}

func (p pbProgress) GetCurrent() int64 {
	return p.bar.Current()
}

// Close marks the progress as 100% complete and that Inc() can no longer be
// called.
func (p pbProgress) Close() {
	if p.bar.Completed() || p.bar.Aborted() {
		return
	}
	p.bar.Abort(false)
}

// Wait implements the ProgressWaiter interface.
func (p pbProgress) Wait(ctx context.Context) error {
	ch := make(chan struct{})
	go func() {
		p.progress.Wait()
		close(ch)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ch:
		return nil
	}
}

// ProgressWaiter is the extended `Progress“: which provides a `wait` method to
// allow caller wait until all unit in the progress finished.
type ProgressWaiter interface {
	Progress
	Wait(context.Context) error
}

type noOPWaiter struct {
	Progress
}

func (nw noOPWaiter) Wait(context.Context) error {
	return nil
}

// cbOnComplete like `decor.OnComplete`, however allow the message provided by a function.
func cbOnComplete(decl decor.Decorator, cb func() string) decor.DecorFunc {
	return func(s decor.Statistics) string {
		if s.Completed {
			return cb()
		}
		return decl.Decor(s)
	}
}

func (ops ConsoleOperations) OutputIsTTY() bool {
	f, ok := ops.Out().(*os.File)
	if !ok {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}

// StartProgressBar starts a progress bar with the console operations.
// Note: This function has overlapped function with `glue.StartProgress`, however this supports display extra fields
//
//	after success, and implement by `mpb` (instead of `pb`).
//
// Note': Maybe replace the old `StartProgress` with `mpb` too.
func (ops ConsoleOperations) StartProgressBar(title string, total int, extraFields ...ExtraField) ProgressWaiter {
	if !ops.OutputIsTTY() {
		return ops.startProgressBarOverDummy(title, total, extraFields...)
	}
	return ops.startProgressBarOverTTY(title, total, extraFields...)
}

func (ops ConsoleOperations) startProgressBarOverDummy(title string, total int, extraFields ...ExtraField) ProgressWaiter {
	return noOPWaiter{utils.StartProgress(context.TODO(), title, int64(total), true, nil)}
}

func (ops ConsoleOperations) startProgressBarOverTTY(title string, total int, extraFields ...ExtraField) ProgressWaiter {
	pb := mpb.New(mpb.WithOutput(ops.Out()), mpb.WithRefreshRate(400*time.Millisecond))
	greenTitle := color.GreenString(title)
	bar := pb.New(int64(total),
		// Play as if the old BR style.
		mpb.BarStyle().Lbound("<").Filler("-").Padding(".").Rbound(">").Tip("-", "\\", "|", "/", "-").TipOnComplete("-"),
		mpb.BarFillerMiddleware(func(bf mpb.BarFiller) mpb.BarFiller {
			return mpb.BarFillerFunc(func(w io.Writer, reqWidth int, stat decor.Statistics) {
				if stat.Aborted || stat.Completed {
					return
				}
				bf.Fill(w, reqWidth, stat)
			})
		}),
		mpb.PrependDecorators(decor.OnAbort(decor.OnComplete(decor.Name(greenTitle), fmt.Sprintf("%s  ::", title)), fmt.Sprintf("%s  ::", title))),
		mpb.AppendDecorators(decor.OnAbort(decor.Any(cbOnComplete(decor.NewPercentage("%02.2f"), printFinalMessage(extraFields))), color.RedString("ABORTED"))),
	)

	// If total is zero, finish right now.
	if total == 0 {
		bar.SetTotal(0, true)
	}

	return pbProgress{
		bar:      bar,
		ops:      ops,
		progress: pb,
	}
}
