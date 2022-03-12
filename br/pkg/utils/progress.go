// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type logFunc func(msg string, fields ...zap.Field)

// ProgressPrinter prints a progress bar.
type ProgressPrinter struct {
	name        string
	total       int64
	redirectLog bool
	progress    int64

	closeMu sync.Mutex
	closeCh chan struct{}
	closed  chan struct{}
}

// NewProgressPrinter returns a new progress printer.
func NewProgressPrinter(
	name string,
	total int64,
	redirectLog bool,
) *ProgressPrinter {
	return &ProgressPrinter{
		name:        name,
		total:       total,
		redirectLog: redirectLog,
	}
}

// Inc increases the current progress bar.
func (pp *ProgressPrinter) Inc() {
	atomic.AddInt64(&pp.progress, 1)
}

// Close closes the current progress bar.
func (pp *ProgressPrinter) Close() {
	pp.closeMu.Lock()
	defer pp.closeMu.Unlock()

	if pp.closeCh != nil {
		select {
		case pp.closeCh <- struct{}{}:
		default:
		}
		<-pp.closed
	} else {
		log.Warn("closing no-started progress printer")
	}
}

// goPrintProgress starts a gorouinte and prints progress.
func (pp *ProgressPrinter) goPrintProgress(
	ctx context.Context,
	logFuncImpl logFunc,
	testWriter io.Writer, // Only for tests
) {
	bar := pb.New64(pp.total)
	if pp.redirectLog || testWriter != nil {
		tmpl := `{"P":"{{percent .}}","C":"{{counters . }}","E":"{{etime .}}","R":"{{rtime .}}","S":"{{speed .}}"}`
		bar.SetTemplateString(tmpl)
		bar.SetRefreshRate(2 * time.Minute)
		bar.Set(pb.Static, false)       // Do not update automatically
		bar.Set(pb.ReturnSymbol, false) // Do not append '\r'
		bar.Set(pb.Terminal, false)     // Do not use terminal width
		// Hack! set Color to avoid separate progress string
		bar.Set(pb.Color, true)
		if logFuncImpl == nil {
			logFuncImpl = log.Info
		}
		bar.SetWriter(&wrappedWriter{name: pp.name, log: logFuncImpl})
	} else {
		tmpl := `{{string . "barName" | green}} {{ bar . "<" "-" (cycle . "-" "\\" "|" "/" ) "." ">"}} {{percent .}}`
		bar.SetTemplateString(tmpl)
		bar.Set("barName", pp.name)
	}
	if testWriter != nil {
		bar.SetWriter(testWriter)
		bar.SetRefreshRate(2 * time.Second)
	}
	bar.Start()

	closeCh := make(chan struct{}, 1)
	closed := make(chan struct{})
	pp.closeMu.Lock()
	pp.closeCh = closeCh
	pp.closed = closed
	pp.closeMu.Unlock()
	go func() {
		defer close(closed)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		defer bar.Finish()

		for {
			select {
			case <-ctx.Done():
				// a hacky way to adapt the old behavior:
				// when canceled by the context, leave the progress unchanged.
				return
			case <-closeCh:
				// a hacky way to adapt the old behavior:
				// when canceled by Close method (the 'internal' way), push the progress to 100%.
				bar.SetCurrent(pp.total)
				return
			case <-t.C:
			}

			currentProgress := atomic.LoadInt64(&pp.progress)
			if currentProgress <= pp.total {
				bar.SetCurrent(currentProgress)
			} else {
				bar.SetCurrent(pp.total)
			}
		}
	}()
}

type wrappedWriter struct {
	name string
	log  logFunc
}

func (ww *wrappedWriter) Write(p []byte) (int, error) {
	var info struct {
		P string
		C string
		E string
		R string
		S string
	}
	if err := json.Unmarshal(p, &info); err != nil {
		return 0, errors.Trace(err)
	}
	ww.log("progress",
		zap.String("step", ww.name),
		zap.String("progress", info.P),
		zap.String("count", info.C),
		zap.String("speed", info.S),
		zap.String("elapsed", info.E),
		zap.String("remaining", info.R))
	return len(p), nil
}

// StartProgress starts progress bar.
func StartProgress(
	ctx context.Context,
	name string,
	total int64,
	redirectLog bool,
	log logFunc,
) *ProgressPrinter {
	progress := NewProgressPrinter(name, total, redirectLog)
	progress.goPrintProgress(ctx, log, nil)
	return progress
}
