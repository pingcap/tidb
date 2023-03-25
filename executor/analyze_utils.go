// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/atomic"
)

func getBuildStatsConcurrency(ctx sessionctx.Context) (int, error) {
	sessionVars := ctx.GetSessionVars()
	concurrency, err := sessionVars.GetSessionOrGlobalSystemVar(context.Background(), variable.TiDBBuildStatsConcurrency)
	if err != nil {
		return 0, err
	}
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), err
}

var errAnalyzeWorkerPanic = errors.New("analyze worker panic")
var errAnalyzeOOM = errors.Errorf("analyze panic due to memory quota exceeds, please try with smaller samplerate(refer to %d/count)", config.DefRowsForSampleRate)

func isAnalyzeWorkerPanic(err error) bool {
	return err == errAnalyzeWorkerPanic || err == errAnalyzeOOM
}

func getAnalyzePanicErr(r interface{}) error {
	if msg, ok := r.(string); ok {
		if msg == globalPanicAnalyzeMemoryExceed {
			return errAnalyzeOOM
		}
		if strings.Contains(msg, memory.PanicMemoryExceed) {
			return errors.Errorf(msg, errAnalyzeOOM)
		}
	}
	if err, ok := r.(error); ok {
		if err.Error() == globalPanicAnalyzeMemoryExceed {
			return errAnalyzeOOM
		}
		return err
	}
	return errAnalyzeWorkerPanic
}

// analyzeResultsNotifyWaitGroupWrapper is a wrapper for sync.WaitGroup
// Please add all goroutine count when to `Add` to avoid exiting in advance.
type analyzeResultsNotifyWaitGroupWrapper struct {
	sync.WaitGroup
	notify chan *statistics.AnalyzeResults
	cnt    atomic.Uint64
}

// NewAnalyzeResultsNotifyWaitGroupWrapper is to create analyzeResultsNotifyWaitGroupWrapper
func NewAnalyzeResultsNotifyWaitGroupWrapper(notify chan *statistics.AnalyzeResults) *analyzeResultsNotifyWaitGroupWrapper {
	return &analyzeResultsNotifyWaitGroupWrapper{
		notify: notify,
		cnt:    *atomic.NewUint64(0),
	}
}

// Run runs a function in a goroutine and calls done when function returns.
// Please DO NOT use panic in the cb function.
func (w *analyzeResultsNotifyWaitGroupWrapper) Run(exec func()) {
	old := w.cnt.Inc() - 1
	go func(cnt uint64) {
		defer func() {
			w.Done()
			if cnt == 0 {
				w.Wait()
				close(w.notify)
			}
		}()
		exec()
	}(old)
}

// notifyErrorWaitGroupWrapper is a wrapper for sync.WaitGroup
// Please add all goroutine count when to `Add` to avoid exiting in advance.
type notifyErrorWaitGroupWrapper struct {
	sync.WaitGroup
	notify chan error
	cnt    atomic.Uint64
}

// newNotifyErrorWaitGroupWrapper is to create notifyErrorWaitGroupWrapper
func newNotifyErrorWaitGroupWrapper(notify chan error) *notifyErrorWaitGroupWrapper {
	return &notifyErrorWaitGroupWrapper{
		notify: notify,
		cnt:    *atomic.NewUint64(0),
	}
}

// Run runs a function in a goroutine and calls done when function returns.
// Please DO NOT use panic in the cb function.
func (w *notifyErrorWaitGroupWrapper) Run(exec func()) {
	old := w.cnt.Inc() - 1
	go func(cnt uint64) {
		defer func() {
			w.Done()
			if cnt == 0 {
				w.Wait()
				close(w.notify)
			}
		}()
		exec()
	}(old)
}
