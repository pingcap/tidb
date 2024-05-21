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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tiancaiamao/gp"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func adaptiveAnlayzeDistSQLConcurrency(ctx context.Context, sctx sessionctx.Context) int {
	concurrency := sctx.GetSessionVars().AnalyzeDistSQLScanConcurrency()
	if concurrency > 0 {
		return concurrency
	}
	tikvStore, ok := sctx.GetStore().(helper.Storage)
	if !ok {
		logutil.BgLogger().Warn("Information about TiKV store status can be gotten only when the storage is TiKV")
		return variable.DefAnalyzeDistSQLScanConcurrency
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}
	pdCli, err := tikvHelper.TryGetPDHTTPClient()
	if err != nil {
		logutil.BgLogger().Warn("fail to TryGetPDHTTPClient", zap.Error(err))
		return variable.DefAnalyzeDistSQLScanConcurrency
	}
	storesStat, err := pdCli.GetStores(ctx)
	if err != nil {
		logutil.BgLogger().Warn("fail to get stores info", zap.Error(err))
		return variable.DefAnalyzeDistSQLScanConcurrency
	}
	if storesStat.Count <= 5 {
		return variable.DefAnalyzeDistSQLScanConcurrency
	} else if storesStat.Count <= 10 {
		return storesStat.Count
	} else if storesStat.Count <= 20 {
		return storesStat.Count * 2
	} else if storesStat.Count <= 50 {
		return storesStat.Count * 3
	}
	return storesStat.Count * 4
}

func getIntFromSessionVars(ctx sessionctx.Context, name string) (int, error) {
	sessionVars := ctx.GetSessionVars()
	concurrency, err := sessionVars.GetSessionOrGlobalSystemVar(context.Background(), name)
	if err != nil {
		return 0, err
	}
	c, err := strconv.ParseInt(concurrency, 10, 64)
	return int(c), err
}

func getBuildStatsConcurrency(ctx sessionctx.Context) (int, error) {
	return getIntFromSessionVars(ctx, variable.TiDBBuildStatsConcurrency)
}

func getBuildSamplingStatsConcurrency(ctx sessionctx.Context) (int, error) {
	return getIntFromSessionVars(ctx, variable.TiDBBuildSamplingStatsConcurrency)
}

var errAnalyzeWorkerPanic = errors.New("analyze worker panic")
var errAnalyzeOOM = errors.Errorf("analyze panic due to memory quota exceeds, please try with smaller samplerate(refer to %d/count)", config.DefRowsForSampleRate)

func isAnalyzeWorkerPanic(err error) bool {
	return err == errAnalyzeWorkerPanic || err == errAnalyzeOOM
}

func getAnalyzePanicErr(r any) error {
	if msg, ok := r.(string); ok {
		if msg == globalPanicAnalyzeMemoryExceed {
			return errors.Trace(errAnalyzeOOM)
		}
	}
	if err, ok := r.(error); ok {
		if err.Error() == globalPanicAnalyzeMemoryExceed {
			return errAnalyzeOOM
		}
		return err
	}
	return errors.Trace(errAnalyzeWorkerPanic)
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
	*util.WaitGroupPool
	notify chan error
	cnt    atomic.Uint64
}

// newNotifyErrorWaitGroupWrapper is to create notifyErrorWaitGroupWrapper
func newNotifyErrorWaitGroupWrapper(gp *gp.Pool, notify chan error) *notifyErrorWaitGroupWrapper {
	return &notifyErrorWaitGroupWrapper{
		WaitGroupPool: util.NewWaitGroupPool(gp),
		notify:        notify,
		cnt:           *atomic.NewUint64(0),
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
