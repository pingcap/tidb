// Copyright 2024 PingCAP, Inc.
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

package workloadrepo

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func (w *worker) samplingTable(ctx context.Context, rt *repositoryTable) {
	_sessctx := w.getSessionWithRetry()
	defer w.sesspool.Put(_sessctx)
	sess := _sessctx.(sessionctx.Context)

	if rt.insertStmt == "" {
		if err := buildInsertQuery(ctx, sess, rt); err != nil {
			logutil.BgLogger().Info("workload repository sampling failed: could not generate insert statement", zap.String("tbl", rt.destTable), zap.NamedError("err", err))
			return
		}
	}

	if _, err := runQuery(ctx, sess, rt.insertStmt, w.instanceID); err != nil {
		logutil.BgLogger().Info("workload repository sampling failed: could not run insert statement", zap.String("tbl", rt.destTable), zap.NamedError("err", err))
	}
}

func (w *worker) startSample(ctx context.Context) func() {
	return func() {
		w.Lock()
		w.samplingTicker = time.NewTicker(time.Duration(w.samplingInterval) * time.Second)
		w.Unlock()

		for {
			select {
			case <-ctx.Done():
				return
			case <-w.samplingTicker.C:
				// sample thread
				var wg util.WaitGroupWrapper

				for rtIdx := range workloadTables {
					rt := &workloadTables[rtIdx]
					if rt.tableType != samplingTable {
						continue
					}
					wg.Run(func() {
						w.samplingTable(ctx, rt)
					})
				}

				wg.Wait()
			}
		}
	}
}

func (w *worker) resetSamplingInterval(newRate int32) {
	if w.samplingTicker == nil {
		return
	}

	if newRate == 0 {
		w.samplingTicker.Stop()
	} else {
		w.samplingTicker.Reset(time.Duration(newRate) * time.Second)
	}
}

func (w *worker) changeSamplingInterval(_ context.Context, d string) error {
	n, err := strconv.Atoi(d)
	if err != nil {
		return err
	}

	w.Lock()
	defer w.Unlock()

	if int32(n) != w.samplingInterval {
		w.samplingInterval = int32(n)
		w.resetSamplingInterval(w.samplingInterval)
	}

	return nil
}
