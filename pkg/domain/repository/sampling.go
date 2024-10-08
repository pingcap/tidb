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

package repository

import (
	"context"
	"time"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func (w *Worker) samplingTable(ctx context.Context, rt *repositoryTable) {
	_sessctx := w.getSessionWithRetry()
	defer w.sesspool.Put(_sessctx)
	sess := _sessctx.(sessionctx.Context)

	if rt.insertStmt == "" {
		if err := w.buildInsertQuery(ctx, sess, rt); err != nil {
			logutil.BgLogger().Info("repository sampling failed: could not generate insert statement", zap.String("tbl", rt.destTable), zap.NamedError("err", err))
			return
		}
	}

	if _, err := w.runQuery(ctx, sess, rt.insertStmt, w.instanceID); err != nil {
		logutil.BgLogger().Info("repository sampling failed: could not run insert statement", zap.String("tbl", rt.destTable), zap.NamedError("err", err))
	}
}

func (w *Worker) startSample(ctx context.Context) func() {
	return func() {
		ticker := time.NewTicker(5 * time.Second)

		for {
			select {
			case <-w.exit:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
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
