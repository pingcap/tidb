// Copyright 2021 PingCAP, Inc.
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

package txn

import (
	"context"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type binlogExecutor struct {
	txn     *tikv.KVTxn
	binInfo *binloginfo.BinlogInfo
}

func (e *binlogExecutor) Skip() {
	binloginfo.RemoveOneSkippedCommitter()
}

func (e *binlogExecutor) Prewrite(ctx context.Context, primary []byte) <-chan tikv.BinlogWriteResult {
	ch := make(chan tikv.BinlogWriteResult, 1)
	go func() {
		logutil.Eventf(ctx, "start prewrite binlog")
		bin := e.binInfo.Data
		bin.StartTs = int64(e.txn.StartTS())
		if bin.Tp == binlog.BinlogType_Prewrite {
			bin.PrewriteKey = primary
		}
		wr := e.binInfo.WriteBinlog(e.txn.GetClusterID())
		if wr.Skipped() {
			e.binInfo.Data.PrewriteValue = nil
			binloginfo.AddOneSkippedCommitter()
		}
		logutil.Eventf(ctx, "finish prewrite binlog")
		ch <- wr
	}()
	return ch
}

func (e *binlogExecutor) Commit(ctx context.Context, commitTS int64) {
	e.binInfo.Data.Tp = binlog.BinlogType_Commit
	if commitTS == 0 {
		e.binInfo.Data.Tp = binlog.BinlogType_Rollback
	}
	e.binInfo.Data.CommitTs = commitTS
	e.binInfo.Data.PrewriteValue = nil

	wg := sync.WaitGroup{}
	mock := false
	failpoint.Inject("mockSyncBinlogCommit", func(val failpoint.Value) {
		if val.(bool) {
			wg.Add(1)
			mock = true
		}
	})
	go func() {
		logutil.Eventf(ctx, "start write finish binlog")
		binlogWriteResult := e.binInfo.WriteBinlog(e.txn.GetClusterID())
		err := binlogWriteResult.GetError()
		if err != nil {
			logutil.BgLogger().Error("failed to write binlog",
				zap.Error(err))
		}
		logutil.Eventf(ctx, "finish write finish binlog")
		if mock {
			wg.Done()
		}
	}()
	if mock {
		wg.Wait()
	}
}
