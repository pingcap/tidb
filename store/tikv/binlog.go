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
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"context"
	"sync"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tipb/go-binlog"
	zap "go.uber.org/zap"
)

// BinlogExecutor defines the logic to replicate binlogs during transaction commit.
type BinlogExecutor interface {
	Prewrite(ctx context.Context, primary []byte) <-chan BinlogWriteResult
	Commit(ctx context.Context, commitTS int64)
	Skip()
}

// BinlogWriteResult defines the result of prewrite binlog.
type BinlogWriteResult interface {
	Skipped() bool
	GetError() error
}

type binlogExecutor struct {
	txn *KVTxn
}

func (e *binlogExecutor) Skip() {
	binloginfo.RemoveOneSkippedCommitter()
}

func (e *binlogExecutor) Prewrite(ctx context.Context, primary []byte) <-chan BinlogWriteResult {
	ch := make(chan BinlogWriteResult, 1)
	go func() {
		logutil.Eventf(ctx, "start prewrite binlog")
		binInfo := e.txn.us.GetOption(kv.BinlogInfo).(*binloginfo.BinlogInfo)
		bin := binInfo.Data
		bin.StartTs = int64(e.txn.startTS)
		if bin.Tp == binlog.BinlogType_Prewrite {
			bin.PrewriteKey = primary
		}
		wr := binInfo.WriteBinlog(e.txn.store.clusterID)
		if wr.Skipped() {
			binInfo.Data.PrewriteValue = nil
			binloginfo.AddOneSkippedCommitter()
		}
		logutil.Eventf(ctx, "finish prewrite binlog")
		ch <- wr
	}()
	return ch
}

func (e *binlogExecutor) Commit(ctx context.Context, commitTS int64) {
	binInfo := e.txn.us.GetOption(kv.BinlogInfo).(*binloginfo.BinlogInfo)
	binInfo.Data.Tp = binlog.BinlogType_Commit
	if commitTS == 0 {
		binInfo.Data.Tp = binlog.BinlogType_Rollback
	}
	binInfo.Data.CommitTs = commitTS
	binInfo.Data.PrewriteValue = nil

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
		binlogWriteResult := binInfo.WriteBinlog(e.txn.store.clusterID)
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
