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

package readcommitted

import (
	"context"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/tikv/client-go/v2/oracle"
)

type rcStmtContext struct {
	sctx          sessionctx.Context
	ctx           context.Context
	prevStmt      *rcStmtContext
	useTxnStartTS bool
	ts            uint64
	tsFuture      oracle.Future
}

func (s *rcStmtContext) getTS() (ts uint64, err error) {
	if s.ts != 0 {
		return s.ts, nil
	}

	s.prepareTS()
	txn, err := s.sctx.Txn(true)
	if err != nil {
		return 0, err
	}

	if s.useTxnStartTS {
		ts = txn.StartTS()
	} else {
		ts, err = s.tsFuture.Wait()
	}

	if err != nil {
		return 0, err
	}

	s.ts = ts
	s.sctx.GetSessionVars().TxnCtx.SetForUpdateTS(ts)
	txn.SetOption(kv.SnapshotTS, ts)
	return ts, nil
}

func (s *rcStmtContext) retry() error {
	forUpdateTS := s.sctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	if s.ts > 0 && forUpdateTS > s.ts {
		s.tsFuture = sessiontxn.ConstantTSFuture(forUpdateTS)
	} else {
		s.tsFuture = s.getOracleFuture()
	}

	s.ts = 0
	s.useTxnStartTS = false

	// trigger tso fetch immediately
	_, err := s.getTS()
	return err
}

func (s *rcStmtContext) prepareTS() {
	s.sctx.PrepareTSFuture(s.ctx)
	if s.useTxnStartTS || s.tsFuture != nil {
		return
	}

	sessVars := s.sctx.GetSessionVars()
	if !sessVars.StmtCtx.RCCheckTS && s.prevStmt != nil && s.prevStmt.ts > 0 {
		s.tsFuture = sessiontxn.ConstantTSFuture(s.prevStmt.ts)
	} else {
		s.tsFuture = s.getOracleFuture()
	}
}

func (s *rcStmtContext) getOracleFuture() oracle.Future {
	return sessiontxn.GetTxnOracleFuture(s.ctx, s.sctx, s.sctx.GetSessionVars().CheckAndGetTxnScope())
}

type txnContextProvider struct {
	sctx sessionctx.Context
	is   infoschema.InfoSchema
	stmt *rcStmtContext
}

// NewRCTxnContextProvider creates a new txnContextProvider
func NewRCTxnContextProvider(sctx sessionctx.Context) sessiontxn.TxnContextProvider {
	return &txnContextProvider{
		sctx: sctx,
		is: temptable.AttachLocalTemporaryTableInfoSchema(
			sctx,
			sctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema),
		),
	}
}

func (p *txnContextProvider) GetTxnInfoSchema() infoschema.InfoSchema {
	if snapshotIS := p.sctx.GetSessionVars().SnapshotInfoschema; snapshotIS != nil {
		return snapshotIS.(infoschema.InfoSchema)
	}
	return p.is
}

func (p *txnContextProvider) GetReadTS() (uint64, error) {
	if snapshotTS := p.sctx.GetSessionVars().SnapshotTS; snapshotTS != 0 {
		return snapshotTS, nil
	}
	return p.stmt.getTS()
}

func (p *txnContextProvider) GetForUpdateTS() (uint64, error) {
	return p.stmt.getTS()
}

func (p *txnContextProvider) OnStmtStart(ctx context.Context) error {
	p.stmt = &rcStmtContext{
		ctx:           ctx,
		sctx:          p.sctx,
		prevStmt:      p.stmt,
		useTxnStartTS: p.stmt == nil,
	}
	return nil
}

func (p *txnContextProvider) OnStmtRetry() error {
	return p.stmt.retry()
}

func (p *txnContextProvider) Advise(opt sessiontxn.AdviceOption, _ ...interface{}) error {
	switch opt {
	case sessiontxn.AdviceWarmUpNow:
		p.stmt.prepareTS()
	}
	return nil
}
