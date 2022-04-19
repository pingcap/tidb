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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/tikv/client-go/v2/oracle"
)

type rcStmtContext struct {
	sctx     sessionctx.Context
	ctx      context.Context
	prevStmt *rcStmtContext
	// useTxnStartTS means whether to use the transaction's start ts as the read ts
	useTxnStartTS bool
	// When RCCheckTS optimization is enabled, `usePrevStmtTS` can be true and try reuse the previous statement's ts first.
	// This may cause an error from the storage layer. At that, the statement will retry and get the ts from oracle again.
	usePrevStmtTS bool
	ts            uint64
	tsFuture      oracle.Future
	// advisedRetryTS is the advised ts when retry
	advisedRetryTS uint64
	isError        bool
}

func newRcStmtContext(ctx context.Context, sctx sessionctx.Context, prevStmt *rcStmtContext) (*rcStmtContext, error) {
	useTxnStartTS := false
	if prevStmt == nil {
		txn, err := sctx.Txn(false)
		if err != nil {
			return nil, err
		}
		useTxnStartTS = !txn.Valid()
	}

	return &rcStmtContext{
		ctx:           ctx,
		sctx:          sctx,
		prevStmt:      prevStmt,
		useTxnStartTS: useTxnStartTS,
		usePrevStmtTS: prevStmt != nil && !prevStmt.isError && prevStmt.ts > 0 && sctx.GetSessionVars().StmtCtx.RCCheckTS,
	}, nil
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

	if ts, err = s.tsFuture.Wait(); err != nil {
		return 0, err
	}

	s.ts = ts
	s.sctx.GetSessionVars().TxnCtx.SetForUpdateTS(ts)
	txn.SetOption(kv.SnapshotTS, ts)
	return ts, nil
}

func (s *rcStmtContext) retry(ctx context.Context) error {
	s.ctx = ctx
	if s.ts > 0 && s.advisedRetryTS > s.ts {
		s.tsFuture = sessiontxn.ConstantTSFuture(s.advisedRetryTS)
	} else {
		s.tsFuture = s.getTxnOracleFuture()
	}

	s.ts = 0
	s.useTxnStartTS = false
	s.advisedRetryTS = 0

	// trigger tso fetch immediately
	_, err := s.getTS()
	return err
}

func (s *rcStmtContext) prepareTS() {
	if s.tsFuture != nil {
		return
	}

	s.sctx.PrepareTSFuture(s.ctx)
	if s.useTxnStartTS {
		s.tsFuture = s.getTxnStartTSFuture()
	} else if s.usePrevStmtTS {
		s.tsFuture = sessiontxn.ConstantTSFuture(s.prevStmt.ts)
	} else {
		s.tsFuture = s.getTxnOracleFuture()
	}
}

func (s *rcStmtContext) getTxnStartTSFuture() sessiontxn.FuncTSFuture {
	return func() (uint64, error) {
		txn, err := s.sctx.Txn(false)
		if err != nil {
			return 0, err
		}

		if !txn.Valid() {
			return 0, errors.New("txn is invalid")
		}

		return txn.StartTS(), nil
	}
}

func (s *rcStmtContext) getTxnOracleFuture() oracle.Future {
	return sessiontxn.GetTxnOracleFuture(s.ctx, s.sctx, s.sctx.GetSessionVars().CheckAndGetTxnScope())

}

type txnContextProvider struct {
	sctx                  sessionctx.Context
	is                    infoschema.InfoSchema
	stmt                  *rcStmtContext
	causalConsistencyOnly bool

	// tidbSnapshotVarTS is the set by @@tidb_snapshot
	tidbSnapshotVarTS uint64
	// tidbSnapshotVarInfoSchema is the timestamp according to tidbSnapshotVarTS
	tidbSnapshotVarInfoSchema infoschema.InfoSchema
}

// NewRCTxnContextProvider creates a new txnContextProvider
func NewRCTxnContextProvider(sctx sessionctx.Context, causalConsistencyOnly bool) sessiontxn.TxnContextProvider {
	return &txnContextProvider{
		sctx:                  sctx,
		causalConsistencyOnly: causalConsistencyOnly,
		is: temptable.AttachLocalTemporaryTableInfoSchema(
			sctx,
			sctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema),
		),
	}
}

func (p *txnContextProvider) GetTxnInfoSchema() infoschema.InfoSchema {
	if p.tidbSnapshotVarTS != 0 {
		return p.tidbSnapshotVarInfoSchema
	}
	return p.is
}

func (p *txnContextProvider) ActiveTxn(ctx context.Context) (kv.Transaction, error) {
	if txn, err := p.sctx.Txn(false); err == nil && txn.Valid() {
		return txn, nil
	}

	if p.stmt == nil {
		if err := p.sctx.NewTxn(ctx); err != nil {
			return nil, err
		}
	} else {
		p.sctx.PrepareTSFuture(ctx)
	}

	txn, err := p.sctx.Txn(true)
	if err != nil {
		return nil, err
	}

	if p.causalConsistencyOnly {
		txn.SetOption(kv.GuaranteeLinearizability, false)
	}
	return txn, nil
}

// IsTxnActive returns whether the txn is active
func (p *txnContextProvider) IsTxnActive() bool {
	txn, _ := p.sctx.Txn(false)
	return txn.Valid()
}

func (p *txnContextProvider) GetReadTS() (uint64, error) {
	if p.tidbSnapshotVarTS != 0 {
		return p.tidbSnapshotVarTS, nil
	}
	return p.stmt.getTS()
}

func (p *txnContextProvider) GetForUpdateTS() (uint64, error) {
	return p.stmt.getTS()
}

func (p *txnContextProvider) OnStmtStart(ctx context.Context) error {
	if snapshotTS := p.sctx.GetSessionVars().SnapshotTS; snapshotTS != 0 {
		p.tidbSnapshotVarTS = snapshotTS
		p.tidbSnapshotVarInfoSchema = p.sctx.GetSessionVars().SnapshotInfoschema.(infoschema.InfoSchema)
	}
	stmt, err := newRcStmtContext(ctx, p.sctx, p.stmt)
	if err != nil {
		return err
	}
	p.stmt = stmt
	return nil
}

func (p *txnContextProvider) OnStmtError(_ error) {
	p.stmt.isError = true
	return
}

func (p *txnContextProvider) OnStmtRetry(ctx context.Context, _ error) error {
	return p.stmt.retry(ctx)
}

func (p *txnContextProvider) warmUp() error {
	if p.tidbSnapshotVarTS != 0 {
		p.stmt.prepareTS()
	}
	return nil
}

func (p *txnContextProvider) adviseRetryTS(ts uint64) error {
	if p.stmt == nil {
		p.stmt.advisedRetryTS = ts
	}
	return nil
}

func (p *txnContextProvider) Advise(opt sessiontxn.AdviceOption, val ...interface{}) error {
	switch opt {
	case sessiontxn.AdviceWarmUpNow:
		return p.warmUp()
	case sessiontxn.AdviceNoConflictForUpdateTS:
		return sessiontxn.WithValUint64(val, p.adviseRetryTS)
	}
	return nil
}
