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

package legacy

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table/temptable"
)

// SimpleTxnContextProvider implements TxnContextProvider
// It is only used in refactor stage
// TODO: remove it after refactor finished
type SimpleTxnContextProvider struct {
	Ctx                context.Context
	Sctx               sessionctx.Context
	InfoSchema         infoschema.InfoSchema
	GetReadTSFunc      func() (uint64, error)
	GetForUpdateTSFunc func() (uint64, error)

	Pessimistic           bool
	CausalConsistencyOnly bool

	isTxnActive bool
	reuseTxn    bool
}

// GetTxnInfoSchema returns the information schema used by txn
func (p *SimpleTxnContextProvider) GetTxnInfoSchema() infoschema.InfoSchema {
	return p.InfoSchema
}

// GetStmtReadTS returns the read timestamp used by select statement (not for select ... for update)
func (p *SimpleTxnContextProvider) GetStmtReadTS() (uint64, error) {
	if p.GetReadTSFunc == nil {
		return 0, errors.New("ReadTSFunc not set")
	}
	return p.GetReadTSFunc()
}

// GetStmtForUpdateTS returns the read timestamp used by update/insert/delete or select ... for update
func (p *SimpleTxnContextProvider) GetStmtForUpdateTS() (uint64, error) {
	if p.GetForUpdateTSFunc == nil {
		return 0, errors.New("GetForUpdateTSFunc not set")
	}
	return p.GetForUpdateTSFunc()
}

// ReuseTxn reuses the old txn
func (p *SimpleTxnContextProvider) ReuseTxn() {
	p.reuseTxn = true
}

// OnInitialize is the hook that should be called when enter a new txn with this provider
func (p *SimpleTxnContextProvider) OnInitialize(ctx context.Context, activeNow bool) error {
	p.Ctx = ctx
	sessVars := p.Sctx.GetSessionVars()
	switch {
	case p.reuseTxn:
		// When rese txn, use the old TxnCtx
	case activeNow:
		if err := p.Sctx.NewTxn(ctx); err != nil {
			return err
		}
	default:
		sessVars.TxnCtx = &variable.TransactionContext{
			InfoSchema: temptable.AttachLocalTemporaryTableInfoSchema(p.Sctx, domain.GetDomain(p.Sctx).InfoSchema()),
			CreateTime: time.Now(),
			ShardStep:  int(sessVars.ShardAllocateStep),
			TxnScope:   sessVars.CheckAndGetTxnScope(),
		}
	}

	if sessVars.TxnCtx.InfoSchema != nil {
		p.InfoSchema = sessVars.TxnCtx.InfoSchema.(infoschema.InfoSchema)
	}
	sessVars.TxnCtx.IsPessimistic = p.Pessimistic

	if activeNow {
		if _, err := p.activeTxn(); err != nil {
			return err
		}
	}

	return nil
}

// OnStmtStart is the hook that should be called when a new statement started
func (p *SimpleTxnContextProvider) OnStmtStart(ctx context.Context) error {
	p.Ctx = ctx
	p.InfoSchema = p.Sctx.GetInfoSchema().(infoschema.InfoSchema)
	return nil
}

// activeTxn actives the txn
func (p *SimpleTxnContextProvider) activeTxn() (kv.Transaction, error) {
	if p.isTxnActive {
		return p.Sctx.Txn(true)
	}

	txn, err := p.Sctx.Txn(true)
	if err != nil {
		return nil, err
	}

	if p.Pessimistic {
		p.Sctx.GetSessionVars().TxnCtx.IsPessimistic = true
		txn.SetOption(kv.Pessimistic, true)
	}

	if p.CausalConsistencyOnly {
		txn.SetOption(kv.GuaranteeLinearizability, false)
	}

	p.isTxnActive = true
	return txn, nil
}
