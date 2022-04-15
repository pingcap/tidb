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

package sessiontxn

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
)

type AdviceOption int64

const (
	AdviceWarmUpNow AdviceOption = iota
	AdviceNoConflictForUpdateTS
)

// TxnContextProvider provides txn context
type TxnContextProvider interface {
	// GetTxnInfoSchema returns the information schema used by txn
	GetTxnInfoSchema() infoschema.InfoSchema
	// GetReadTS returns the read timestamp used by select statement (not for select ... for update)
	GetReadTS() (uint64, error)
	// GetForUpdateTS returns the read timestamp used by update/insert/delete or select ... for update
	GetForUpdateTS() (uint64, error)

	// ActiveTxn actives the txn
	ActiveTxn(ctx context.Context) (kv.Transaction, error)
	// OnStmtStart is the hook that should be called when a new statement started
	OnStmtStart(ctx context.Context) error
	// OnStmtRetry is the hook that should be called when a statement is retrying
	OnStmtRetry() error
	// Advise is used to provide some extra information to make a better performance.
	// For example, we can give `AdviceWarmUpNow` to advice provider prefetch tso.
	// Give or not give an advice should not affect the correctness.
	Advise(opt AdviceOption, val ...interface{}) error
}

// SimpleTxnContextProvider implements TxnContextProvider
// It is only used in refactor stage
// TODO: remove it after refactor finished
type SimpleTxnContextProvider struct {
	ctx                   context.Context
	Sctx                  sessionctx.Context
	CausalConsistencyOnly bool
	stmtStarted           bool
	InfoSchema            infoschema.InfoSchema
	GetReadTSFunc         func() (uint64, error)
	GetForUpdateTSFunc    func() (uint64, error)
}

// GetTxnInfoSchema returns the information schema used by txn
func (p *SimpleTxnContextProvider) GetTxnInfoSchema() infoschema.InfoSchema {
	return p.InfoSchema
}

// GetReadTS returns the read timestamp used by select statement (not for select ... for update)
func (p *SimpleTxnContextProvider) GetReadTS() (uint64, error) {
	if p.GetReadTSFunc == nil {
		return 0, errors.New("ReadTSFunc not set")
	}
	return p.GetReadTSFunc()
}

// GetForUpdateTS returns the read timestamp used by update/insert/delete or select ... for update
func (p *SimpleTxnContextProvider) GetForUpdateTS() (uint64, error) {
	if p.GetForUpdateTSFunc == nil {
		return 0, errors.New("GetForUpdateTSFunc not set")
	}
	return p.GetForUpdateTSFunc()
}

// ActiveTxn actives the txn
func (p *SimpleTxnContextProvider) ActiveTxn(ctx context.Context) (kv.Transaction, error) {
	if !p.stmtStarted {
		if err := p.Sctx.NewTxn(ctx); err != nil {
			return nil, err
		}
	} else {
		p.Sctx.PrepareTSFuture(ctx)
	}

	txnMode, _ := ctx.Value("txnMode").(string)
	if txnMode == "" {
		txnMode = p.Sctx.GetSessionVars().TxnMode
	}
	if txnMode == ast.Pessimistic {
		p.Sctx.GetSessionVars().TxnCtx.IsPessimistic = true
	}
	txn, err := p.Sctx.Txn(true)
	if err != nil {
		return nil, err
	}
	if p.Sctx.GetSessionVars().TxnCtx.IsPessimistic {
		txn.SetOption(kv.Pessimistic, true)
	}
	if p.CausalConsistencyOnly {
		txn.SetOption(kv.GuaranteeLinearizability, false)
	}
	return txn, nil
}

// OnStmtStart is the hook that should be called when a new statement started
func (p *SimpleTxnContextProvider) OnStmtStart(ctx context.Context) error {
	p.ctx = ctx
	p.InfoSchema = p.Sctx.GetInfoSchema().(infoschema.InfoSchema)
	p.stmtStarted = true
	return nil
}

// OnStmtRetry is the hook that should be called when a statement is retrying
func (p *SimpleTxnContextProvider) OnStmtRetry() error {
	return nil
}

// Advise implements `TxnContextProvider.Advise`
func (p *SimpleTxnContextProvider) Advise(opt AdviceOption, _ ...interface{}) error {
	switch opt {
	case AdviceWarmUpNow:
		p.Sctx.PrepareTSFuture(p.ctx)
	}
	return nil
}

func UsingNonSimpleProvider(sctx sessionctx.Context) bool {
	_, ok := GetTxnManager(sctx).GetContextProvider().(*SimpleTxnContextProvider)
	return ok
}

// TxnManager is an interface providing txn context management in session
type TxnManager interface {
	// GetTxnInfoSchema returns the information schema used by txn
	GetTxnInfoSchema() infoschema.InfoSchema
	// GetReadTS returns the read timestamp used by select statement (not for select ... for update)
	GetReadTS() (uint64, error)
	// GetForUpdateTS returns the read timestamp used by update/insert/delete or select ... for update
	GetForUpdateTS() (uint64, error)

	// ActiveTxn actives the txn
	ActiveTxn(ctx context.Context) (kv.Transaction, error)
	// GetContextProvider returns the current TxnContextProvider
	GetContextProvider() TxnContextProvider
	// SetContextProvider sets the context provider
	SetContextProvider(provider TxnContextProvider) error

	// OnStmtStart is the hook that should be called when a new statement started
	OnStmtStart(ctx context.Context) error
	// OnStmtRetry is the hook that should be called when a statement is retrying
	OnStmtRetry() error
	// Advise is used to provide some extra information to make a better performance.
	// For example, we can give `AdviceWarmUpNow` to advice provider prefetch tso.
	// Give or not give an advice should not affect the correctness.
	Advise(opt AdviceOption, val ...interface{}) error
}

func AdviseTxnWarmUp(sctx sessionctx.Context) error {
	return GetTxnManager(sctx).Advise(AdviceWarmUpNow)
}

func AdviseNoConflictUpdateTS(sctx sessionctx.Context, ts uint64) error {
	return GetTxnManager(sctx).Advise(AdviceNoConflictForUpdateTS, ts)
}

func WithValUint64(val []interface{}, fn func(uint64) error) error {
	if len(val) == 0 {
		return errors.New("%v need a uint64 arg")
	}

	if ts, ok := val[0].(uint64); ok {
		return fn(ts)
	}

	return errors.New("%v need a uint64 arg")

}

// GetTxnManager returns the TxnManager object from session context
var GetTxnManager func(sctx sessionctx.Context) TxnManager
