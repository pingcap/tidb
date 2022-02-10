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

package staleread

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table/temptable"
)

type PreparedTSEvaluator func(sctx sessionctx.Context) (uint64, error)

type ProcessType uint8

const (
	ProcessTypeInitTxnContext ProcessType = iota
	ProcessTypeParsePrepare
	ProcessTypeValidateCreateView
)

// StmtProcessor processes the stale read stmt
type StmtProcessor interface {
	// GetType returns the process type
	GetType() ProcessType
	// IsStmtStaleness indicates that whether the stmt has the staleness declaration
	IsStmtStaleness() bool
	// GetStalenessInfoSchema returns the information schema if it is stale read, otherwise returns nil
	GetStalenessInfoSchema() infoschema.InfoSchema
	// GetStalenessReadTS returns the ts if it is stale read, otherwise returns 0
	GetStalenessReadTS() uint64

	// OnSelectTable will be called when process table in select statement.
	// Support type: ProcessTypeInitTxnContext, ProcessTypeParsePrepare, ProcessTypeValidateCreateView
	OnSelectTable(tn *ast.TableName) error
	// OnExecuteStmtWithPreparedTS will be called when process execute statement
	// Support type: ProcessTypeInitTxnContext
	OnExecuteStmtWithPreparedTS(evaluator PreparedTSEvaluator) error
	// OnBeginStmt will be called when process begin statement
	// Support type: ProcessTypeInitTxnContext
	OnBeginStmt(begin *ast.BeginStmt) error
}

type baseProcessor struct {
	tp         ProcessType
	sctx       sessionctx.Context
	txnManager sessiontxn.TxnManager

	evaluated bool
	ts        uint64
	is        infoschema.InfoSchema
}

func (p *baseProcessor) init(sctx sessionctx.Context, tp ProcessType) {
	p.tp = tp
	p.sctx = sctx
	p.txnManager = sessiontxn.GetTxnManager(sctx)
}

func (p *baseProcessor) GetType() ProcessType {
	return p.tp
}

func (p *baseProcessor) IsStmtStaleness() bool {
	return p.ts != 0
}

func (p *baseProcessor) GetStalenessInfoSchema() infoschema.InfoSchema {
	return p.is
}

func (p *baseProcessor) GetStalenessReadTS() uint64 {
	return p.ts
}

func (p *baseProcessor) OnSelectTable(_ *ast.TableName) error {
	return errors.New("not supported")
}

func (p *baseProcessor) OnExecuteStmtWithPreparedTS(_ PreparedTSEvaluator) error {
	return errors.New("not supported")
}

func (p *baseProcessor) OnBeginStmt(_ *ast.BeginStmt) error {
	return errors.New("not supported")
}

func (p *baseProcessor) setEvaluatedTS(ts uint64) error {
	if p.evaluated {
		if ts != p.ts {
			return errAsOf.GenWithStack("can not set different time in the as of")
		}
		return nil
	}

	if ts != 0 {
		is, err := domain.GetDomain(p.sctx).GetSnapshotInfoSchema(ts)
		if err != nil {
			return err
		}
		p.is = temptable.AttachLocalTemporaryTableInfoSchema(p.sctx, is)
	}

	p.ts = ts
	p.evaluated = true
	return nil
}

func (p *baseProcessor) buildContextProvider(readReplicaScope string) sessiontxn.TxnContextProvider {
	if readReplicaScope == "" {
		readReplicaScope = kv.GlobalReplicaScope
	}

	return &staleReadTxnContextProvider{
		is:               p.is,
		ts:               p.ts,
		readReplicaScope: readReplicaScope,
	}
}

func (p *baseProcessor) useStmtOrTxnReadTS(stmtTS uint64) (ts uint64, err error) {
	staleReadTS := p.sctx.GetSessionVars().TxnReadTS.UseTxnReadTS()
	if staleReadTS != 0 && stmtTS != 0 {
		return 0, errAsOf.FastGenWithCause("can't use select as of while already set transaction as of")
	}

	if staleReadTS == 0 {
		staleReadTS = stmtTS
	}

	return staleReadTS, nil
}

type InitTxnContextProcessor struct {
	baseProcessor
}

func NewInitContextProcessor(sctx sessionctx.Context) *InitTxnContextProcessor {
	p := &InitTxnContextProcessor{}
	p.init(sctx, ProcessTypeInitTxnContext)
	return p
}

func (p *InitTxnContextProcessor) OnSelectTable(tn *ast.TableName) (err error) {
	ts, err := parseAndValidateAsOf(p.sctx, tn.AsOf)
	if err != nil {
		return err
	}

	return p.onSelectOrExecuteTS(ts)
}

func (p *InitTxnContextProcessor) OnExecuteStmtWithPreparedTS(evaluator PreparedTSEvaluator) (err error) {
	var ts uint64
	if evaluator != nil {
		ts, err = evaluator(p.sctx)
		if err != nil {
			return err
		}
	}

	return p.onSelectOrExecuteTS(ts)
}

func (p *InitTxnContextProcessor) OnBeginStmt(begin *ast.BeginStmt) error {
	ts, err := parseAndValidateAsOf(p.sctx, begin.AsOf)
	if err != nil {
		return err
	}

	txnReadTS := p.sctx.GetSessionVars().TxnReadTS
	if txnReadTS.PeakTxnReadTS() != 0 && ts != 0 {
		return errors.New("start transaction read only as of is forbidden after set transaction read only as of")
	}

	if ts == 0 {
		ts = txnReadTS.UseTxnReadTS()
	}

	if err = p.setEvaluatedTS(ts); err != nil {
		return err
	}

	var provider sessiontxn.TxnContextProvider
	if ts != 0 {
		provider = p.buildContextProvider(config.GetTxnScopeFromConfig())
	}

	p.sctx.GetSessionVars().StmtCtx.ContextProviderForBeginStmt = provider
	if !p.txnManager.InExplicitTxn() && provider != nil {
		err = p.txnManager.SetContextProvider(provider)
	}
	return err
}

func (p *InitTxnContextProcessor) onSelectOrExecuteTS(ts uint64) (err error) {
	if p.txnManager.InExplicitTxn() {
		// When in explicit txn, it is not allowed to declare stale read in statement
		// and the sys variables should also be ignored no matter it is set or not
		if ts != 0 {
			return errAsOf.FastGenWithCause("as of timestamp can't be set in transaction.")
		}
		return p.setEvaluatedTS(0)
	}

	if txnReadTS := p.sctx.GetSessionVars().TxnReadTS.UseTxnReadTS(); txnReadTS != 0 {
		if ts != 0 {
			return errAsOf.FastGenWithCause("can't use select as of while already set transaction as of")
		}
		ts = txnReadTS
	}

	if ts == 0 {
		evaluator := getTsEvaluatorFromReadStaleness(p.sctx)
		if evaluator != nil {
			ts, err = evaluator(p.sctx)
		}

		if err != nil {
			return err
		}
	}

	if err = p.setEvaluatedTS(ts); err != nil {
		return err
	}

	if ts != 0 {
		err = p.txnManager.SetContextProvider(p.buildContextProvider(config.GetTxnScopeFromConfig()))
	}

	return err
}

type PrepareParseProcessor struct {
	baseProcessor
	setPreparedTSEvaluator func(evaluator PreparedTSEvaluator)
}

func NewPrepareParseProcessor(sctx sessionctx.Context, setEvaluator func(evaluator PreparedTSEvaluator)) *PrepareParseProcessor {
	p := &PrepareParseProcessor{}
	p.init(sctx, ProcessTypeParsePrepare)
	p.setPreparedTSEvaluator = setEvaluator
	return p
}

func (p *PrepareParseProcessor) OnSelectTable(tn *ast.TableName) (err error) {
	ts, err := parseAndValidateAsOf(p.sctx, tn.AsOf)
	if err != nil {
		return err
	}

	if p.txnManager.InExplicitTxn() {
		// Prepare in explicit txn will ignore sys variables
		return p.setEvaluatedTS(ts)
	}

	if txnReadTS := p.sctx.GetSessionVars().TxnReadTS.UseTxnReadTS(); txnReadTS != 0 {
		if ts != 0 {
			return errAsOf.FastGenWithCause("can't use select as of while already set transaction as of")
		}
		ts = txnReadTS
	}

	var evaluator PreparedTSEvaluator
	if ts == 0 {
		evaluator = getTsEvaluatorFromReadStaleness(p.sctx)
		if evaluator != nil {
			ts, err = evaluator(p.sctx)
			if err != nil {
				return err
			}
		}
	} else {
		evaluator = func(sctx sessionctx.Context) (uint64, error) {
			return ts, nil
		}
	}

	if err = p.setEvaluatedTS(ts); err != nil {
		return err
	}

	p.setPreparedTSEvaluator(evaluator)
	return
}

type CreateViewProcessor struct {
	baseProcessor
	getError func() error
}

func NewCreateViewProcessor(sctx sessionctx.Context, getError func() error) *CreateViewProcessor {
	p := &CreateViewProcessor{}
	p.init(sctx, ProcessTypeParsePrepare)
	p.getError = getError
	return p
}

func (p *CreateViewProcessor) OnSelectTable(tn *ast.TableName) (err error) {
	if tn.AsOf != nil {
		return p.getError()
	}
	return nil
}

func ShouldStartStaleReadTxn(sctx sessionctx.Context) bool {
	provider := sctx.GetSessionVars().StmtCtx.ContextProviderForBeginStmt
	if provider == nil {
		return false
	}

	_, ok := provider.(*staleReadTxnContextProvider)
	return ok
}

func ExplicitStartStaleReadTxn(ctx context.Context, sctx sessionctx.Context) error {
	provider := sctx.GetSessionVars().StmtCtx.ContextProviderForBeginStmt.(*staleReadTxnContextProvider)
	if err := sctx.NewStaleTxnWithStartTS(ctx, provider.ts); err != nil {
		return err
	}
	if err := sessiontxn.GetTxnManager(sctx).SetContextProvider(provider); err != nil {
		return err
	}
	// With START TRANSACTION, autocommit remains disabled until you end
	// the transaction with COMMIT or ROLLBACK. The autocommit mode then
	// reverts to its previous state.
	vars := sctx.GetSessionVars()
	if err := vars.SetSystemVar(variable.TiDBSnapshot, ""); err != nil {
		return errors.Trace(err)
	}
	vars.SetInTxn(true)
	return nil
}
