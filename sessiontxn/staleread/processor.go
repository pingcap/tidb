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

type baseProcessor struct {
	sctx               sessionctx.Context
	txnManager         sessiontxn.TxnManager
	isUpdateTxnContext bool

	evaluated                  bool
	ts                         uint64
	is                         infoschema.InfoSchema
	readReplicaScope           string
	tsEvaluatorForPreparedStmt func(sctx sessionctx.Context) (uint64, error)
}

func (p *baseProcessor) init(sctx sessionctx.Context, isUpdateTxnContext bool) {
	p.sctx = sctx
	p.txnManager = sessiontxn.GetTxnManager(sctx)
	p.isUpdateTxnContext = isUpdateTxnContext
}

func (p *baseProcessor) IsStaleness() bool {
	return p.ts != 0
}

func (p *baseProcessor) GetStaleReadTS() uint64 {
	return p.ts
}

func (p *baseProcessor) GetStaleReadInfoSchema() infoschema.InfoSchema {
	return p.is
}

func (p *StmtPreprocessor) GetTSEvaluatorForPreparedStmt() func(sctx sessionctx.Context) (uint64, error) {
	return p.tsEvaluatorForPreparedStmt
}

func (p *baseProcessor) setEvaluatedTS(ts uint64, readReplicaScope string) error {
	if p.evaluated {
		return errors.New("already evaluated")
	}

	if readReplicaScope == "" {
		readReplicaScope = kv.GlobalReplicaScope
	}

	if ts != 0 {
		is, err := domain.GetDomain(p.sctx).GetSnapshotInfoSchema(ts)
		if err != nil {
			return err
		}
		p.is = temptable.AttachLocalTemporaryTableInfoSchema(p.sctx, is)
		p.readReplicaScope = readReplicaScope
	}

	p.ts = ts
	p.evaluated = true
	return nil
}

func (p *baseProcessor) buildContextProvider() sessiontxn.TxnContextProvider {
	if p.ts == 0 {
		return nil
	}

	return &staleReadTxnContextProvider{
		is:               p.is,
		ts:               p.ts,
		readReplicaScope: p.readReplicaScope,
	}
}

func (p *baseProcessor) getAsOfTS(asOf *ast.AsOfClause) (uint64, error) {
	if asOf == nil {
		return 0, nil
	}

	staleReadTS, err := calculateAsOfTsExpr(p.sctx, asOf)
	if err != nil {
		return 0, err
	}

	if err = ValidateStaleReadTS(context.TODO(), p.sctx, staleReadTS); err != nil {
		return 0, err
	}

	return staleReadTS, nil
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

func (p *baseProcessor) getReadStalenessTS() (uint64, func(sctx sessionctx.Context) (uint64, error), error) {
	readStaleness := p.sctx.GetSessionVars().ReadStaleness
	if readStaleness == 0 {
		return 0, nil, nil
	}

	staleReadTS, err := calculateTsWithReadStaleness(p.sctx, readStaleness)
	if err != nil {
		return 0, nil, err
	}

	return staleReadTS, func(sctx sessionctx.Context) (uint64, error) {
		return calculateTsWithReadStaleness(sctx, readStaleness)
	}, nil
}

func (p *baseProcessor) onStmtTS(ts uint64, readReplicaScope string) error {
	if p.txnManager.InExplicitTxn() {
		if ts != 0 {
			return errAsOf.FastGenWithCause("as of timestamp can't be set in transaction.")
		}
		return nil
	}

	staleReadTS, err := p.useStmtOrTxnReadTS(ts)
	if err != nil {
		return err
	}

	var tsEvaluatorForPreparedStmt func(sctx sessionctx.Context) (uint64, error)
	if staleReadTS == 0 {
		staleReadTS, tsEvaluatorForPreparedStmt, err = p.getReadStalenessTS()
		if err != nil {
			return err
		}
	} else {
		tsEvaluatorForPreparedStmt = func(sctx sessionctx.Context) (uint64, error) {
			return staleReadTS, nil
		}
	}

	if !p.evaluated {
		err = p.setEvaluatedTS(staleReadTS, readReplicaScope)
		if err != nil {
			return err
		}
		p.tsEvaluatorForPreparedStmt = tsEvaluatorForPreparedStmt

		if p.isUpdateTxnContext {
			provider := p.buildContextProvider()
			if provider != nil {
				if err = p.txnManager.SetContextProvider(provider); err != nil {
					return err
				}
			}
		}
	} else if staleReadTS != p.ts {
		return errAsOf.GenWithStack("can not set different time in the as of")
	}

	return nil
}

type StmtPreprocessor struct {
	baseProcessor
}

func NewStmtPreprocessor(sctx sessionctx.Context, isUpdateTxnContext bool) *StmtPreprocessor {
	p := &StmtPreprocessor{}
	p.init(sctx, isUpdateTxnContext)
	return p
}

func (p *StmtPreprocessor) OnTSEvaluatorInExecute(evaluator func(sctx sessionctx.Context) (uint64, error)) (err error) {
	ts := uint64(0)
	if evaluator != nil {
		ts, err = evaluator(p.sctx)
		if err != nil {
			return err
		}
	}

	if err = p.onStmtTS(ts, config.GetTxnScopeFromConfig()); err != nil {
		return err
	}

	return nil
}

func (p *StmtPreprocessor) OnSelectTable(tn *ast.TableName) error {
	ts, err := p.getAsOfTS(tn.AsOf)
	if err != nil {
		return err
	}
	return p.onStmtTS(ts, getStaleReadReplicaScope(p.sctx))
}

func (p *StmtPreprocessor) OnStartTransaction(begin *ast.BeginStmt) error {
	if !p.isUpdateTxnContext {
		return errors.New("Must update txn context when start transaction")
	}

	ts, err := p.getAsOfTS(begin.AsOf)
	if err != nil {
		return err
	}

	if !p.txnManager.InExplicitTxn() {
		if p.sctx.GetSessionVars().TxnReadTS.PeakTxnReadTS() != 0 && ts != 0 {
			return errors.New("start transaction read only as of is forbidden after set transaction read only as of")
		}

		if ts == 0 {
			ts = p.sctx.GetSessionVars().TxnReadTS.UseTxnReadTS()
		}
	}

	if err = p.setEvaluatedTS(ts, config.GetTxnScopeFromConfig()); err != nil {
		return err
	}

	if provider := p.buildContextProvider(); provider != nil {
		p.sctx.GetSessionVars().StmtCtx.ContextProviderForBeginStmt = provider
		if !p.txnManager.InExplicitTxn() {
			if err = p.txnManager.SetContextProvider(provider); err != nil {
				return err
			}
		}
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
