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
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table/temptable"
)

// enforce implement Processor interface
var _ Processor = &staleReadProcessor{}

// StalenessTSEvaluator is a function to get staleness ts
type StalenessTSEvaluator func(sctx sessionctx.Context) (uint64, error)

// Processor is an interface used to process stale read
type Processor interface {
	// IsStaleness indicates that whether we should use the staleness
	IsStaleness() bool
	// GetStalenessInfoSchema returns the information schema if it is stale read, otherwise returns nil
	GetStalenessInfoSchema() infoschema.InfoSchema
	// GetStalenessReadTS returns the ts if it is stale read, otherwise returns 0
	GetStalenessReadTS() uint64
	// GetStalenessTSEvaluatorForPrepare returns a function that will be used by prepare to evaluate ts
	GetStalenessTSEvaluatorForPrepare() StalenessTSEvaluator

	// OnSelectTable will be called when process table in select statement
	OnSelectTable(tn *ast.TableName) error
}

type baseProcessor struct {
	sctx       sessionctx.Context
	txnManager sessiontxn.TxnManager

	evaluated   bool
	ts          uint64
	tsEvaluator StalenessTSEvaluator
	is          infoschema.InfoSchema
}

func (p *baseProcessor) init(sctx sessionctx.Context) {
	p.sctx = sctx
	p.txnManager = sessiontxn.GetTxnManager(sctx)
}

func (p *baseProcessor) IsStaleness() bool {
	return p.ts != 0
}

func (p *baseProcessor) GetStalenessInfoSchema() infoschema.InfoSchema {
	return p.is
}

func (p *baseProcessor) GetStalenessReadTS() uint64 {
	return p.ts
}

func (p *baseProcessor) GetStalenessTSEvaluatorForPrepare() StalenessTSEvaluator {
	return p.tsEvaluator
}

func (p *baseProcessor) OnSelectTable(_ *ast.TableName) error {
	return errors.New("not supported")
}

func (p *baseProcessor) setAsNonStaleRead() error {
	return p.setEvaluatedValues(0, nil, nil)
}

func (p *baseProcessor) setEvaluatedTS(ts uint64) (err error) {
	is, err := domain.GetDomain(p.sctx).GetSnapshotInfoSchema(ts)
	if err != nil {
		return err
	}
	is = temptable.AttachLocalTemporaryTableInfoSchema(p.sctx, is)
	return p.setEvaluatedValues(ts, is, func(sctx sessionctx.Context) (uint64, error) {
		return ts, nil
	})
}

func (p *baseProcessor) setEvaluatedEvaluator(evaluator StalenessTSEvaluator) error {
	ts, err := evaluator(p.sctx)
	if err != nil {
		return err
	}

	is, err := domain.GetDomain(p.sctx).GetSnapshotInfoSchema(ts)
	is = temptable.AttachLocalTemporaryTableInfoSchema(p.sctx, is)
	if err != nil {
		return err
	}

	return p.setEvaluatedValues(ts, is, evaluator)
}

func (p *baseProcessor) setEvaluatedValues(ts uint64, is infoschema.InfoSchema, tsEvaluator StalenessTSEvaluator) error {
	if p.evaluated {
		return errors.New("already evaluated")
	}

	p.ts = ts
	p.is = is
	p.evaluated = true
	p.tsEvaluator = tsEvaluator
	return nil
}

type staleReadProcessor struct {
	baseProcessor
	stmtAsOfTs uint64
}

// NewStaleReadProcessor creates a new stale read processor
func NewStaleReadProcessor(sctx sessionctx.Context) Processor {
	p := &staleReadProcessor{}
	p.init(sctx)
	return p
}

// OnSelectTable will be called when process table in select statement
func (p *staleReadProcessor) OnSelectTable(tn *ast.TableName) error {
	if p.sctx.GetSessionVars().InTxn() {
		// When in explicit txn, it is not allowed to declare stale read in statement
		// and the sys variables should also be ignored no matter it is set or not
		if tn.AsOf != nil {
			return errAsOf.FastGenWithCause("as of timestamp can't be set in transaction.")
		}

		if p.evaluated {
			return nil
		}

		if txnCtx := p.sctx.GetSessionVars().TxnCtx; txnCtx.IsStaleness {
			// It means we meet following case:
			// 1. start transaction read only as of timestamp ts
			// 2. select statement
			return p.setEvaluatedValues(
				txnCtx.StartTS,
				temptable.AttachLocalTemporaryTableInfoSchema(p.sctx, txnCtx.InfoSchema.(infoschema.InfoSchema)),
				nil,
			)
		}
		return p.setAsNonStaleRead()
	}

	// If `stmtAsOfTS` is not 0, it means we use 'select ... from xxx as of timestamp ...'
	stmtAsOfTS, err := parseAndValidateAsOf(p.sctx, tn.AsOf)
	if err != nil {
		return err
	}

	if p.evaluated {
		// If the select statement is related to multi tables, we should guarantee that all tables use the same timestamp
		if p.stmtAsOfTs != stmtAsOfTS {
			return errAsOf.GenWithStack("can not set different time in the as of")
		}
		return nil
	}

	// If `txnReadTS` is not 0, it means  we meet following situation:
	// start transaction read only as of timestamp ...
	// select from table
	txnReadTS := p.sctx.GetSessionVars().TxnReadTS.UseTxnReadTS()
	if txnReadTS > 0 && stmtAsOfTS > 0 {
		// `as of` and `@@tx_read_ts` cannot be set in the same time
		return errAsOf.FastGenWithCause("can't use select as of while already set transaction as of")
	}

	if stmtAsOfTS > 0 {
		p.stmtAsOfTs = stmtAsOfTS
		return p.setEvaluatedTS(stmtAsOfTS)
	}

	if txnReadTS > 0 {
		return p.setEvaluatedTS(txnReadTS)
	}

	// If both txnReadTS and stmtAsOfTS is empty while the readStaleness isn't, it means we meet following situation:
	// set @@tidb_read_staleness='-5';
	// select from table
	// Then the following select statement should be affected by the tidb_read_staleness in session.
	if evaluator := getTsEvaluatorFromReadStaleness(p.sctx); evaluator != nil {
		return p.setEvaluatedEvaluator(evaluator)
	}

	// Otherwise, it means we should not use stale read.
	return p.setAsNonStaleRead()
}

func parseAndValidateAsOf(sctx sessionctx.Context, asOf *ast.AsOfClause) (uint64, error) {
	if asOf == nil {
		return 0, nil
	}

	ts, err := CalculateAsOfTsExpr(sctx, asOf)
	if err != nil {
		return 0, err
	}

	if err = sessionctx.ValidateStaleReadTS(context.TODO(), sctx, ts); err != nil {
		return 0, err
	}

	return ts, nil
}

func getTsEvaluatorFromReadStaleness(sctx sessionctx.Context) StalenessTSEvaluator {
	readStaleness := sctx.GetSessionVars().ReadStaleness
	if readStaleness == 0 {
		return nil
	}

	return func(sctx sessionctx.Context) (uint64, error) {
		return CalculateTsWithReadStaleness(sctx, readStaleness)
	}
}
