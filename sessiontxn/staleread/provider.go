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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table/temptable"
)

// StalenessTxnContextProvider implements sessiontxn.TxnContextProvider
type StalenessTxnContextProvider struct {
	sctx sessionctx.Context
	is   infoschema.InfoSchema
	ts   uint64
}

// NewStalenessTxnContextProvider creates a new StalenessTxnContextProvider
func NewStalenessTxnContextProvider(sctx sessionctx.Context, ts uint64, is infoschema.InfoSchema) *StalenessTxnContextProvider {
	return &StalenessTxnContextProvider{
		sctx: sctx,
		is:   is,
		ts:   ts,
	}
}

// GetTxnInfoSchema returns the information schema used by txn
func (p *StalenessTxnContextProvider) GetTxnInfoSchema() infoschema.InfoSchema {
	return p.is
}

// GetStmtReadTS returns the read timestamp
func (p *StalenessTxnContextProvider) GetStmtReadTS() (uint64, error) {
	return p.ts, nil
}

// GetStmtForUpdateTS will return an error because stale read does not support it
func (p *StalenessTxnContextProvider) GetStmtForUpdateTS() (uint64, error) {
	return 0, errors.New("GetForUpdateTS not supported for stalenessTxnProvider")
}

// OnInitialize is the hook that should be called when enter a new txn with this provider
func (p *StalenessTxnContextProvider) OnInitialize(ctx context.Context, tp sessiontxn.EnterNewTxnType) error {
	switch tp {
	case sessiontxn.EnterNewTxnDefault, sessiontxn.EnterNewTxnWithBeginStmt:
		if err := p.sctx.NewStaleTxnWithStartTS(ctx, p.ts); err != nil {
			return err
		}
		p.is = p.sctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema)
		if err := p.sctx.GetSessionVars().SetSystemVar(variable.TiDBSnapshot, ""); err != nil {
			return err
		}
	case sessiontxn.EnterNewTxnWithReplaceProvider:
		if p.is == nil {
			is, err := GetSessionSnapshotInfoSchema(p.sctx, p.ts)
			if err != nil {
				return err
			}
			p.is = temptable.AttachLocalTemporaryTableInfoSchema(p.sctx, is)
		}
	default:
		return errors.Errorf("Unsupported type: %v", tp)
	}

	txnCtx := p.sctx.GetSessionVars().TxnCtx
	txnCtx.IsStaleness = true
	txnCtx.InfoSchema = p.is
	return nil
}

// OnStmtStart is the hook that should be called when a new statement started
func (p *StalenessTxnContextProvider) OnStmtStart(_ context.Context) error {
	return nil
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (p *StalenessTxnContextProvider) OnStmtErrorForNextAction(_ sessiontxn.StmtErrorHandlePoint, _ error) (sessiontxn.StmtErrorAction, error) {
	return sessiontxn.NoIdea()
}

// OnStmtRetry is the hook that should be called when a statement retry
func (p *StalenessTxnContextProvider) OnStmtRetry(_ context.Context) error {
	return nil
}
