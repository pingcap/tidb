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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
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

// GetReadTS returns the read timestamp
func (p *StalenessTxnContextProvider) GetReadTS() (uint64, error) {
	return p.ts, nil
}

// GetForUpdateTS will return an error because stale read does not support it
func (p *StalenessTxnContextProvider) GetForUpdateTS() (uint64, error) {
	return 0, errors.New("GetForUpdateTS not supported for stalenessTxnProvider")
}

// ActiveTxn actives stale read txn
func (p *StalenessTxnContextProvider) ActiveTxn(ctx context.Context) (kv.Transaction, error) {
	if err := p.sctx.NewStaleTxnWithStartTS(ctx, p.ts); err != nil {
		return nil, err
	}
	p.is = p.sctx.GetSessionVars().TxnCtx.InfoSchema.(infoschema.InfoSchema)
	return p.sctx.Txn(true)
}

// OnStmtStart is the hook that should be called when a new statement started
func (p *StalenessTxnContextProvider) OnStmtStart(_ context.Context) error {
	if p.is == nil {
		is, err := domain.GetDomain(p.sctx).GetSnapshotInfoSchema(p.ts)
		if err != nil {
			return err
		}

		p.is = temptable.AttachLocalTemporaryTableInfoSchema(p.sctx, is)
	}
	return nil
}

// OnStmtRetry is the hook that should be called when a statement is retrying
func (p *StalenessTxnContextProvider) OnStmtRetry() error {
	return nil
}

// Advise implements `TxnContextProvider.Advise`
func (p *StalenessTxnContextProvider) Advise(_ sessiontxn.AdviceOption, _ ...interface{}) error {
	return nil
}
