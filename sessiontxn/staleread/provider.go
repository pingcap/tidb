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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
)

// StalenessTxnContextProvider implements sessiontxn.TxnContextProvider
type StalenessTxnContextProvider struct {
	is infoschema.InfoSchema
	ts uint64
}

// NewStalenessTxnContextProvider creates a new StalenessTxnContextProvider
func NewStalenessTxnContextProvider(is infoschema.InfoSchema, ts uint64) *StalenessTxnContextProvider {
	return &StalenessTxnContextProvider{
		is: is,
		ts: ts,
	}
}

// Initialize the provider with session context
func (p *StalenessTxnContextProvider) Initialize(_ sessionctx.Context) error {
	return nil
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
