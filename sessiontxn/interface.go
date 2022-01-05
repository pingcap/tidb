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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
)

// TxnContextProvider provides txn context
type TxnContextProvider interface {
	// Initialize the provider with session context
	Initialize(sctx sessionctx.Context) error
	// GetTxnInfoSchema returns the information schema used by txn
	GetTxnInfoSchema() infoschema.InfoSchema
}

// SimpleTxnContextProvider implements TxnContextProvider
// It is only used in refactor stage
// TODO: remove it after refactor finished
type SimpleTxnContextProvider struct {
	InfoSchema infoschema.InfoSchema
}

// Initialize the provider with session context
func (p *SimpleTxnContextProvider) Initialize(_ sessionctx.Context) error {
	return nil
}

// GetTxnInfoSchema returns the information schema used by txn
func (p *SimpleTxnContextProvider) GetTxnInfoSchema() infoschema.InfoSchema {
	return p.InfoSchema
}

// TxnManager is an interface providing txn context management in session
type TxnManager interface {
	// GetTxnInfoSchema returns the information schema used by txn
	GetTxnInfoSchema() infoschema.InfoSchema

	// GetContextProvider returns the current TxnContextProvider
	GetContextProvider() TxnContextProvider
	// SetContextProvider sets the context provider
	SetContextProvider(provider TxnContextProvider) error
}

// GetTxnManager returns the TxnManager object from session context
var GetTxnManager func(sctx sessionctx.Context) TxnManager
