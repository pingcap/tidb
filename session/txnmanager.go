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

package session

import (
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
)

func init() {
	sessiontxn.GetTxnManager = getTxnManager
}

func getTxnManager(sctx sessionctx.Context) sessiontxn.TxnManager {
	if manager, ok := sctx.GetSessionVars().TxnManager.(sessiontxn.TxnManager); ok {
		return manager
	}

	manager := newTxnManager(sctx)
	sctx.GetSessionVars().TxnManager = manager
	return manager
}

// txnManager implements sessiontxn.TxnManager
type txnManager struct {
	sessiontxn.TxnContextProvider
	sctx sessionctx.Context
}

func newTxnManager(sctx sessionctx.Context) *txnManager {
	return &txnManager{sctx: sctx}
}

func (m *txnManager) InExplicitTxn() bool {
	return m.sctx.GetSessionVars().TxnCtx.IsExplicit
}

func (m *txnManager) GetContextProvider() sessiontxn.TxnContextProvider {
	return m.TxnContextProvider
}

func (m *txnManager) SetContextProvider(provider sessiontxn.TxnContextProvider) error {
	m.TxnContextProvider = provider
	return nil
}
