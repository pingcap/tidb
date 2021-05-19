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
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// TxnScopeVar indicates the used txnScope for oracle
type TxnScopeVar struct {
	// varValue indicates the value of @@txn_scope, which can only be `global` or `local`
	varValue string
	// txnScope indicates the value which the tidb-server holds to request tso to pd
	txnScope string
}

// GetTxnScopeVar gets TxnScopeVar from config
func GetTxnScopeVar() TxnScopeVar {
	isGlobal, location := config.GetTxnScopeFromConfig()
	if isGlobal {
		return NewGlobalTxnScopeVar()
	}
	return NewLocalTxnScopeVar(location)
}

// NewGlobalTxnScopeVar creates a Global TxnScopeVar
func NewGlobalTxnScopeVar() TxnScopeVar {
	return newTxnScopeVar(GlobalTxnScope, GlobalTxnScope)
}

// NewLocalTxnScopeVar creates a Local TxnScopeVar with given real txnScope value.
func NewLocalTxnScopeVar(txnScope string) TxnScopeVar {
	return newTxnScopeVar(LocalTxnScope, txnScope)
}

// GetVarValue returns the value of @@txn_scope which can only be `global` or `local`
func (t TxnScopeVar) GetVarValue() string {
	return t.varValue
}

// GetTxnScope returns the value of the tidb-server holds to request tso to pd.
func (t TxnScopeVar) GetTxnScope() string {
	return t.txnScope
}

func newTxnScopeVar(varValue string, txnScope string) TxnScopeVar {
	return TxnScopeVar{
		varValue: varValue,
		txnScope: txnScope,
	}
}

// Transaction scopes constants.
const (
	// GlobalTxnScope is synced with PD's define of global scope.
	// If we want to remove the dependency on store/tikv here, we need to map
	// the two GlobalTxnScopes in the driver layer.
	GlobalTxnScope = oracle.GlobalTxnScope
	// LocalTxnScope indicates the transaction should use local ts.
	LocalTxnScope = "local"
)
