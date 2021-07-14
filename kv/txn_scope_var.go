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
	"github.com/tikv/client-go/v2/oracle"
)

// TxnScopeVar indicates the used txnScope for oracle
type TxnScopeVar struct {
	// varValue indicates the value of @@txn_scope, which can only be `global` or `local`
	varValue string
	// txnScope indicates the value which the tidb-server holds to request tso to pd
	txnScope string
}

// NewDefaultTxnScopeVar creates a default TxnScopeVar according to the config.
// If zone label is set, we will check whether it's not the GlobalTxnScope and create a new Local TxnScopeVar.
// If zone label is not set, we will create a new Global TxnScopeVar.
func NewDefaultTxnScopeVar() TxnScopeVar {
	if txnScope := config.GetTxnScopeFromConfig(); txnScope != GlobalTxnScope {
		return NewLocalTxnScopeVar(txnScope)
	}
	return NewGlobalTxnScopeVar()
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
// When varValue is 'global`, directly return global here
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
