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
	"context"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
)

type EnterNewTxnType int

const (
	EnterNewTxnDefault EnterNewTxnType = iota
	EnterNewTxnWithBeginStmt
	EnterNewTxnBeforeStmt
	EnterNewTxnWithReplaceProvider
)

// EnterNewTxnRequest is the request when entering a new transaction
type EnterNewTxnRequest struct {
	// Type is the type for new entering a new txn
	Type EnterNewTxnType
	// provider is the context provider
	Provider TxnContextProvider
	// txnMode is the transaction mode for the new txn. It has 3 values: `ast.Pessimistic` ,`ast.Optimistic` or empty/
	// When the value is empty, it means the value will be determined from sys vars.
	TxnMode string
	// causalConsistencyOnly means whether enable causal consistency for transactions, default is false
	CausalConsistencyOnly bool
	// staleReadTS indicates the read ts for the stale read transaction.
	//The default value is zero which means not a stale read transaction.
	StaleReadTS uint64
}

// TxnContextProvider provides txn context
type TxnContextProvider interface {
	// GetTxnInfoSchema returns the information schema used by txn
	GetTxnInfoSchema() infoschema.InfoSchema
	// GetStmtReadTS returns the read timestamp used by select statement (not for select ... for update)
	GetStmtReadTS() (uint64, error)
	// GetStmtForUpdateTS returns the read timestamp used by update/insert/delete or select ... for update
	GetStmtForUpdateTS() (uint64, error)

	// OnInitialize is the hook that should be called when enter a new txn with this provider
	OnInitialize(ctx context.Context, enterNewTxnType EnterNewTxnType) error
	// OnStmtStart is the hook that should be called when a new statement started
	OnStmtStart(ctx context.Context) error
}

// TxnManager is an interface providing txn context management in session
type TxnManager interface {
	// GetTxnInfoSchema returns the information schema used by txn
	GetTxnInfoSchema() infoschema.InfoSchema
	// GetStmtReadTS returns the read timestamp used by select statement (not for select ... for update)
	GetStmtReadTS() (uint64, error)
	// GetStmtForUpdateTS returns the read timestamp used by update/insert/delete or select ... for update
	GetStmtForUpdateTS() (uint64, error)
	// GetContextProvider returns the current TxnContextProvider
	GetContextProvider() TxnContextProvider

	// EnterNewTxn enters a new transaction.
	EnterNewTxn(ctx context.Context, req *EnterNewTxnRequest) error
	// OnStmtStart is the hook that should be called when a new statement started
	OnStmtStart(ctx context.Context) error
}

// NewTxn starts a new optimistic and active txn
func NewTxn(ctx context.Context, sctx sessionctx.Context) error {
	return GetTxnManager(sctx).EnterNewTxn(ctx, &EnterNewTxnRequest{
		Type:    EnterNewTxnDefault,
		TxnMode: ast.Pessimistic,
	})
}

// NewTxnInStmt us used when we should commit the old txn and create a new one for some statement like ddl operations.
func NewTxnInStmt(ctx context.Context, sctx sessionctx.Context) error {
	if err := NewTxn(ctx, sctx); err != nil {
		return err
	}
	return GetTxnManager(sctx).OnStmtStart(ctx)
}

// GetTxnManager returns the TxnManager object from session context
var GetTxnManager func(sctx sessionctx.Context) TxnManager
