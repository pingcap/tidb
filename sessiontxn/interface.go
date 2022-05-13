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

// EnterNewTxnType is the type to enter a new txn
type EnterNewTxnType int

const (
	// EnterNewTxnDefault means to enter a new txn. Its behavior is more straight-forward
	// just starting a new txn right now without any scenario assumptions.
	EnterNewTxnDefault EnterNewTxnType = iota
	// EnterNewTxnWithBeginStmt indicates to enter a new txn when execute 'BEGIN' or 'START TRANSACTION'
	EnterNewTxnWithBeginStmt
	// EnterNewTxnBeforeStmt indicates to enter a new txn before each statement when the txn is not present
	// If `EnterNewTxnBeforeStmt` is used, the new txn will always act as the 'lazy' mode. That means the inner transaction
	// is only activated when needed to reduce unnecessary overhead.
	EnterNewTxnBeforeStmt
	// EnterNewTxnWithReplaceProvider indicates to replace the current provider. Now only stale read are using this
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

// StmtErrorHandlePoint is where the error is being handled
type StmtErrorHandlePoint int

const (
	// StmtErrAfterQuery means we are handling an error after the query failed
	StmtErrAfterQuery StmtErrorHandlePoint = iota
	// StmtErrAfterPessimisticLock means we are handling an error after pessimistic lock failed.
	StmtErrAfterPessimisticLock
)

// StmtErrorAction is the next action advice when an error occurs when executing a statement
type StmtErrorAction int

const (
	// StmtActionError means the error should be returned directly without any retry
	StmtActionError StmtErrorAction = iota
	// StmtActionRetryReady means the error is caused by this component, and it is ready for retry.
	StmtActionRetryReady
	// StmtActionNoIdea means the error is not caused by this component, and whether retry or not should be determined by other components.
	// If the user do not know whether to retry or not, it is advised to return the original error.
	StmtActionNoIdea
)

// ErrorAction returns StmtActionError with specified error
func ErrorAction(err error) (StmtErrorAction, error) {
	return StmtActionError, err
}

// RetryReady returns StmtActionRetryReady, nil
func RetryReady() (StmtErrorAction, error) {
	return StmtActionRetryReady, nil
}

// NoIdea returns StmtActionNoIdea, nil
func NoIdea() (StmtErrorAction, error) {
	return StmtActionNoIdea, nil
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
	// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
	OnStmtErrorForNextAction(point StmtErrorHandlePoint, err error) (StmtErrorAction, error)
	// OnStmtRetry is the hook that should be called when a statement is retried internally.
	OnStmtRetry(ctx context.Context) error
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
	// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
	// This method is not required to be called for every error in the statement,
	// it is only required to be called for some errors handled in some specified points given by the parameter `point`.
	// When the return error is not nil the return action is 'StmtActionError' and vice versa.
	OnStmtErrorForNextAction(point StmtErrorHandlePoint, err error) (StmtErrorAction, error)
	// OnStmtRetry is the hook that should be called when a statement retry
	OnStmtRetry(ctx context.Context) error
}

// NewTxn starts a new optimistic and active txn, it can be used for the below scenes:
// 1. Commit the current transaction and do some work in a new transaction for some specific operations, for example: DDL
// 2. Some background job need to do something in a transaction.
// In other scenes like 'BEGIN', 'START TRANSACTION' or prepare transaction in a new statement,
// you should use `TxnManager`.`EnterNewTxn` and pass the relevant to it.
func NewTxn(ctx context.Context, sctx sessionctx.Context) error {
	return GetTxnManager(sctx).EnterNewTxn(ctx, &EnterNewTxnRequest{
		Type:    EnterNewTxnDefault,
		TxnMode: ast.Optimistic,
	})
}

// NewTxnInStmt is like `NewTxn` but it will call `OnStmtStart` after it.
// It should be used when a statement already started.
func NewTxnInStmt(ctx context.Context, sctx sessionctx.Context) error {
	if err := NewTxn(ctx, sctx); err != nil {
		return err
	}
	return GetTxnManager(sctx).OnStmtStart(ctx)
}

// GetTxnManager returns the TxnManager object from session context
var GetTxnManager func(sctx sessionctx.Context) TxnManager
