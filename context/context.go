// Copyright 2015 PingCAP, Inc.
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

package context

import (
	"fmt"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
)

// Context is an interface for transaction and executive args environment.
type Context interface {
	// NewTxn creates a new transaction for further execution.
	// If old transaction is valid, it is committed first.
	// It's used in BEGIN statement and DDL statements to commit old transaction.
	NewTxn() error

	// Txn returns the current transaction which is created before executing a statement.
	Txn() kv.Transaction

	// GetClient gets a kv.Client.
	GetClient() kv.Client

	// SetValue saves a value associated with this context for key.
	SetValue(key fmt.Stringer, value interface{})

	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) interface{}

	// ClearValue clears the value associated with this context for key.
	ClearValue(key fmt.Stringer)

	GetSessionVars() *variable.SessionVars

	GetSessionManager() util.SessionManager

	// ActivePendingTxn receives the pending transaction from the transaction channel.
	// It should be called right before we builds an executor.
	ActivePendingTxn() error

	// InitTxnWithStartTS initializes a transaction with startTS.
	// It should be called right before we builds an executor.
	InitTxnWithStartTS(startTS uint64) error

	// Done returns a channel for cancelation, the same as standard context.Context.
	// See https://godoc.org/context for more examples of how to use it.
	Done() <-chan struct{}
}

// CtxForCancel implements the standard Go context.Context interface.
type CtxForCancel struct {
	Context
}

// Value implements the standard Go context.Context interface.
func (ctx CtxForCancel) Value(interface{}) interface{} {
	return nil
}

// Deadline implements the standard Go context.Context interface.
func (ctx CtxForCancel) Deadline() (deadline time.Time, ok bool) {
	return
}

// Err implements the standard Go context.Context interface.
func (ctx CtxForCancel) Err() error {
	return nil
}

type basicCtxType int

func (t basicCtxType) String() string {
	switch t {
	case QueryString:
		return "query_string"
	case Initing:
		return "initing"
	}
	return "unknown"
}

// Context keys.
const (
	// QueryString is the key for original query string.
	QueryString basicCtxType = 1
	// Initing is the key for indicating if the server is running bootstrap or upgrad job.
	Initing basicCtxType = 2
)
