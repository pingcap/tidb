// Copyright 2024 PingCAP, Inc.
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

package context

import (
	"fmt"

	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/tableutil"
	"github.com/pingcap/tipb/go-binlog"
)

var _ AllocatorContext = MutateContext(nil)

// MutateContext is used to when mutating a table.
type MutateContext interface {
	AllocatorContext
	// GetExprCtx returns the context to build or evaluate expressions
	GetExprCtx() exprctx.ExprContext
	// Value returns the value associated with this context for key.
	Value(key fmt.Stringer) any
	// GetSessionVars returns the session variables.
	GetSessionVars() *variable.SessionVars
	// Txn returns the current transaction which is created before executing a statement.
	// The returned kv.Transaction is not nil, but it maybe pending or invalid.
	// If the active parameter is true, call this function will wait for the pending txn
	// to become valid.
	Txn(active bool) (kv.Transaction, error)
	// StmtGetMutation gets the binlog mutation for current statement.
	StmtGetMutation(int64) *binlog.TableMutation
	// GetDomainInfoSchema returns the latest information schema in domain
	GetDomainInfoSchema() infoschema.MetaOnlyInfoSchema
	// TxnRecordTempTable record the temporary table to the current transaction.
	// This method will be called when the temporary table is modified or should allocate id in the transaction.
	TxnRecordTempTable(tbl *model.TableInfo) tableutil.TempTable
}

// AllocatorContext is used to provide context for method `table.Allocators`.
type AllocatorContext interface {
	// TxnRecordTempTable record the temporary table to the current transaction.
	// This method will be called when the temporary table is modified or should allocate id in the transaction.
	TxnRecordTempTable(tbl *model.TableInfo) tableutil.TempTable
}
