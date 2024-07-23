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
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tidb/pkg/util/tableutil"
	"github.com/pingcap/tipb/go-binlog"
)

var _ AllocatorContext = MutateContext(nil)

// RowEncodingConfig is used to provide config for row encoding.
type RowEncodingConfig struct {
	// IsRowLevelChecksumEnabled indicates whether the row level checksum is enabled.
	IsRowLevelChecksumEnabled bool
	// RowEncoder is used to encode a row
	RowEncoder *rowcodec.Encoder
}

// BinlogSupport is used for binlog operations
type BinlogSupport interface {
	// GetBinlogMutation returns a `binlog.TableMutation` object for a table.
	GetBinlogMutation(tblID int64) *binlog.TableMutation
}

// StatisticsSupport is used for statistics update operations.
type StatisticsSupport interface {
	// UpdatePhysicalTableDelta updates the physical table delta.
	UpdatePhysicalTableDelta(physicalTableID int64, delta int64, count int64, cols variable.DeltaCols)
}

// MutateContext is used to when mutating a table.
type MutateContext interface {
	AllocatorContext
	// GetExprCtx returns the context to build or evaluate expressions
	GetExprCtx() exprctx.ExprContext
	// GetSessionVars returns the session variables.
	GetSessionVars() *variable.SessionVars
	// Txn returns the current transaction which is created before executing a statement.
	// The returned kv.Transaction is not nil, but it maybe pending or invalid.
	// If the active parameter is true, call this function will wait for the pending txn
	// to become valid.
	Txn(active bool) (kv.Transaction, error)
	// GetDomainInfoSchema returns the latest information schema in domain
	GetDomainInfoSchema() infoschema.MetaOnlyInfoSchema
	// TxnRecordTempTable record the temporary table to the current transaction.
	// This method will be called when the temporary table is modified or should allocate id in the transaction.
	TxnRecordTempTable(tbl *model.TableInfo) tableutil.TempTable
	// ConnectionID returns the id of the current connection.
	// If the current environment is not in a query from the client, the return value is 0.
	ConnectionID() uint64
	// InRestrictedSQL returns whether the current context is used in restricted SQL.
	InRestrictedSQL() bool
	// TxnAssertionLevel returns the assertion level of the current transaction.
	TxnAssertionLevel() variable.AssertionLevel
	// EnableMutationChecker returns whether to check data consistency for mutations.
	EnableMutationChecker() bool
	// GetRowEncodingConfig returns the RowEncodingConfig.
	GetRowEncodingConfig() RowEncodingConfig
	// GetMutateBuffers returns the MutateBuffers,
	// which is a buffer for table related structures that aims to reuse memory and
	// saves allocation.
	GetMutateBuffers() *MutateBuffers
	// GetBinlogSupport returns a `BinlogSupport` if the context supports it.
	// If the context does not support binlog, the second return value will be false.
	GetBinlogSupport() (BinlogSupport, bool)
	// GetStatisticsSupport returns a `StatisticsSupport` if the context supports it.
	// If the context does not support statistics update, the second return value will be false.
	GetStatisticsSupport() (StatisticsSupport, bool)
}

// AllocatorContext is used to provide context for method `table.Allocators`.
type AllocatorContext interface {
	// TxnRecordTempTable record the temporary table to the current transaction.
	// This method will be called when the temporary table is modified or should allocate id in the transaction.
	TxnRecordTempTable(tbl *model.TableInfo) tableutil.TempTable
}
