// Copyright 2016 PingCAP, Inc.
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

package perfschema

import (
	"reflect"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/terror"
)

var (
	errInvalidPerfSchemaTable = terror.ClassPerfSchema.New(codeInvalidPerfSchemaTable, "invalid perfschema table")
	errInvalidTimerFlag       = terror.ClassPerfSchema.New(codeInvalidTimerFlag, "invalid timer flag")
)

// StatementInstrument defines the methods for statement instrumentation points
type StatementInstrument interface {
	RegisterStatement(category, name string, elem interface{})

	StartStatement(sql string, connID uint64, callerName EnumCallerName, elem interface{}) *StatementState

	EndStatement(state *StatementState)
}

// PerfSchema defines the methods to be invoked by the executor
type PerfSchema interface {

	// For statement instrumentation only.
	StatementInstrument

	// GetDBMeta returns db info for PerformanceSchema.
	GetDBMeta() *model.DBInfo
	// GetTable returns table instance for name.
	GetTable(name string) (table.Table, bool)
}

type perfSchema struct {
	store       kv.Storage
	dbInfo      *model.DBInfo
	tables      map[string]*model.TableInfo
	mTables     map[string]table.Table // Memory tables for perfSchema
	stmtHandles []int64
	stmtInfos   map[reflect.Type]*statementInfo
}

var _ PerfSchema = (*perfSchema)(nil)

// NewPerfHandle creates a new perfSchema on store.
func NewPerfHandle() (PerfSchema, error) {
	schema := &perfSchema{}
	err := schema.initialize()
	if err != nil {
		return nil, errors.Trace(err)
	}
	schema.registerStatements()
	return schema, nil
}

// perfschema error codes.
const (
	codeInvalidPerfSchemaTable terror.ErrCode = 1
	codeInvalidTimerFlag                      = 2
)
