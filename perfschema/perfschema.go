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
	"sync"

	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/plan"
	"github.com/syndtr/goleveldb/leveldb"
)

// PerfSchema defines the methods to be invoked by the executor
type PerfSchema interface {
	// For SELECT statement only.
	NewPerfSchemaPlan(tableName string) (plan.Plan, error)

	// For INSERT statement only.
	ExecInsert(insertVals *InsertValues) error
}

type perfSchema struct {
	tables  map[string]*model.TableInfo
	columns map[columnName]*model.ColumnInfo
	fields  map[columnName]*field.ResultField

	// Definition order same as MySQL's reference manual, so don't bother to
	// adjust according to alphabetical order. For ease of implementation, we
	// choose to store records separately.
	setupActorsStore            *leveldb.DB
	setupObjectsStore           *leveldb.DB
	setupInstrumentsStore       *leveldb.DB
	setupConsumersStore         *leveldb.DB
	setupTimersStore            *leveldb.DB
	stmtsCurrentStore           *leveldb.DB
	stmtsHistoryStore           *leveldb.DB
	stmtsHistoryLongStore       *leveldb.DB
	preparedStmtsInstancesStore *leveldb.DB
	transCurrentStore           *leveldb.DB
	transHistoryStore           *leveldb.DB
	transHistoryLongStore       *leveldb.DB
	stagesCurrentStore          *leveldb.DB
	stagesHistoryStore          *leveldb.DB
	stagesHistoryLongStore      *leveldb.DB

	setupActorsRecordNo            uint64
	setupObjectsRecordNo           uint64
	setupInstrumentsRecordNo       uint64
	setupConsumersRecordNo         uint64
	setupTimersRecordNo            uint64
	stmtsCurrentRecordNo           uint64
	stmtsHistoryRecordNo           uint64
	stmtsHistoryLongRecordNo       uint64
	preparedStmtsInstancesRecordNo uint64
	transCurrentRecordNo           uint64
	transHistoryRecordNo           uint64
	transHistoryLongRecordNo       uint64
	stagesCurrentRecordNo          uint64
	stagesHistoryRecordNo          uint64
	stagesHistoryLongRecordNo      uint64
}

type columnName struct {
	table string
	name  string
}

var _ PerfSchema = (*perfSchema)(nil)

var (
	pool = sync.Pool{
		New: func() interface{} {
			return &leveldb.Batch{}
		},
	}
)

// PerfHandle is the only access point for the in-memory performance schema information
var PerfHandle PerfSchema

func init() {
	// TODO: need error handling later
	schema := &perfSchema{}
	_ = schema.initialize()

	var v interface{} = schema
	PerfHandle, _ = v.(PerfSchema)
}
