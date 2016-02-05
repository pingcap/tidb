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
}

type perfSchema struct {
	tables map[string]*model.TableInfo
	fields map[string][]*field.ResultField
	stores map[string]*leveldb.DB
	lsns   map[string]*uint64
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

	PerfHandle = schema
}
