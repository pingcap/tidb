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

package tableutil

import (
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
)

// TempTable is used to store transaction-specific or session-specific information for global / local temporary tables.
// For example, stats and autoID should have their own copies of data, instead of being shared by all sessions.
type TempTable interface {
	// GetAutoIDAllocator gets the autoID allocator of this table.
	GetAutoIDAllocator() autoid.Allocator

	// SetModified sets that the table is modified.
	SetModified(bool)

	// GetModified queries whether the table is modified.
	GetModified() bool

	// The stats of this table (*statistics.Table).
	// Define the return type as interface{} here to avoid cycle imports.
	GetStats() interface{}

	GetSize() int64
	SetSize(int64)

	GetMeta() *model.TableInfo
}

// TempTableFromMeta builds a TempTable from *model.TableInfo.
// Currently, it is assigned to tables.TempTableFromMeta in tidb package's init function.
var TempTableFromMeta func(tblInfo *model.TableInfo) TempTable
