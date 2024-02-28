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

package briefapi

import (
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/tableutil"
)

// Type is used to distinguish between different tables that store data in different ways.
type Type int16

const (
	// NormalTable stores data in tikv, mocktikv and so on.
	NormalTable Type = iota
	// VirtualTable stores no data, just extract data from the memory struct.
	VirtualTable
	// ClusterTable contains the `VirtualTable` in the all cluster tidb nodes.
	ClusterTable
)

// IsNormalTable checks whether the table is a normal table type.
func (tp Type) IsNormalTable() bool {
	return tp == NormalTable
}

// IsVirtualTable checks whether the table is a virtual table type.
func (tp Type) IsVirtualTable() bool {
	return tp == VirtualTable
}

// IsClusterTable checks whether the table is a cluster table type.
func (tp Type) IsClusterTable() bool {
	return tp == ClusterTable
}

// AllocatorContext is used to provide context for method `table.Allocators`.
type AllocatorContext interface {
	// TxnRecordTempTable record the temporary table to the current transaction.
	// This method will be called when the temporary table is modified or should allocate id in the transaction.
	TxnRecordTempTable(tbl *model.TableInfo) tableutil.TempTable
}

type Table interface {
	columnAPI
	// Indices returns the indices of the table.
	// The caller must be aware of that not all the returned indices are public.
	Indices() []Index
	//RecordPrefix returns the record key prefix.
	RecordPrefix() kv.Key
	// IndexPrefix returns the index key prefix.
	IndexPrefix() kv.Key
	// Meta returns TableInfo.
	Meta() *model.TableInfo
	// Type returns the type of table
	Type() Type
	// GetPartitionedTable returns nil if not partitioned
	GetPartitionedTable() PartitionedTable
	// Allocators returns all allocators.
	Allocators(ctx AllocatorContext) autoid.Allocators
}

// PhysicalTable is an abstraction for two kinds of table representation: partition or non-partitioned table.
// PhysicalID is a ID that can be used to construct a key ranges, all the data in the key range belongs to the corresponding PhysicalTable.
// For a non-partitioned table, its PhysicalID equals to its TableID; For a partition of a partitioned table, its PhysicalID is the partition's ID.
type PhysicalTable interface {
	Table
	GetPhysicalID() int64
}

// PartitionedTable is a Table, and it has a GetPartition() method.
// GetPartition() gets the partition from a partition table by a physical table ID,
type PartitionedTable interface {
	Table
	GetPartition(physicalID int64) PhysicalTable
	GetAllPartitionIDs() []int64
	GetPartitionColumnIDs() []int64
	GetPartitionColumnNames() []model.CIStr
}
