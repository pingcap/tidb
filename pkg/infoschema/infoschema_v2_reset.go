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

package infoschema

import (
	"sync/atomic"

	"github.com/google/btree"
)

// resetBeforeFullLoad is called before a full recreate operation within builder.InitWithDBInfos().
// TODO: write a generics version to avoid repeated code.
func (isd *Data) resetBeforeFullLoad(schemaVersion int64) {
	resetTableInfoResidentBeforeFullLoad(&isd.tableInfoResident, schemaVersion)

	resetByIDBeforeFullLoad(&isd.byID, schemaVersion)
	resetByNameBeforeFullLoad(&isd.byName, schemaVersion)

	resetSchemaMapBeforeFullLoad(&isd.schemaMap, schemaVersion)
	resetSchemaID2NameBeforeFullLoad(&isd.schemaID2Name, schemaVersion)

	resetPID2TIDBeforeFullLoad(&isd.pid2tid, schemaVersion)
	resetFKBeforeFullLoad(&isd.referredForeignKeys, schemaVersion)
}

func resetByIDBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[*tableItem]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}

	batchSize := min(bt.Len(), 1000)
	items := make([]*tableItem, 0, batchSize)
	items = append(items, pivot)
	for {
		bt.DescendLessOrEqual(pivot, func(item *tableItem) bool {
			if pivot.tableID == item.tableID {
				return true // skip MVCC version
			}
			pivot = item
			items = append(items, pivot)
			return len(items) < cap(items)
		})
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			btreeSet(ptr, &tableItem{
				dbName:        item.dbName,
				dbID:          item.dbID,
				tableName:     item.tableName,
				tableID:       item.tableID,
				schemaVersion: schemaVersion,
				tomb:          true,
			})
		}
		items = items[:0]
	}
}

func resetByNameBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[*tableItem]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}

	batchSize := min(bt.Len(), 1000)
	items := make([]*tableItem, 0, batchSize)
	items = append(items, pivot)
	for {
		bt.DescendLessOrEqual(pivot, func(item *tableItem) bool {
			if pivot.dbName == item.dbName && pivot.tableName == item.tableName {
				return true // skip MVCC version
			}
			pivot = item
			items = append(items, pivot)
			return len(items) < cap(items)
		})
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			btreeSet(ptr, &tableItem{
				dbName:        item.dbName,
				dbID:          item.dbID,
				tableName:     item.tableName,
				tableID:       item.tableID,
				schemaVersion: schemaVersion,
				tomb:          true,
			})
		}
		items = items[:0]
	}
}

func resetTableInfoResidentBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[tableInfoItem]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}
	items := make([]tableInfoItem, 0, bt.Len())
	items = append(items, pivot)
	bt.DescendLessOrEqual(pivot, func(item tableInfoItem) bool {
		if pivot.dbName == item.dbName && pivot.tableID == item.tableID {
			return true // skip MVCC version
		}
		pivot = item
		items = append(items, pivot)
		return true
	})
	for _, item := range items {
		btreeSet(ptr, tableInfoItem{
			dbName:        item.dbName,
			tableID:       item.tableID,
			schemaVersion: schemaVersion,
			tomb:          true,
		})
	}
}

func resetSchemaMapBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[schemaItem]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}
	items := make([]schemaItem, 0, bt.Len())
	items = append(items, pivot)
	bt.DescendLessOrEqual(pivot, func(item schemaItem) bool {
		if pivot.Name() == item.Name() {
			return true // skip MVCC version
		}
		pivot = item
		items = append(items, pivot)
		return true
	})
	for _, item := range items {
		btreeSet(ptr, schemaItem{
			dbInfo:        item.dbInfo,
			schemaVersion: schemaVersion,
			tomb:          true,
		})
	}
}

func resetSchemaID2NameBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[schemaIDName]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}
	items := make([]schemaIDName, 0, bt.Len())
	items = append(items, pivot)
	bt.DescendLessOrEqual(pivot, func(item schemaIDName) bool {
		if pivot.id == item.id {
			return true // skip MVCC version
		}
		pivot = item
		items = append(items, pivot)
		return true
	})
	for _, item := range items {
		btreeSet(ptr, schemaIDName{
			id:            item.id,
			name:          item.name,
			schemaVersion: schemaVersion,
			tomb:          true,
		})
	}
}

func resetPID2TIDBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[partitionItem]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}

	batchSize := min(bt.Len(), 1000)
	items := make([]partitionItem, 0, batchSize)
	items = append(items, pivot)
	for {
		bt.DescendLessOrEqual(pivot, func(item partitionItem) bool {
			if pivot.partitionID == item.partitionID {
				return true // skip MVCC version
			}
			pivot = item
			items = append(items, pivot)
			return len(items) < cap(items)
		})
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			btreeSet(ptr, partitionItem{
				partitionID:   item.partitionID,
				tableID:       item.tableID,
				schemaVersion: schemaVersion,
				tomb:          true,
			})
		}
		items = items[:0]
	}
}

func resetFKBeforeFullLoad(ptr *atomic.Pointer[btree.BTreeG[*referredForeignKeyItem]], schemaVersion int64) {
	bt := ptr.Load()
	pivot, ok := bt.Max()
	if !ok {
		return
	}
	items := make([]*referredForeignKeyItem, 0, bt.Len())
	items = append(items, pivot)
	bt.DescendLessOrEqual(pivot, func(item *referredForeignKeyItem) bool {
		if pivot.dbName == item.dbName && pivot.tableName == item.tableName {
			return true
		}
		pivot = item
		items = append(items, pivot)
		return true
	})
	for _, item := range items {
		btreeSet(ptr, &referredForeignKeyItem{
			dbName:         item.dbName,
			tableName:      item.tableName,
			schemaVersion:  schemaVersion,
			tomb:           true,
			referredFKInfo: nil,
		})
	}
}
