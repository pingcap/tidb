// Copyright 2023 PingCAP, Inc.
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

package usage

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
)

// IndexUsageInformation is the data struct to store index usage information.
type IndexUsageInformation struct {
	LastUsedAt   string
	QueryCount   int64
	RowsSelected int64
}

// GlobalIndexID is the key type for indexUsageMap.
type GlobalIndexID struct {
	TableID int64
	IndexID int64
}

type indexUsageMap map[GlobalIndexID]IndexUsageInformation

// SessionIndexUsageCollector is a list item that holds the index usage mapper. If you want to write or read mapper, you must lock it.
// TODO: use a third-party thread-safe list implementation instead of maintaining the list manually.
/*
                            [session1]                [session2]                        [sessionN]
                                |                         |                                 |
                            update into              update into                       update into
                                |                         |                                 |
                                v                         v                                 v
[StatsHandle.Head] --> [session1.IndexUsage] --> [session2.IndexUsage] --> ... --> [sessionN.IndexUsage]
                                |                         |                                 |
                                +-------------------------+---------------------------------+
                                                          |
                                        collect and dump into storage periodically
                                                          |
                                                          v
                                                      [storage]
*/
type SessionIndexUsageCollector struct {
	mapper indexUsageMap
	next   *SessionIndexUsageCollector
	sync.Mutex

	deleted bool
}

// NewSessionIndexUsageCollector creates a new SessionIndexUsageCollector.
// If listHead is not nil, add this element to the list.
func NewSessionIndexUsageCollector(listHead *SessionIndexUsageCollector) *SessionIndexUsageCollector {
	if listHead == nil {
		return &SessionIndexUsageCollector{mapper: make(indexUsageMap)}
	}
	listHead.Lock()
	defer listHead.Unlock()
	newCollector := &SessionIndexUsageCollector{
		mapper: make(indexUsageMap),
		next:   listHead.next,
	}
	listHead.next = newCollector
	return newCollector
}

func (m indexUsageMap) updateByKey(id GlobalIndexID, value *IndexUsageInformation) {
	item := m[id]
	item.QueryCount += value.QueryCount
	item.RowsSelected += value.RowsSelected
	if item.LastUsedAt < value.LastUsedAt {
		item.LastUsedAt = value.LastUsedAt
	}
	m[id] = item
}

func (m indexUsageMap) update(tableID int64, indexID int64, value *IndexUsageInformation) {
	id := GlobalIndexID{TableID: tableID, IndexID: indexID}
	m.updateByKey(id, value)
}

func (m indexUsageMap) merge(destMap indexUsageMap) {
	for id := range destMap {
		item := destMap[id]
		m.updateByKey(id, &item)
	}
}

// Update updates the mapper in SessionIndexUsageCollector.
func (s *SessionIndexUsageCollector) Update(tableID int64, indexID int64, value *IndexUsageInformation) {
	value.LastUsedAt = time.Now().Format(types.TimeFSPFormat)
	s.Lock()
	defer s.Unlock()
	s.mapper.update(tableID, indexID, value)
}

// Delete will set s.deleted to true which means it can be deleted from linked list.
func (s *SessionIndexUsageCollector) Delete() {
	s.Lock()
	defer s.Unlock()
	s.deleted = true
}

// sweepIdxUsageList will loop over the list, merge each session's local index usage information into handle
// and remove closed session's collector.
// For convenience, we keep idxUsageListHead always points to sentinel node. So that we don't need to consider corner case.
func sweepIdxUsageList(listHead *SessionIndexUsageCollector) indexUsageMap {
	prev := listHead
	prev.Lock()
	mapper := make(indexUsageMap)
	for curr := prev.next; curr != nil; curr = curr.next {
		curr.Lock()
		mapper.merge(curr.mapper)
		if curr.deleted {
			prev.next = curr.next
			curr.Unlock()
		} else {
			prev.Unlock()
			curr.mapper = make(indexUsageMap)
			prev = curr
		}
	}
	prev.Unlock()
	return mapper
}

// batchInsertSize is the batch size used by internal SQL to insert values to some system table.
const batchInsertSize = 10

var (
	// useCurrentSession to make sure the sql is executed in current session.
	useCurrentSession = []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}
)

// DumpIndexUsageToKV will dump in-memory index usage information to KV.
func DumpIndexUsageToKV(sctx sessionctx.Context, listHead *SessionIndexUsageCollector) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	mapper := sweepIdxUsageList(listHead)
	type FullIndexUsageInformation struct {
		information IndexUsageInformation
		id          GlobalIndexID
	}
	indexInformationSlice := make([]FullIndexUsageInformation, 0, len(mapper))
	for id, value := range mapper {
		indexInformationSlice = append(indexInformationSlice, FullIndexUsageInformation{id: id, information: value})
	}
	for i := 0; i < len(mapper); i += batchInsertSize {
		end := i + batchInsertSize
		if end > len(mapper) {
			end = len(mapper)
		}
		sql := new(strings.Builder)
		sqlexec.MustFormatSQL(sql, "insert into mysql.SCHEMA_INDEX_USAGE (table_id,index_id,query_count,rows_selected,last_used_at) values")
		for j := i; j < end; j++ {
			index := indexInformationSlice[j]
			sqlexec.MustFormatSQL(sql, "(%?, %?, %?, %?, %?)", index.id.TableID, index.id.IndexID,
				index.information.QueryCount, index.information.RowsSelected, index.information.LastUsedAt)
			if j < end-1 {
				sqlexec.MustFormatSQL(sql, ",")
			}
		}
		sqlexec.MustFormatSQL(sql, "on duplicate key update query_count=query_count+values(query_count),rows_selected=rows_selected+values(rows_selected),last_used_at=greatest(last_used_at, values(last_used_at))")
		if _, _, err := exec.ExecRestrictedSQL(ctx, useCurrentSession, sql.String()); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// GCIndexUsageOnKV will delete the usage information of non-existent indexes.
func GCIndexUsageOnKV(sctx sessionctx.Context) error {
	// For performance and implementation reasons, mysql.schema_index_usage doesn't handle DDL.
	// We periodically delete the usage information of non-existent indexes through information_schema.tidb_indexes.
	// This sql will delete the usage information of those indexes that not in information_schema.tidb_indexes.
	sql := `delete from mysql.SCHEMA_INDEX_USAGE as stats where stats.index_id not in (select idx.index_id from information_schema.tidb_indexes as idx)`
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := sctx.(sqlexec.RestrictedSQLExecutor)
	_, _, err := exec.ExecRestrictedSQL(ctx, useCurrentSession, sql)
	return err
}
