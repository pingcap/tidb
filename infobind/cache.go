// Copyright 2019 PingCAP, Inc.
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

package infobind

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
)

const defaultBindCacheSize = 5

// bindRecord store the basic bind info and bindSql astNode.
type bindRecord struct {
	bindMeta
	ast ast.StmtNode
}

// bindCache holds a bindDataMap
//   key: origin sql
//   value: bindRecord slice.
type bindCache map[string][]*bindRecord

// Handler hold an atomic bindCache.
type Handler struct {
	bind atomic.Value
}

// BindCacheUpdater use to update the bindCache.
// BindCacheUpdater is used to update the global bindCache.
// BindCacheUpdater will update the bind cache pre 3 second in domain gorountine loop. When the tidb server first startup, the updater will load all bind info in memory; then load diff bind info pre 3 second.
type BindCacheUpdater struct {
	ctx sessionctx.Context

	parser         *parser.Parser
	lastUpdateTime types.Time
	globalHandler  *Handler
}

type bindMeta struct {
	OriginalSQL string
	BindSQL     string
	Db          string
	// Status will only be 0 or 1:
	//   0: bindMeta has been marked as deleted status.
	//   1: bindMeta is in using status.
	Status     int64
	CreateTime types.Time
	UpdateTime types.Time
	Charset    string
	Collation  string
}

// NewBindCacheUpdater create a new BindCacheUpdater.
func NewBindCacheUpdater(ctx sessionctx.Context, handler *Handler, parser *parser.Parser) *BindCacheUpdater {
	return &BindCacheUpdater{
		globalHandler: handler,
		parser:        parser,
		ctx:           ctx,
	}
}

// NewHandler create a Handler with a bindCache.
func NewHandler() *Handler {
	handler := &Handler{}
	return handler
}

// Get get bindCache from a Handler.
func (h *Handler) Get() bindCache {
	bc := h.bind.Load()

	if bc != nil {
		return bc.(map[string][]*bindRecord)
	}

	return make(map[string][]*bindRecord, defaultBindCacheSize)
}

// LoadDiff use to load new bind info to bindCache bc.
func (h *BindCacheUpdater) loadDiff(sql string, bc bindCache) error {
	recordSets, err := h.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	if err != nil {
		return errors.Trace(err)
	}

	rs := recordSets[0]
	defer terror.Call(rs.Close)

	chkBatch := rs.NewRecordBatch()
	for {
		err = rs.Next(context.TODO(), chkBatch)
		if err != nil {
			return errors.Trace(err)
		}

		if chkBatch.NumRows() == 0 {
			return nil
		}
		it := chunk.NewIterator4Chunk(chkBatch.Chunk)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			record := decodeBindTableRow(row)
			err = bc.appendNode(record, h.parser)
			if err != nil {
				return err
			}

			if record.UpdateTime.Compare(h.lastUpdateTime) == 1 {
				h.lastUpdateTime = record.UpdateTime
			}
		}
	}
}

// Update update the BindCacheUpdater's bindCache if tidb first startup,the fullLoad is true,otherwise fullLoad is false.
func (h *BindCacheUpdater) Update(fullLoad bool) (err error) {
	var sql string

	bc := h.globalHandler.Get()

	newBc := make(map[string][]*bindRecord, len(bc))

	for hash, bindDataArr := range bc {
		newBc[hash] = append(newBc[hash], bindDataArr...)
	}

	if fullLoad {
		sql = fmt.Sprintf("select original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation from mysql.bind_info")
	} else {
		sql = fmt.Sprintf("select original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation from mysql.bind_info where update_time > \"%s\"", h.lastUpdateTime.String())
	}
	err = h.loadDiff(sql, newBc)
	if err != nil {
		return errors.Trace(err)
	}

	h.globalHandler.bind.Store(newBc)

	return nil
}

func decodeBindTableRow(row chunk.Row) bindMeta {
	var value bindMeta

	value.OriginalSQL = row.GetString(0)
	value.BindSQL = row.GetString(1)
	value.Db = row.GetString(2)
	value.Status = row.GetInt64(3)
	value.CreateTime = row.GetTime(4)
	value.UpdateTime = row.GetTime(5)
	value.Charset = row.GetString(6)
	value.Collation = row.GetString(7)

	return value
}

func (b bindCache) appendNode(newBindRecord bindMeta, sparser *parser.Parser) error {
	hash := parser.DigestHash(newBindRecord.OriginalSQL)

	if bindArr, ok := b[hash]; ok {
		for idx, v := range bindArr {
			if v.OriginalSQL == newBindRecord.OriginalSQL && v.Db == newBindRecord.Db {
				b[hash] = append(b[hash][:idx], b[hash][idx+1:]...)
				if len(b[hash]) == 0 {
					delete(b, hash)
				}
				break
			}
		}
	}

	if newBindRecord.Status == 0 {
		return nil
	}

	stmtNodes, _, err := sparser.Parse(newBindRecord.BindSQL, newBindRecord.Charset, newBindRecord.Collation)
	if err != nil {
		log.Warnf("parse error:\n%v\n%s", err, newBindRecord.BindSQL)
		return errors.Trace(err)
	}

	newNode := &bindRecord{
		bindMeta: newBindRecord,
		ast:      stmtNodes[0],
	}

	b[hash] = append(b[hash], newNode)
	return nil
}
