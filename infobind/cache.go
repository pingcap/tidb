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

// bindData store the basic bind info and bindSql astNode.
type bindData struct {
	bindMeta
	ast ast.StmtNode
}

// bindCache holds a bindDataMap, key:origin sql hash value: bindData slice.
type bindCache struct {
	Cache map[string][]*bindData
}

// Handler hold a atomic bindCache.
type Handler struct {
	bind atomic.Value
}

// HandleUpdater use to update the bindCache.
type HandleUpdater struct {
	parser         *parser.Parser
	lastUpdateTime types.Time
	ctx            sessionctx.Context
	globalHandler  *Handler
}

type bindMeta struct {
	OriginalSQL string
	BindSQL     string
	Db          string
	Status      int64 // If the bindMeta has been deleted, the status will be 0 or will be 1.
	CreateTime  types.Time
	UpdateTime  types.Time
	Charset     string
	Collation   string
}

// NewHandleUpdater create a new HandleUpdater.
func NewHandleUpdater(ctx sessionctx.Context, handler *Handler, parser *parser.Parser) *HandleUpdater {
	return &HandleUpdater{
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
func (h *Handler) Get() *bindCache {
	bc := h.bind.Load()

	if bc != nil {
		return bc.(*bindCache)
	}

	return &bindCache{
		Cache: make(map[string][]*bindData, defaultBindCacheSize),
	}
}

// LoadDiff use to load new bind info to bindCache bc.
func (h *HandleUpdater) loadDiff(sql string, bc *bindCache) error {
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

// Update update the HandleUpdater's bindCache if tidb first startup,the fullLoad is true,otherwise fullLoad is false.
func (h *HandleUpdater) Update(fullLoad bool) (err error) {
	var sql string

	bc := h.globalHandler.Get()

	newBc := &bindCache{
		Cache: make(map[string][]*bindData, len(bc.Cache)),
	}

	for hash, bindDataArr := range bc.Cache {
		newBc.Cache[hash] = append(newBc.Cache[hash], bindDataArr...)
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

func (b *bindCache) appendNode(newBindRecord bindMeta, sparser *parser.Parser) error {
	hash := parser.DigestHash(newBindRecord.OriginalSQL)

	if bindArr, ok := b.Cache[hash]; ok {
		for idx, v := range bindArr {
			if v.OriginalSQL == newBindRecord.OriginalSQL && v.Db == newBindRecord.Db {
				b.Cache[hash] = append(b.Cache[hash][:idx], b.Cache[hash][idx+1:]...)
				if len(b.Cache[hash]) == 0 {
					delete(b.Cache, hash)
				}
				break
			}
		}
	}

	// If the bindMeta has been deleted, the status will be 0 or will be 1.
	if newBindRecord.Status == 0 {
		return nil
	}

	stmtNodes, _, err := sparser.Parse(newBindRecord.BindSQL, newBindRecord.Charset, newBindRecord.Collation)
	if err != nil {
		log.Warnf("parse error:\n%v\n%s", err, newBindRecord.BindSQL)
		return errors.Trace(err)
	}

	newNode := &bindData{
		bindMeta: newBindRecord,
		ast:      stmtNodes[0],
	}

	b.Cache[hash] = append(b.Cache[hash], newNode)
	return nil
}
