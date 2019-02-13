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
	bindRecord
	ast ast.StmtNode
}

// bindCache hold a bindDataMap, key:origin sql hash value: bindData slice.
type bindCache struct {
	Cache map[string][]*bindData
}

// Handle hold a atomic bindCache.
type Handle struct {
	bind atomic.Value
}

// HandleUpdater use to update the bindCache.
type HandleUpdater struct {
	parser         *parser.Parser
	lastUpdateTime types.Time
	ctx            sessionctx.Context
	globalHandle   *Handle
}

type bindRecord struct {
	OriginalSQL string
	BindSQL     string
	Db          string
	Status      int64
	CreateTime  types.Time
	UpdateTime  types.Time
	Charset     string
	Collation   string
}

// NewHandleUpdater create a new HandleUpdater.
func NewHandleUpdater(handle *Handle, parser *parser.Parser, ctx sessionctx.Context) *HandleUpdater {
	return &HandleUpdater{
		globalHandle: handle,
		parser:       parser,
		ctx:          ctx,
	}
}

// NewHandle create a Handle with a bindCache.
func NewHandle() *Handle {
	handle := &Handle{}
	return handle
}

// Get get bindCache from a Handle.
func (h *Handle) Get() *bindCache {
	bc := h.bind.Load()

	if bc != nil {
		return bc.(*bindCache)
	}

	bc = &bindCache{
		Cache: make(map[string][]*bindData, defaultBindCacheSize),
	}

	return bc.(*bindCache)
}

// LoadDiff use to load new bind info to bindCache bc.
func (h *HandleUpdater) loadDiff(sql string, bc *bindCache) error {
	tmp, err := h.ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	if err != nil {
		return errors.Trace(err)
	}

	rs := tmp[0]
	defer terror.Call(rs.Close)

	fs := rs.Fields()
	chk := rs.NewChunk()
	for {
		err = rs.Next(context.TODO(), chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			return nil
		}
		it := chunk.NewIterator4Chunk(chk)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			record := decodeBindTableRow(row, fs)
			err = bc.appendNode(h.ctx, record, h.parser)
			if err != nil {
				continue
			}

			if record.UpdateTime.Compare(h.lastUpdateTime) == 1 {
				h.lastUpdateTime = record.UpdateTime
			}
		}
		chk = chunk.Renew(chk, h.ctx.GetSessionVars().MaxChunkSize)
	}
}

// Update update the HandleUpdater's bindCache if tidb first startup,the fullLoad is true,otherwise fullLoad is false.
func (h *HandleUpdater) Update(fullLoad bool) error {
	var (
		err error
		sql string
	)
	bc := h.globalHandle.Get()

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

	h.globalHandle.bind.Store(newBc)
	return nil
}

func parseSQL(parser *parser.Parser, sql string, charset string, collation string) ([]ast.StmtNode, []error, error) {
	return parser.Parse(sql, charset, collation)
}

func decodeBindTableRow(row chunk.Row, fs []*ast.ResultField) bindRecord {
	var value bindRecord

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

func (b *bindCache) appendNode(sctx sessionctx.Context, newBindRecord bindRecord, sparser *parser.Parser) error {
	hash := parser.Normalize(newBindRecord.OriginalSQL)

	if bindArr, ok := b.Cache[hash]; ok {
		for idx, v := range bindArr {
			if v.OriginalSQL == newBindRecord.OriginalSQL && v.Db == newBindRecord.Db {
				b.Cache[hash] = append(b.Cache[hash][:idx], b.Cache[hash][idx+1:]...)
				break
			}
		}
	}

	// If the bindRecord has been deleted, the status will be 0 or will be 1.
	if newBindRecord.Status == 0 {
		if _, ok := b.Cache[hash]; ok {
			if len(b.Cache[hash]) == 0 {
				delete(b.Cache, hash)
			}
		}

		return nil
	}

	stmtNodes, _, err := parseSQL(sparser, newBindRecord.BindSQL, newBindRecord.Charset, newBindRecord.Collation)
	if err != nil {
		log.Warnf("parse error:\n%v\n%s", err, newBindRecord.BindSQL)
		return errors.Trace(err)
	}

	newNode := &bindData{
		bindRecord: newBindRecord,
		ast:        stmtNodes[0],
	}

	b.Cache[hash] = append(b.Cache[hash], newNode)
	return nil
}
