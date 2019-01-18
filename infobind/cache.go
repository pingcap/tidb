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

const defaultBindCacheSize  = 5

// BindData store the basic bind info and bindSql astNode.
type BindData struct {
	bindRecord
	ast ast.StmtNode
}

// BindCache hold a bindDataMap, key:origin sql hash value: bindData slice.
type BindCache struct {
	Cache map[string][]*BindData
}

// Handle hold a atomic bindCache.
type Handle struct {
	bind atomic.Value
}

// HandleUpdater use to update the BindCache.
type HandleUpdater struct {
	Parser         *parser.Parser
	LastUpdateTime types.Time
	Ctx            sessionctx.Context
	*Handle
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

// NewHandle create a Handle with a BindCache.
func NewHandle() *Handle {
	handle := &Handle{}
	return handle
}

// Get get bindCache from a Handle.
func (h *Handle) Get() *BindCache {
	bc := h.bind.Load()

	if bc != nil {
		return bc.(*BindCache)
	}

	return nil
}

// LoadDiff use to load new bind info to bindCache bc.
func (h *HandleUpdater) loadDiff(sql string, bc *BindCache) error {
	tmp, err := h.Ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
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
			record, err := decodeBindTableRow(row, fs)
			if err != nil {
				log.Errorf("row decode error %s", err)
				continue
			}
			err = bc.appendNode(h.Ctx, record, h.Parser)
			if err != nil {
				continue
			}

			if record.UpdateTime.Compare(h.LastUpdateTime) == 1 {
				h.LastUpdateTime = record.UpdateTime
			}
		}
		chk = chunk.Renew(chk, h.Ctx.GetSessionVars().MaxChunkSize)
	}
}

// Update update the HandleUpdater's bindCache if tidb first startup,the fullLoad is true,otherwise fullLoad is false.
func (h *HandleUpdater) Update(fullLoad bool) error {
	var (
		err error
		sql string
	)
	bc := h.Get()

	length := defaultBindCacheSize
	if bc != nil {
		length = len(bc.Cache)
	}

	newBc := &BindCache{
		Cache: make(map[string][]*BindData, length),
	}

	if bc != nil {
		for hash, bindDataArr := range bc.Cache {
			newBc.Cache[hash] = append(newBc.Cache[hash], bindDataArr...)
		}
	}

	if fullLoad {
		sql = fmt.Sprintf("select * from mysql.bind_info")
	} else {
		sql = fmt.Sprintf("select * from mysql.bind_info where update_time > \"%s\"", h.LastUpdateTime.String())
	}
	err = h.loadDiff(sql, newBc)
	if err != nil {
		return errors.Trace(err)
	}

	h.bind.Store(newBc)
	return nil
}

func parseSQL(sctx sessionctx.Context, parser *parser.Parser, sql string, charset string, collation string) ([]ast.StmtNode, []error, error) {
	return parser.Parse(sql, charset, collation)
}

func decodeBindTableRow(row chunk.Row, fs []*ast.ResultField) (bindRecord, error) {
	var value bindRecord

	value.OriginalSQL = row.GetString(0)
	value.BindSQL = row.GetString(1)
	value.Db = row.GetString(2)
	value.Status = row.GetInt64(3)
	value.CreateTime = row.GetTime(4)
	value.UpdateTime = row.GetTime(5)
	value.Charset = row.GetString(6)
	value.Collation = row.GetString(7)

	return value, nil
}

func (b *BindCache) appendNode(sctx sessionctx.Context, value bindRecord, sparser *parser.Parser) error {
	hash := parser.Digest(value.OriginalSQL)
	if value.Status == 0 {
		if bindArr, ok := b.Cache[hash]; ok {
			if len(bindArr) == 1 {
				if bindArr[0].Db == value.Db {
					delete(b.Cache, hash)
				}
				return nil
			}
			for idx, v := range bindArr {
				if v.Db == value.Db {
					b.Cache[hash] = append(b.Cache[hash][:idx], b.Cache[hash][idx+1:]...)
				}
			}
		}
		return nil
	}

	stmtNodes, _, err := parseSQL(sctx, sparser, value.BindSQL, value.Charset, value.Collation)
	if err != nil {
		log.Warnf("parse error:\n%v\n%s", err, value.BindSQL)
		return errors.Trace(err)
	}

	newNode := &BindData{
		bindRecord: value,
		ast:        stmtNodes[0],
	}

	if bindArr, ok := b.Cache[hash]; ok {
		for idx, v := range bindArr {
			if v.Db == value.Db {
				b.Cache[hash][idx] = newNode
				return nil
			}
		}
	}
	b.Cache[hash] = append(b.Cache[hash], newNode)
	return nil

}
