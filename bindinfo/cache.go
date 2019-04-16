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

package bindinfo

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	// using is the bind info's in use status.
	using = "using"
	// deleted is the bind info's deleted status.
	deleted = "deleted"
)

// bindMeta stores the basic bind info and bindSql astNode.
type bindMeta struct {
	*bindRecord
	ast ast.StmtNode //ast will be used to do query sql bind check
}

// cache is a k-v map, key is original sql, value is a slice of bindMeta.
type cache map[string][]*bindMeta

// Handle holds an atomic cache.
type Handle struct {
	atomic.Value
	lock             *sync.Mutex
	ctx              sessionctx.Context
	BindCacheUpdater *BindCacheUpdater
}

// BindCacheUpdater is used to update the global cache.
// BindCacheUpdater will update the bind cache per 3 seconds in domain
// gorountine loop. When the tidb server first startup, the updater will load
// all bind info into memory; then load diff bind info per 3 second.
type BindCacheUpdater struct {
	ctx sessionctx.Context

	lock           *sync.Mutex
	parser         *parser.Parser
	lastUpdateTime types.Time
	globalHandle   *Handle
}

type bindRecord struct {
	OriginalSQL string
	BindSQL     string
	Db          string
	// Status represents the status of the binding. It can only be one of the following values:
	// 1. deleted: bindRecord is deleted, can not be used anymore.
	// 2. using: bindRecord is in the normal active mode.
	Status     string
	CreateTime types.Time
	UpdateTime types.Time
	Charset    string
	Collation  string
}

// NewBindCacheUpdater creates a new BindCacheUpdater.
func NewBindCacheUpdater(ctx sessionctx.Context, handle *Handle, parser *parser.Parser) *BindCacheUpdater {
	return &BindCacheUpdater{
		ctx:          ctx,
		lock:         &sync.Mutex{},
		parser:       parser,
		globalHandle: handle,
	}
}

// NewHandle creates a Handle with a cache.
func NewHandle(ctx sessionctx.Context) *Handle {
	handle := &Handle{
		ctx:  ctx,
		lock: &sync.Mutex{},
	}

	return handle
}

// Get gets cache from a Handle.
func (h *Handle) Get() cache {
	bc := h.Load()
	if bc != nil {
		return bc.(map[string][]*bindMeta)
	}
	return make(map[string][]*bindMeta)
}

// LoadDiff is used to load new bind info to cache bc.
func (bindCacheUpdater *BindCacheUpdater) loadDiff(sql string, bc cache) error {
	rows, _, err := bindCacheUpdater.ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(nil, sql)
	if err != nil {
		return err
	}
	for _, row := range rows {
		record := newBindRecord(row)
		err = bc.appendNode(record, bindCacheUpdater.parser)
		if err != nil {
			return err
		}
		if record.UpdateTime.Compare(bindCacheUpdater.lastUpdateTime) == 1 {
			bindCacheUpdater.lastUpdateTime = record.UpdateTime
		}
	}
	return nil
}

// Update updates the BindCacheUpdater's cache.
// The `fullLoad` is true only when tidb first startup, otherwise it is false.
func (bindCacheUpdater *BindCacheUpdater) Update(fullLoad bool) (err error) {
	bindCacheUpdater.lock.Lock()
	defer bindCacheUpdater.lock.Unlock()
	var sql string
	bc := bindCacheUpdater.globalHandle.Get()
	newBc := make(map[string][]*bindMeta, len(bc))
	for hash, bindDataArr := range bc {
		newBc[hash] = append(newBc[hash], bindDataArr...)
	}

	if fullLoad {
		sql = "select original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation from mysql.bind_info"
	} else {
		sql = fmt.Sprintf("select original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation from mysql.bind_info where update_time > \"%s\"", bindCacheUpdater.lastUpdateTime.String())
	}
	err = bindCacheUpdater.loadDiff(sql, newBc)
	if err != nil {
		return err
	}

	bindCacheUpdater.globalHandle.Store(newBc)
	return nil
}

func (bindCacheUpdater *BindCacheUpdater) updateOneBind(normdOrigSQL, bindSQL, defaultDB string, createTs, updateTs types.Time, status, charset, collation string) error {
	bindCacheUpdater.lock.Lock()
	defer bindCacheUpdater.lock.Unlock()

	record := &bindRecord{
		OriginalSQL: normdOrigSQL,
		BindSQL:     bindSQL,
		Db:          defaultDB,
		Status:      status,
		CreateTime:  createTs,
		UpdateTime:  updateTs,
		Charset:     charset,
		Collation:   collation,
	}

	bc := bindCacheUpdater.globalHandle.Get()
	newBc := make(map[string][]*bindMeta, len(bc))
	for hash, bindDataArr := range bc {
		newBc[hash] = append(newBc[hash], bindDataArr...)
	}
	var err error
	err = appendNode(newBc, record, bindCacheUpdater.parser) //avoid globalHandle store error: store of inconsistently typed value into Value
	if err != nil {
		return err
	}
	bindCacheUpdater.globalHandle.Store(newBc)

	return nil
}

func newBindRecord(row chunk.Row) *bindRecord {
	return &bindRecord{
		OriginalSQL: row.GetString(0),
		BindSQL:     row.GetString(1),
		Db:          row.GetString(2),
		Status:      row.GetString(3),
		CreateTime:  row.GetTime(4),
		UpdateTime:  row.GetTime(5),
		Charset:     row.GetString(6),
		Collation:   row.GetString(7),
	}
}

func appendNode(b cache, newBindRecord *bindRecord, sparser *parser.Parser) error {
	return b.appendNode(newBindRecord, sparser)
}

func (b cache) appendNode(newBindRecord *bindRecord, sparser *parser.Parser) error {
	hash := parser.DigestHash(newBindRecord.OriginalSQL)
	if bindArr, ok := b[hash]; ok {
		for idx, v := range bindArr {
			if v.OriginalSQL == newBindRecord.OriginalSQL && v.Db == newBindRecord.Db && newBindRecord.UpdateTime.Compare(v.UpdateTime) > 0 {
				b[hash] = append(b[hash][:idx], b[hash][idx+1:]...)
				if len(b[hash]) == 0 {
					delete(b, hash)
				}
				break
			}
		}
	}
	if newBindRecord.Status == deleted {
		return nil
	}
	stmtNodes, _, err := sparser.Parse(newBindRecord.BindSQL, newBindRecord.Charset, newBindRecord.Collation)
	if err != nil {
		return err
	}
	newNode := &bindMeta{
		bindRecord: newBindRecord,
		ast:        stmtNodes[0],
	}
	b[hash] = append(b[hash], newNode)
	return nil
}

// AddGlobalBind implements GlobalBindAccessor.AddGlobalBind interface.
func (h *Handle) AddGlobalBind(normdOrigSQL, bindSQL, defaultDB, charset, collation string) (err error) {
	var sqlTime types.Time
	var escapedBindSQL string
	defer func() {
		if err == nil {
			err = h.BindCacheUpdater.updateOneBind(normdOrigSQL, escapedBindSQL, defaultDB, sqlTime, sqlTime, using, charset, collation)
		}
	}()

	h.lock.Lock()
	defer h.lock.Unlock()

	ctx := context.TODO() //we need a new ctx to execute a transcation
	exec, _ := h.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(ctx, "BEGIN")
	if err != nil {
		return
	}
	defer func() {
		if err == nil {
			_, err = exec.Execute(ctx, "COMMIT")
		} else {
			_, err = exec.Execute(ctx, "ROLLBACK")
		}
	}()

	sql := fmt.Sprintf("DELETE FROM mysql.bind_info WHERE original_sql='%s' AND default_db='%s'",
		normdOrigSQL, defaultDB)
	_, err = exec.Execute(ctx, sql)
	if err != nil {
		return
	}
	txn, err := h.ctx.Txn(true) //active a new txn to get startTs as bind's createTs.
	if err != nil {
		return
	}
	ts := oracle.GetTimeFromTS(txn.StartTS())
	tsStr := getTimeStringWithoutZone(ts)
	escapedBindSQL = getEscapeCharacter(bindSQL)
	sql = fmt.Sprintf(`INSERT INTO mysql.bind_info(original_sql,bind_sql,default_db,status,create_time,update_time,charset,collation) VALUES ('%s', '%s', '%s', '%s', '%s', '%s','%s', '%s')`,
		normdOrigSQL, escapedBindSQL, defaultDB, using, tsStr, tsStr, charset, collation)
	_, err = exec.Execute(ctx, sql)
	if err != nil {
		return
	}

	sqlTime = types.Time{
		Time: types.FromGoTime(ts),
		Type: mysql.TypeTimestamp,
		Fsp:  3,
	}

	return
}

func getTimeStringWithoutZone(ts time.Time) string {
	tsString := ts.String()
	strSlice := strings.Split(tsString, " ")
	return strings.Join(strSlice[0:len(strSlice)-2], " ")
}

func getEscapeCharacter(str string) string {
	var buffer bytes.Buffer
	for _, v := range str {
		if v == '\'' || v == '"' || v == '\\' {
			buffer.WriteString("\\")
		}
		buffer.WriteString(string(v))
	}
	return buffer.String()
}
