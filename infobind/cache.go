package infobind

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync/atomic"
	"time"
)

type BindData struct {
	Ast ast.StmtNode
	DB  []string
}

type BindInfo struct {
	cache map[string]*BindData
}

type Handle struct {
	bind atomic.Value
}

type tablesBindRecord struct {
	originalSql string
	bindSql     string
	hashCode    []byte
	db          string
	status      int64
	createTime  time.Time
	updateTime  time.Time
}

func NewHandle() *Handle {
	return &Handle{}
}

func (h *Handle) Get() *BindInfo {
	bindInfo := h.bind.Load()
	if bindInfo != nil {
		return bindInfo.(*BindInfo)
	}
	return &BindInfo{
		cache : make(map[string]*BindData, 1000),
	}
}

func (b *BindInfo) LoadDiff(sctx sessionctx.Context, parser *parser.Parser, lastUpTime time.Time) (error, time.Time) {
	ctx := context.Background()
	sql := fmt.Sprintf("select * from mysql.bind_info where update_time > \"%s\"", lastUpTime.Format("2006-01-02 15:04:05.000000"))
	tmp, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, sql)
	if err != nil {
		return errors.Trace(err), lastUpTime
	}

	rs := tmp[0]
	defer terror.Call(rs.Close)

	fs := rs.Fields()
	chk := rs.NewChunk()
	for {
		err = rs.Next(context.TODO(), chk)
		if err != nil {
			return errors.Trace(err), lastUpTime
		}
		if chk.NumRows() == 0 {
			return nil, lastUpTime
		}
		it := chunk.NewIterator4Chunk(chk)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			err, lastUpTime = b.decodeBindTableRow(sctx, row, fs, parser, lastUpTime)
			if err != nil {
				continue
			}
		}
		chk = chunk.Renew(chk, sctx.GetSessionVars().MaxChunkSize)
	}
	return nil, lastUpTime
}

func (h *Handle) Update(ctx sessionctx.Context, lastUpTime time.Time, parser *parser.Parser) (error, time.Time) {
	var err error

	bindInfo := h.Get()
	err, lastUpTime = bindInfo.LoadDiff(ctx, parser, lastUpTime)
	if err != nil {
		return errors.Trace(err), lastUpTime
	}

	h.bind.Store(bindInfo)
	return nil, lastUpTime
}

func (b *BindInfo) parseSQL(sctx sessionctx.Context, parser *parser.Parser, sql string) ([]ast.StmtNode, error) {
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	parser.SetSQLMode(sctx.GetSessionVars().SQLMode)
	parser.EnableWindowFunc(sctx.GetSessionVars().EnableWindowFunction)
	return parser.Parse(sql, charset, collation)
}

func (b *BindInfo) decodeBindTableRow(sctx sessionctx.Context, row chunk.Row, fs []*ast.ResultField, parser *parser.Parser, lastUpTime time.Time) (error, time.Time) {
	var value tablesBindRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "original_sql":
			value.originalSql = row.GetString(i)
		case f.ColumnAsName.L == "bind_sql":
			value.bindSql = row.GetString(i)
		case f.ColumnAsName.L == "db":
			value.db = row.GetString(i)
		case f.ColumnAsName.L == "status":
			value.status = row.GetInt64(i)
		case f.ColumnAsName.L == "create_time":
			var err error
			value.createTime, err = row.GetTime(i).Time.GoTime(time.Local)
			if err != nil {
				return errors.Trace(err), lastUpTime
			}
		case f.ColumnAsName.L == "update_time":
			var err error
			value.updateTime, err = row.GetTime(i).Time.GoTime(time.Local)
			if err != nil {
				return errors.Trace(err), lastUpTime
			}
		}
	}
	return b.append(sctx, value, parser, lastUpTime)
}

func (b *BindInfo) append(sctx sessionctx.Context, value tablesBindRecord, sparser *parser.Parser, lastUpTime time.Time) (error, time.Time) {
	hash := parser.Digest(value.originalSql)
	if value.status == 1 {
		stmtNodes, err := b.parseSQL(sctx, sparser, value.bindSql)
		if err != nil {
			log.Warnf("parse error:\n%v\n%s", err, value.originalSql)
			return errors.Trace(err), lastUpTime
		}

		b.cache[hash] = &BindData{
			Ast: stmtNodes[0],
			DB:  strings.Split(value.db, ","),
		}
	} else {
		delete(b.cache, hash)
	}
	if value.updateTime.After(lastUpTime) {
		lastUpTime = value.updateTime
	}

	return nil, lastUpTime
}

