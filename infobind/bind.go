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
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/prometheus/common/log"
	"strings"
	"time"
)

type InfoBind struct {
	Ast      ast.StmtNode
	Database []string
}

type Handle struct {
	cache          *kvcache.SimpleMap
	lastUpdateTime time.Time
	parser         *parser.Parser
}

type BindStat struct {
	bindCache *kvcache.SimpleMap
}
type tablesBindRecord struct {
	originalSql string
	bindSql     string
	hashCode    []byte
	db          string
	status     int64
	createTime  time.Time
	updateTime  time.Time
}

func NewHandle() *Handle {
	return &Handle{
		cache:          kvcache.NewSimpleMapCache(),
		lastUpdateTime: time.Now(),
		parser:         parser.New(),
	}
}

func (h *Handle) Get(key string) *InfoBind {
	value, ok := h.cache.Get(key)
	if ok {
		return value.(*InfoBind)
	}
	return nil
}

func (h *Handle) Put(key string, value *InfoBind)  {
	 h.cache.Put(key, value)
}

func (h *Handle) Delete(key string) {
	h.cache.Delete(key)
}

func (h *Handle) Update(sctx sessionctx.Context, fullLoad bool) error {
	ctx := context.Background()
	var sql string
	if fullLoad {
		sql = fmt.Sprintf("select * from mysql.bind_info")
	} else {
		sql = fmt.Sprintf("select * from mysql.bind_info where update_time > %s", h.lastUpdateTime)
	}
	tmp, err := sctx.(sqlexec.SQLExecutor).Execute(ctx, sql)
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
			err = h.decodeBindTableRow(sctx, row, fs)
			if err != nil {
				continue
			}
		}
		chk = chunk.Renew(chk, sctx.GetSessionVars().MaxChunkSize)
	}
	return nil
}
func (h *Handle) ParseSQL(sctx sessionctx.Context, sql string) ([]ast.StmtNode, error){
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	h.parser.SetSQLMode(sctx.GetSessionVars().SQLMode)
	h.parser.EnableWindowFunc(sctx.GetSessionVars().EnableWindowFunction)
	return  h.parser.Parse(sql, charset, collation)
}

func (h *Handle) decodeBindTableRow(sctx sessionctx.Context, row chunk.Row, fs []*ast.ResultField) error {
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
				return errors.Trace(err)
			}
		case f.ColumnAsName.L == "update_time":
			var err error
			value.updateTime, err = row.GetTime(i).Time.GoTime(time.Local)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	hash := parser.Digest(value.originalSql)
	if value.status == 1 {
		stmtNodes, err := h.ParseSQL(sctx, value.originalSql)
		if err != nil {
			log.Warnf("parse error:\n%v\n%s",  err, value.originalSql)
			return  errors.Trace(err)
		}

		h.Put(hash, &InfoBind{
			Ast:      stmtNodes[0],
			Database: strings.Split(value.db, ","),
		})
	} else {
		h.Delete(hash)
	}
	if value.updateTime.After(h.lastUpdateTime) {
		h.lastUpdateTime = value.updateTime
	}

	return nil
}
