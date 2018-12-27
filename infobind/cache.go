package infobind

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	log "github.com/sirupsen/logrus"
)

type BindData struct {
	bindRecord
	ast         ast.StmtNode
}

type BindCache struct {
	cache map[string][]*BindData
}

type Handle struct {
	bind atomic.Value
}

type HandleUpdater struct {
	Parser         *parser.Parser
	LastUpdateTime time.Time
	Ctx            sessionctx.Context
	*Handle
}

type bindRecord struct {
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

func (h *Handle) Get() *BindCache {
	bc := h.bind.Load()
	if bc != nil {
		return bc.(*BindCache)
	}
	return &BindCache{
		cache: make(map[string][]*BindData, 1000),
	}
}

func (h *HandleUpdater) LoadDiff(sql string, bc *BindCache) (error, *BindCache) {
	tmp, err := h.Ctx.(sqlexec.SQLExecutor).Execute(context.Background(), sql)
	if err != nil {
		return errors.Trace(err), bc
	}

	rs := tmp[0]
	defer terror.Call(rs.Close)

	fs := rs.Fields()
	chk := rs.NewChunk()
	for {
		err = rs.Next(context.TODO(), chk)
		if err != nil {
			return errors.Trace(err), bc
		}
		if chk.NumRows() == 0 {
			return nil, bc
		}
		it := chunk.NewIterator4Chunk(chk)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			err, record := decodeBindTableRow(row, fs)
			if err != nil {
				log.Errorf("row decode error %s", err)
				continue
			}
			log.Infof("record %v", record)
			err = bc.appendNode(h.Ctx, record, h.Parser)
			if err != nil {
				continue
			}
			if record.updateTime.After(h.LastUpdateTime) {
				h.LastUpdateTime = record.updateTime
			}
		}
		chk = chunk.Renew(chk, h.Ctx.GetSessionVars().MaxChunkSize)
	}

	return nil, bc
}

func (h *HandleUpdater) Update(fullLoad bool) error {
	var (
		err error
		sql string
	)
	bc := h.Get()
	if fullLoad {
		sql = fmt.Sprintf("select * from mysql.bind_info")
	} else {
		sql = fmt.Sprintf("select * from mysql.bind_info where update_time > \"%s\"", h.LastUpdateTime.Format("2006-01-02 15:04:05.000000"))
	}
	log.Infof("sql %s", sql)
	err, bc = h.LoadDiff(sql, bc)
	if err != nil {
		return errors.Trace(err)
	}

	h.bind.Store(bc)
	bc.Display()
	return nil
}

func parseSQL(sctx sessionctx.Context, parser *parser.Parser, sql string) ([]ast.StmtNode, error) {
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	parser.SetSQLMode(sctx.GetSessionVars().SQLMode)
	parser.EnableWindowFunc(sctx.GetSessionVars().EnableWindowFunction)
	return parser.Parse(sql, charset, collation)
}

func decodeBindTableRow(row chunk.Row, fs []*ast.ResultField) (error, bindRecord) {
	var value bindRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "original_sql":
			value.originalSql = row.GetString(i)
		case f.ColumnAsName.L == "bind_sql":
			value.bindSql = row.GetString(i)
		case f.ColumnAsName.L == "default_db":
			value.db = row.GetString(i)
		case f.ColumnAsName.L == "status":
			value.status = row.GetInt64(i)
		case f.ColumnAsName.L == "create_time":
			var err error
			value.createTime, err = row.GetTime(i).Time.GoTime(time.Local)
			if err != nil {
				return errors.Trace(err), value
			}
		case f.ColumnAsName.L == "update_time":
			var err error
			value.updateTime, err = row.GetTime(i).Time.GoTime(time.Local)
			if err != nil {
				return errors.Trace(err), value
			}
		}
	}
	return nil, value
}

func (b *BindCache) appendNode(sctx sessionctx.Context, value bindRecord, sparser *parser.Parser) error {
	hash := parser.Digest(value.originalSql)
	if value.status == 0 {
		if bindArr, ok := b.cache[hash]; ok {
			for idx, v := range bindArr {
				if v.db == value.db {
					b.cache[hash] = append(b.cache[hash][:idx], b.cache[hash][idx+1:]...)
				}
			}
			if len(b.cache[hash]) == 0 {
				delete(b.cache, hash)
			}
		}
		return nil
	}

	_, err := parseSQL(sctx, sparser, value.originalSql)
	if err != nil {
		log.Warnf("parse error:\n%v\n%s", err, value.originalSql)
		return errors.Trace(err)
	}
	stmtNodes, err := parseSQL(sctx, sparser, value.bindSql)
	if err != nil {
		log.Warnf("parse error:\n%v\n%s", err, value.bindSql)
		return errors.Trace(err)
	}
	log.Infof("original sql [%s] bind sql [%s]", value.originalSql, value.bindSql)
	bindArr, ok := b.cache[hash]
	if ok {
		for idx, v := range bindArr {
			if v.db == value.db {
				b.cache[hash][idx] = &BindData{
					bindRecord: value,
					ast:         stmtNodes[0],
				}
				return nil
			}
		}
	}
	bindArr = append(bindArr, &BindData{
		bindRecord:value,
		ast:         stmtNodes[0],
	})
	b.cache[hash] = bindArr
	return nil

}

func (b *BindCache) Display() {
	for hash, bindArr := range b.cache {
		log.Infof("------------------hash entry %s-----------------------", hash)
		for _, bindData := range bindArr {
			log.Infof("%v", bindData.bindRecord)
		}
		log.Infof("------------------hash entry end -----------------------")

	}
}
