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

//BindData store the basic bind info and bindSql astNode
type BindData struct {
	bindRecord
	ast ast.StmtNode
}

//BindCache hold a bindDataMap, key:origin sql hash value: bindData slice
type BindCache struct {
	Cache map[string][]*BindData
}

//Handle hold a atomic bindCache
type Handle struct {
	bind atomic.Value
}

//HandleUpdater use to update the BindCache
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
}

//NewHandle create a Handle with a BindCache
func NewHandle() *Handle {
	handle := &Handle{}
	bc := &BindCache{
		Cache: make(map[string][]*BindData, 1000),
	}
	handle.bind.Store(bc)
	return handle
}

//Get get bindCache from a Handle
func (h *Handle) Get() *BindCache {
	bc := h.bind.Load()
	if bc != nil {
		return bc.(*BindCache)
	}
	return &BindCache{
		Cache: make(map[string][]*BindData, 1000),
	}
}

//Load Diff use to load new bind info to bindCache bc
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
			log.Infof("record %v", record)
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

//Update update the HandleUpdater's bindCache if tidb first startup,the fullLoad is true,otherwise fullLoad is false
func (h *HandleUpdater) Update(fullLoad bool) error {
	var (
		err error
		sql string
	)
	bc := h.Get()
	if fullLoad {
		sql = fmt.Sprintf("select * from mysql.bind_info")
	} else {
		sql = fmt.Sprintf("select * from mysql.bind_info where update_time > \"%s\"", h.LastUpdateTime.String())
	}
	err = h.loadDiff(sql, bc)
	if err != nil {
		return errors.Trace(err)
	}

	h.bind.Store(bc)
	bc.Display()
	return nil
}

func parseSQL(sctx sessionctx.Context, parser *parser.Parser, sql string) ([]ast.StmtNode, []error, error) {
	charset, collation := sctx.GetSessionVars().GetCharsetInfo()
	parser.SetSQLMode(sctx.GetSessionVars().SQLMode)
	parser.EnableWindowFunc(sctx.GetSessionVars().EnableWindowFunction)
	return parser.Parse(sql, charset, collation)
}

func decodeBindTableRow(row chunk.Row, fs []*ast.ResultField) (bindRecord, error) {
	var value bindRecord
	for i, f := range fs {
		switch {
		case f.ColumnAsName.L == "original_sql":
			value.OriginalSQL = row.GetString(i)
		case f.ColumnAsName.L == "bind_sql":
			value.BindSQL = row.GetString(i)
		case f.ColumnAsName.L == "default_db":
			value.Db = row.GetString(i)
		case f.ColumnAsName.L == "status":
			value.Status = row.GetInt64(i)
		case f.ColumnAsName.L == "create_time":
			var err error
			value.CreateTime = row.GetTime(i)
			if err != nil {
				return value, errors.Trace(err)
			}
		case f.ColumnAsName.L == "update_time":
			var err error
			value.UpdateTime = row.GetTime(i)
			if err != nil {
				return value, errors.Trace(err)
			}
		}
	}
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

	stmtNodes, _, err := parseSQL(sctx, sparser, value.BindSQL)
	if err != nil {
		log.Warnf("parse error:\n%v\n%s", err, value.BindSQL)
		return errors.Trace(err)
	}

	newNode := &BindData{
		bindRecord: value,
		ast:        stmtNodes[0],
	}

	log.Infof("original sql [%s] bind sql [%s]", value.OriginalSQL, value.BindSQL)
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

//Display print all bind info into log
func (b *BindCache) Display() {
	for hash, bindArr := range b.Cache {
		log.Infof("------------------hash entry %s-----------------------", hash)
		for _, bindData := range bindArr {
			log.Infof("%v", bindData.bindRecord)
		}
		log.Infof("------------------hash entry end -----------------------")

	}
}
