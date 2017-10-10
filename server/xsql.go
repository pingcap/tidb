// Copyright 2017 PingCAP, Inc.
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

package server

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xprotocol/notice"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Resultset"
	"github.com/pingcap/tipb/go-mysqlx/Sql"
)

type xSQL struct {
	xcc *mysqlXClientConn
	ctx *QueryCtx
	pkt *xpacketio.XPacketIO
}

func createXSQL(xcc *mysqlXClientConn) *xSQL {
	return &xSQL{
		xcc: xcc,
		ctx: &xcc.ctx,
		pkt: xcc.pkt,
	}
}

func (xsql *xSQL) dealSQLStmtExecute(payload []byte) error {
	var msg Mysqlx_Sql.StmtExecute
	if err := msg.Unmarshal(payload); err != nil {
		return err
	}

	switch msg.GetNamespace() {
	case "xplugin", "mysqlx":
		// TODO: 'xplugin' is deprecated, need to send a notice message.
		if err := xsql.dispatchAdminCmd(msg); err != nil {
			return errors.Trace(err)
		}
	case "sql", "":
		sql := string(msg.GetStmt())
		if err := xsql.executeStmt(sql); err != nil {
			return errors.Trace(err)
		}
	default:
		return util.ErXInvalidNamespace.GenByArgs(msg.GetNamespace())
	}
	return SendExecOk(xsql.pkt, (*xsql.ctx).LastInsertID())
}

func (xsql *xSQL) executeStmtNoResult(sql string) error {
	if _, err := (*xsql.ctx).Execute(sql); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (xsql *xSQL) executeStmt(sql string) error {
	rs, err := (*xsql.ctx).Execute(sql)
	if err != nil {
		return err
	}
	for _, r := range rs {
		if err := WriteResultSet(r, xsql.pkt, xsql.xcc.alloc); err != nil {
			return err
		}
	}
	return nil
}

// WriteResultSet write result set message to client
// @TODO this is important to performance, need to consider carefully and tuning in next pr
func WriteResultSet(r ResultSet, pkt *xpacketio.XPacketIO, alloc arena.Allocator) error {
	defer r.Close()
	row, err := r.Next()
	if err != nil {
		return errors.Trace(err)
	}
	cols, err := r.Columns()
	if err != nil {
		return errors.Trace(err)
	}

	// Write column information.
	for _, c := range cols {
		var tp Mysqlx_Resultset.ColumnMetaData_FieldType
		tp, err = util.MysqlType2XType(c.Type, mysql.HasUnsignedFlag(uint(c.Flag)))
		if err != nil {
			return errors.Trace(err)
		}
		flags := uint32(c.Flag)
		columnMeta := Mysqlx_Resultset.ColumnMetaData{
			Type:          &tp,
			Name:          []byte(c.Name),
			Table:         []byte(c.OrgName),
			OriginalTable: []byte(c.OrgTable),
			Schema:        []byte(c.Schema),
			Length:        &c.ColumnLength,
			Flags:         &flags,
		}
		var data []byte
		data, err = columnMeta.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		if err = pkt.WritePacket(Mysqlx.ServerMessages_RESULTSET_COLUMN_META_DATA, data); err != nil {
			return errors.Trace(err)
		}
	}

	// Write rows.
	for {
		if err != nil {
			return errors.Trace(err)
		}
		if row == nil {
			break
		}
		if err != nil {
			return errors.Trace(err)
		}

		rowData, err := rowToRow(alloc, cols, row)
		if err != nil {
			return errors.Trace(err)
		}
		data, err := rowData.Marshal()
		if err != nil {
			return errors.Trace(err)
		}

		if err = pkt.WritePacket(Mysqlx.ServerMessages_RESULTSET_ROW, data); err != nil {
			return errors.Trace(err)
		}
		row, err = r.Next()
	}

	if err := pkt.WritePacket(Mysqlx.ServerMessages_RESULTSET_FETCH_DONE, []byte{}); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// SendExecOk send exec ok message to client, used when statement is finished.
func SendExecOk(pkt *xpacketio.XPacketIO, lastID uint64) error {
	if lastID > 0 {
		if err := notice.SendLastInsertID(pkt, lastID); err != nil {
			return errors.Trace(err)
		}
	}
	if err := pkt.WritePacket(Mysqlx.ServerMessages_SQL_STMT_EXECUTE_OK, nil); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func rowToRow(alloc arena.Allocator, columns []*ColumnInfo, row []types.Datum) (*Mysqlx_Resultset.Row, error) {
	if len(columns) != len(row) {
		return nil, mysql.ErrMalformPacket
	}
	var fields [][]byte
	for i, val := range row {
		datum, err := dumpDatumToBinary(alloc, columns[i], val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fields = append(fields, datum)
	}
	return &Mysqlx_Resultset.Row{
		Field: fields,
	}, nil
}
