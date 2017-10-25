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
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tidb/xprotocol/notice"
	"github.com/pingcap/tidb/xprotocol/protocol"
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
		return util.ErrXInvalidNamespace.GenByArgs(msg.GetNamespace())
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

func setColumnMeta(c *ColumnInfo) Mysqlx_Resultset.ColumnMetaData {
	var xflags uint32
	var xctype uint32
	var xtp Mysqlx_Resultset.ColumnMetaData_FieldType
	flags := uint(c.Flag)
	if mysql.HasNotNullFlag(flags) {
		xflags |= protocol.FlagNotNull
	}
	if mysql.HasPriKeyFlag(flags) {
		xflags |= protocol.FlagPrimaryKey
	}
	if mysql.HasUniKeyFlag(flags) {
		xflags |= protocol.FlagUniqueKey
	}
	if mysql.HasMultipleKeyFlag(flags) {
		xflags |= protocol.FlagMultipleKey
	}
	if mysql.HasAutoIncrementFlag(flags) {
		xflags |= protocol.FlagAutoIncrement
	}
	if c.Type == mysql.TypeString {
		if mysql.HasSetFlag(flags) {
			c.Type = mysql.TypeSet
		} else if mysql.HasEnumFlag(flags) {
			c.Type = mysql.TypeEnum
		}
	}
	switch c.Type {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		if mysql.HasUnsignedFlag(flags) {
			xtp = Mysqlx_Resultset.ColumnMetaData_UINT
		} else {
			xtp = Mysqlx_Resultset.ColumnMetaData_SINT
		}

		if mysql.HasZerofillFlag(flags) {
			xflags |= protocol.FlagUintZeroFill
		}
	case mysql.TypeFloat:
		if mysql.HasUnsignedFlag(flags) {
			xflags |= protocol.FlagFloatUnsigned
		}
		xtp = Mysqlx_Resultset.ColumnMetaData_FLOAT

	case mysql.TypeDouble:
		if mysql.HasUnsignedFlag(flags) {
			xflags |= protocol.FlagDoubleUnsigned
		}
		xtp = Mysqlx_Resultset.ColumnMetaData_DOUBLE

	case mysql.TypeDecimal, mysql.TypeNewDecimal:
		if mysql.HasUnsignedFlag(flags) {
			xflags |= protocol.FlagDecimalUnsigned
		}
		xtp = Mysqlx_Resultset.ColumnMetaData_DECIMAL

	case mysql.TypeString:
		xtp = Mysqlx_Resultset.ColumnMetaData_BYTES
		xflags |= protocol.FlagBytesRightpad
		// TODO: Collation should be set properly here.

	case mysql.TypeSet:
		xtp = Mysqlx_Resultset.ColumnMetaData_SET
		// TODO: Collation should be set properly here.

	case mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeVarchar, mysql.TypeVarString:
		xtp = Mysqlx_Resultset.ColumnMetaData_BYTES
		// TODO: Collation should be set properly here.

	case mysql.TypeJSON:
		xtp = Mysqlx_Resultset.ColumnMetaData_BYTES
		xctype = protocol.ContentTypeJSON
		// TODO: Collation should be set properly here.
		break

	case mysql.TypeGeometry:
		xtp = Mysqlx_Resultset.ColumnMetaData_BYTES
		xctype = protocol.ContentTypeGeometry

	case mysql.TypeDuration, mysql.TypeTime2:
		xtp = Mysqlx_Resultset.ColumnMetaData_TIME

	case mysql.TypeNewDate, mysql.TypeDate:
		xtp = Mysqlx_Resultset.ColumnMetaData_DATETIME

	case mysql.TypeDatetime, mysql.TypeDatetime2:
		xtp = Mysqlx_Resultset.ColumnMetaData_DATETIME

	case mysql.TypeYear:
		xtp = Mysqlx_Resultset.ColumnMetaData_UINT

	case mysql.TypeTimestamp, mysql.TypeTimestamp2:
		xtp = Mysqlx_Resultset.ColumnMetaData_DATETIME
		xflags = protocol.FlagDatetimeTimestamp

	case mysql.TypeEnum:
		xtp = Mysqlx_Resultset.ColumnMetaData_ENUM
		// TODO: Collation should be set properly here.

	case mysql.TypeNull:
		xtp = Mysqlx_Resultset.ColumnMetaData_BYTES

	case mysql.TypeBit:
		xtp = Mysqlx_Resultset.ColumnMetaData_BIT
	}
	return Mysqlx_Resultset.ColumnMetaData{
		Type:          &xtp,
		Name:          []byte(c.Name),
		Table:         []byte(c.OrgName),
		OriginalTable: []byte(c.OrgTable),
		Schema:        []byte(c.Schema),
		Length:        &c.ColumnLength,
		Flags:         &xflags,
		ContentType:   &xctype,
	}
}

func writeColumnsInfo(columns []*ColumnInfo, pkt *xpacketio.XPacketIO) error {
	for _, c := range columns {
		columnMeta := setColumnMeta(c)
		data, err := columnMeta.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		if err = pkt.WritePacket(Mysqlx.ServerMessages_RESULTSET_COLUMN_META_DATA, data); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// WriteResultSet write result set message to client
// @TODO this is important to performance, need to consider carefully and tuning in next pr
func WriteResultSet(r ResultSet, pkt *xpacketio.XPacketIO, alloc arena.Allocator) error {
	defer terror.Call(r.Close)
	row, err := r.Next()
	if err != nil {
		return errors.Trace(err)
	}
	cols, err := r.Columns()
	if err != nil {
		return errors.Trace(err)
	}

	// Write column information.
	if err = writeColumnsInfo(cols, pkt); err != nil {
		return errors.Trace(err)
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
	for _, val := range row {
		datum, err := protocol.DumpDatumToBinary(alloc, val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		fields = append(fields, datum)
	}
	return &Mysqlx_Resultset.Row{
		Field: fields,
	}, nil
}
