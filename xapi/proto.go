package xapi

import (
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/xapi/tipb"
)

func columnToProto(c *model.ColumnInfo) *tipb.ColumnInfo {
	pc := &tipb.ColumnInfo{
		ColumnId:  proto.Int64(c.ID),
		Collation: collationToProto(c.FieldType.Collate),
		ColumnLen: proto.Int32(int32(c.FieldType.Flen)),
		Decimal:   proto.Int32(int32(c.FieldType.Decimal)),
		Elems:     c.Elems,
	}
	t := tipb.MysqlType(int32(c.FieldType.Tp))
	pc.Tp = &t
	return pc
}

func collationToProto(c string) *tipb.Collation {
	v, ok := tipb.Collation_value[c]
	if ok {
		a := tipb.Collation(v)
		return &a
	}
	a := tipb.Collation_utf8_general_ci
	return &a
}

// TableToProto converts a model.TableInfo to a tipb.TableInfo.
func TableToProto(t *model.TableInfo) *tipb.TableInfo {
	pt := &tipb.TableInfo{
		TableId: proto.Int64(t.ID),
	}
	cols := make([]*tipb.ColumnInfo, 0, len(t.Columns))
	for _, c := range t.Columns {
		col := columnToProto(c)
		if t.PKIsHandle && mysql.HasPriKeyFlag(c.Flag) {
			col.PkHandle = proto.Bool(true)
		} else {
			col.PkHandle = proto.Bool(false)
		}
		cols = append(cols, col)
	}
	pt.Columns = cols
	return pt
}

// IndexToProto converts a model.IndexInfo to a tipb.IndexInfo.
func IndexToProto(t *model.TableInfo, idx *model.IndexInfo) *tipb.IndexInfo {
	pi := &tipb.IndexInfo{
		TableId: proto.Int64(t.ID),
		IndexId: proto.Int64(idx.ID),
		Unique:  proto.Bool(idx.Unique),
	}
	cols := make([]*tipb.ColumnInfo, 0, len(idx.Columns))
	for _, c := range t.Columns {
		cols = append(cols, columnToProto(c))
	}
	pi.Columns = cols
	return pi
}
