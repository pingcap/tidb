package metric_table

import (
	"fmt"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/types"
	"sync"
)

var once sync.Once

// Init register the METRIC_DB virtual tables.
func Init() {
	initOnce := func() {
		dbID := autoid.GenLocalSchemaID()
		metricTables := make([]*model.TableInfo, 0)

		for name, def := range metricTableMap {
			cols := def.genColumnInfos()
			tableInfo := buildTableMeta(name, cols)
			tableInfo.ID = autoid.GenLocalSchemaID()
			for i, c := range tableInfo.Columns {
				c.ID = int64(i) + 1
			}
			metricTables = append(metricTables, tableInfo)
		}
		dbInfo := &model.DBInfo{
			ID:      dbID,
			Name:    model.NewCIStr(MetricDBName),
			Charset: mysql.DefaultCharset,
			Collate: mysql.DefaultCollationName,
			Tables:  metricTables,
		}
		infoschema.RegisterVirtualTable(dbInfo, tableFromMeta)
		fmt.Printf("---------\n register metric tables\n\n")
	}
	once.Do(initOnce)
}

func buildColumnInfo(col columnInfo) *model.ColumnInfo {
	mCharset := charset.CharsetBin
	mCollation := charset.CharsetBin
	mFlag := mysql.UnsignedFlag
	if col.tp == mysql.TypeVarchar || col.tp == mysql.TypeBlob {
		mCharset = charset.CharsetUTF8MB4
		mCollation = charset.CollationUTF8MB4
		mFlag = col.flag
	}
	fieldType := types.FieldType{
		Charset: mCharset,
		Collate: mCollation,
		Tp:      col.tp,
		Flen:    col.size,
		Flag:    mFlag,
	}
	return &model.ColumnInfo{
		Name:         model.NewCIStr(col.name),
		FieldType:    fieldType,
		State:        model.StatePublic,
		DefaultValue: col.deflt,
	}
}

func buildTableMeta(tableName string, cs []columnInfo) *model.TableInfo {
	cols := make([]*model.ColumnInfo, 0, len(cs))
	for _, c := range cs {
		cols = append(cols, buildColumnInfo(c))
	}
	for i, col := range cols {
		col.Offset = i
	}
	return &model.TableInfo{
		Name:    model.NewCIStr(tableName),
		Columns: cols,
		State:   model.StatePublic,
		Charset: mysql.DefaultCharset,
		Collate: mysql.DefaultCollationName,
	}
}
