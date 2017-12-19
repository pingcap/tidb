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

package executor

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// appendDatum2Chunk appends a Datum to a Chunk in column "colIdx" with type "colType".
// TODO: move this function after "executor.Next" removed.
func appendDatum2Chunk(dst *chunk.Chunk, src *types.Datum, colIdx int, colType *types.FieldType) {
	if src.IsNull() {
		dst.AppendNull(colIdx)
		return
	}
	switch colType.Tp {
	case mysql.TypeNull:
		dst.AppendNull(colIdx)
	case mysql.TypeFloat:
		dst.AppendFloat32(colIdx, src.GetFloat32())
	case mysql.TypeDouble:
		dst.AppendFloat64(colIdx, src.GetFloat64())
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		dst.AppendInt64(colIdx, src.GetInt64())
	case mysql.TypeDuration:
		dst.AppendDuration(colIdx, src.GetMysqlDuration())
	case mysql.TypeNewDecimal:
		dst.AppendMyDecimal(colIdx, src.GetMysqlDecimal())
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		dst.AppendTime(colIdx, src.GetMysqlTime())
	case mysql.TypeJSON:
		dst.AppendJSON(colIdx, src.GetMysqlJSON())
	case mysql.TypeBit:
		dst.AppendBytes(colIdx, src.GetMysqlBit())
	case mysql.TypeEnum:
		dst.AppendEnum(colIdx, src.GetMysqlEnum())
	case mysql.TypeSet:
		dst.AppendSet(colIdx, src.GetMysqlSet())
	case mysql.TypeVarchar, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString, mysql.TypeGeometry:
		dst.AppendBytes(colIdx, src.GetBytes())
	default:
		dst.AppendNull(colIdx)
	}
}
