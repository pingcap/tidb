// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvsource

import (
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// ColumnType returns the database/sql ColumnType.DatabaseTypeName() value that
// go-sql-driver/mysql reports for a table column. The dump-service path reads
// table metadata directly and has no SQL connection, so it reconstructs the wire
// metadata the server would send and then maps it to the name a MySQL client
// observes. This keeps the KV path's ColumnTypes in step with the regular SQL
// dump path.
func ColumnType(column *model.ColumnInfo) string {
	return driverDatabaseTypeName(
		dumpType(column.GetType()),
		dumpFlag(column.GetType(), uint16(column.GetFlag())),
		isBinaryColumnCharset(column),
	)
}

// isBinaryColumnCharset reports whether the driver observes the binary
// collation for this column. The server advertises Vector columns as a
// non-binary string, so they never count as binary here either.
func isBinaryColumnCharset(column *model.ColumnInfo) bool {
	if column.GetType() == mysql.TypeTiDBVectorFloat32 {
		return false
	}
	return column.GetCharset() == charset.CharsetBin
}

// dumpType mirrors the server's normalization of a TiDB column type to the type
// byte sent in a MySQL column-definition packet
// (pkg/server/internal/column.dumpType). It is copied rather than imported
// because that helper is server-internal; keep the two in sync.
func dumpType(tp byte) byte {
	switch tp {
	case mysql.TypeSet, mysql.TypeEnum:
		return mysql.TypeString
	case mysql.TypeTiDBVectorFloat32:
		// When passing Vector column to the SQL Client, pretend to be a non-binary String.
		return mysql.TypeLongBlob
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return mysql.TypeBlob
	default:
		return tp
	}
}

// dumpFlag mirrors the server's normalization of column flags
// (pkg/server/internal/column.DumpFlag). It is copied rather than imported
// because that helper is server-internal; keep the two in sync.
func dumpFlag(tp byte, flag uint16) uint16 {
	switch tp {
	case mysql.TypeSet:
		return flag | uint16(mysql.SetFlag)
	case mysql.TypeEnum:
		return flag | uint16(mysql.EnumFlag)
	case mysql.TypeTiDBVectorFloat32:
		// When passing Vector column to the SQL Client, pretend to be a non-binary String.
		return flag & ^uint16(mysql.BinaryFlag)
	default:
		return flag
	}
}
