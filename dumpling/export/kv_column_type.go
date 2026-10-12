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

// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package export

import (
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// kvColumnType returns the database/sql ColumnType.DatabaseTypeName() value that
// go-sql-driver/mysql reports for a table column. The dump-service path reads
// table metadata directly and has no SQL connection, so it reconstructs the wire
// metadata the server would send and then maps it to the name a MySQL client
// observes. This keeps the KV path's ColumnTypes in step with the regular SQL
// dump path.
func kvColumnType(column *model.ColumnInfo) string {
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

// driverDatabaseTypeName mirrors go-sql-driver/mysql v1.7.1
// (*mysqlField).typeDatabaseName. tp and flag are the normalized wire values and
// binary reports whether the advertised charset is the binary collation. Keep it
// in sync when the driver is upgraded.
func driverDatabaseTypeName(tp byte, flag uint16, binary bool) string {
	unsigned := flag&uint16(mysql.UnsignedFlag) != 0
	switch tp {
	case mysql.TypeBit:
		return "BIT"
	case mysql.TypeBlob:
		if binary {
			return "BLOB"
		}
		return "TEXT"
	case mysql.TypeTinyBlob:
		if binary {
			return "TINYBLOB"
		}
		return "TINYTEXT"
	case mysql.TypeMediumBlob:
		if binary {
			return "MEDIUMBLOB"
		}
		return "MEDIUMTEXT"
	case mysql.TypeLongBlob:
		if binary {
			return "LONGBLOB"
		}
		return "LONGTEXT"
	case mysql.TypeDate, mysql.TypeNewDate:
		return "DATE"
	case mysql.TypeDatetime:
		return "DATETIME"
	case mysql.TypeUnspecified, mysql.TypeNewDecimal:
		// MYSQL_TYPE_DECIMAL (0) and MYSQL_TYPE_NEWDECIMAL are both reported as DECIMAL.
		return "DECIMAL"
	case mysql.TypeDouble:
		return "DOUBLE"
	case mysql.TypeEnum:
		return "ENUM"
	case mysql.TypeFloat:
		return "FLOAT"
	case mysql.TypeGeometry:
		return "GEOMETRY"
	case mysql.TypeInt24:
		// The driver reports MEDIUMINT even for the unsigned flag.
		return "MEDIUMINT"
	case mysql.TypeJSON:
		return "JSON"
	case mysql.TypeLong:
		if unsigned {
			return "UNSIGNED INT"
		}
		return "INT"
	case mysql.TypeLonglong:
		if unsigned {
			return "UNSIGNED BIGINT"
		}
		return "BIGINT"
	case mysql.TypeNull:
		return "NULL"
	case mysql.TypeSet:
		return "SET"
	case mysql.TypeShort:
		if unsigned {
			return "UNSIGNED SMALLINT"
		}
		return "SMALLINT"
	case mysql.TypeString:
		if binary {
			return "BINARY"
		}
		return "CHAR"
	case mysql.TypeTimestamp:
		return "TIMESTAMP"
	case mysql.TypeDuration:
		return "TIME"
	case mysql.TypeTiny:
		if unsigned {
			return "UNSIGNED TINYINT"
		}
		return "TINYINT"
	case mysql.TypeVarchar, mysql.TypeVarString:
		if binary {
			return "VARBINARY"
		}
		return "VARCHAR"
	case mysql.TypeYear:
		return "YEAR"
	default:
		return ""
	}
}
