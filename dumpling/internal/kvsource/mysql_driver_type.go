// Copyright 2017 The Go-MySQL-Driver Authors. All rights reserved.
// Copyright 2026 PingCAP, Inc.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Adapted from go-sql-driver/mysql v1.7.1, fields.go:
// https://github.com/go-sql-driver/mysql/blob/v1.7.1/fields.go

package kvsource

import "github.com/pingcap/tidb/pkg/parser/mysql"

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
