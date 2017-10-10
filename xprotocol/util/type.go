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

package util

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tipb/go-mysqlx/Resultset"
	"unicode"
)

var unsignedXType = map[byte]Mysqlx_Resultset.ColumnMetaData_FieldType{
	// Unsigned numeric type
	mysql.TypeTiny:     Mysqlx_Resultset.ColumnMetaData_UINT,
	mysql.TypeShort:    Mysqlx_Resultset.ColumnMetaData_UINT,
	mysql.TypeInt24:    Mysqlx_Resultset.ColumnMetaData_UINT,
	mysql.TypeLong:     Mysqlx_Resultset.ColumnMetaData_UINT,
	mysql.TypeLonglong: Mysqlx_Resultset.ColumnMetaData_UINT,

	// TODO: Remove the following types when TiDB deals with unsigned flag properly.
	// Clarified type
	mysql.TypeDouble:    Mysqlx_Resultset.ColumnMetaData_DOUBLE,
	mysql.TypeFloat:     Mysqlx_Resultset.ColumnMetaData_FLOAT,
	mysql.TypeDecimal:   Mysqlx_Resultset.ColumnMetaData_DECIMAL,
	mysql.TypeVarchar:   Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeString:    Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeGeometry:  Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeDuration:  Mysqlx_Resultset.ColumnMetaData_TIME,
	mysql.TypeDate:      Mysqlx_Resultset.ColumnMetaData_DATETIME,
	mysql.TypeDatetime:  Mysqlx_Resultset.ColumnMetaData_DATETIME,
	mysql.TypeYear:      Mysqlx_Resultset.ColumnMetaData_UINT,
	mysql.TypeTimestamp: Mysqlx_Resultset.ColumnMetaData_DATETIME,
	mysql.TypeSet:       Mysqlx_Resultset.ColumnMetaData_SET,
	mysql.TypeEnum:      Mysqlx_Resultset.ColumnMetaData_ENUM,
	mysql.TypeNull:      Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeBit:       Mysqlx_Resultset.ColumnMetaData_BIT,

	// TODO: Clarify type mapping below.
	mysql.TypeNewDate:    Mysqlx_Resultset.ColumnMetaData_DATETIME,
	mysql.TypeJSON:       Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeNewDecimal: Mysqlx_Resultset.ColumnMetaData_DECIMAL,
	mysql.TypeTinyBlob:   Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeMediumBlob: Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeLongBlob:   Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeBlob:       Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeVarString:  Mysqlx_Resultset.ColumnMetaData_BYTES,
}

var commonXType = map[byte]Mysqlx_Resultset.ColumnMetaData_FieldType{
	// Signed numeric type
	mysql.TypeTiny:     Mysqlx_Resultset.ColumnMetaData_SINT,
	mysql.TypeShort:    Mysqlx_Resultset.ColumnMetaData_SINT,
	mysql.TypeInt24:    Mysqlx_Resultset.ColumnMetaData_SINT,
	mysql.TypeLong:     Mysqlx_Resultset.ColumnMetaData_SINT,
	mysql.TypeLonglong: Mysqlx_Resultset.ColumnMetaData_SINT,

	// Clarified type
	mysql.TypeDouble:    Mysqlx_Resultset.ColumnMetaData_DOUBLE,
	mysql.TypeFloat:     Mysqlx_Resultset.ColumnMetaData_FLOAT,
	mysql.TypeDecimal:   Mysqlx_Resultset.ColumnMetaData_DECIMAL,
	mysql.TypeVarchar:   Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeString:    Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeGeometry:  Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeDuration:  Mysqlx_Resultset.ColumnMetaData_TIME,
	mysql.TypeDate:      Mysqlx_Resultset.ColumnMetaData_DATETIME,
	mysql.TypeDatetime:  Mysqlx_Resultset.ColumnMetaData_DATETIME,
	mysql.TypeYear:      Mysqlx_Resultset.ColumnMetaData_UINT,
	mysql.TypeTimestamp: Mysqlx_Resultset.ColumnMetaData_DATETIME,
	mysql.TypeSet:       Mysqlx_Resultset.ColumnMetaData_SET,
	mysql.TypeEnum:      Mysqlx_Resultset.ColumnMetaData_ENUM,
	mysql.TypeNull:      Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeBit:       Mysqlx_Resultset.ColumnMetaData_BIT,

	// TODO: Clarify type mapping below.
	mysql.TypeNewDate:    Mysqlx_Resultset.ColumnMetaData_DATETIME,
	mysql.TypeJSON:       Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeNewDecimal: Mysqlx_Resultset.ColumnMetaData_DECIMAL,
	mysql.TypeTinyBlob:   Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeMediumBlob: Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeLongBlob:   Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeBlob:       Mysqlx_Resultset.ColumnMetaData_BYTES,
	mysql.TypeVarString:  Mysqlx_Resultset.ColumnMetaData_BYTES,
}

// MysqlType2XType convert MySQL type to X Protocol type.
func MysqlType2XType(tp byte, unsigned bool) (Mysqlx_Resultset.ColumnMetaData_FieldType, error) {
	if unsigned {
		if colTp, ok := unsignedXType[tp]; ok {
			return colTp, nil
		}
	} else {
		if colTp, ok := commonXType[tp]; ok {
			return colTp, nil
		}
	}
	return Mysqlx_Resultset.ColumnMetaData_SINT, errors.Errorf("unknown column type %d", tp)
}

// QuoteIdentifier quotes identifier with "`".
func QuoteIdentifier(str string) string {
	return "`" + str + "`"
}

// QuoteIdentifierIfNeeded quotes identifier if needed.
func QuoteIdentifierIfNeeded(str string) string {
	needQuote := false
	if len(str) > 0 && unicode.IsLetter(rune(str[0])) {
		for _, r := range str[1:] {
			if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '_' {
				needQuote = true
				break
			}
		}
	} else {
		needQuote = true
	}

	if needQuote {
		return QuoteIdentifier(str)
	}
	return str
}

// QuoteString quotes string with "'".
func QuoteString(str string) string {
	return "'" + str + "'"
}
