// Copyright 2015 PingCAP, Inc.
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
	"github.com/pingcap/parser/mysql"
)

const maxColumnNameSize = 256

// ColumnInfo contains information of a column
type ColumnInfo struct {
	Schema             string
	Table              string
	OrgTable           string
	Name               string
	OrgName            string
	ColumnLength       uint32
	Charset            uint16
	Flag               uint16
	Decimal            uint8
	Type               uint8
	DefaultValueLength uint64
	DefaultValue       []byte
}

// Dump dumps ColumnInfo to bytes.
func (column *ColumnInfo) Dump(buffer []byte) []byte {
	nameDump, orgnameDump := []byte(column.Name), []byte(column.OrgName)
	if len(nameDump) > maxColumnNameSize {
		nameDump = nameDump[0:maxColumnNameSize]
	}
	if len(orgnameDump) > maxColumnNameSize {
		orgnameDump = orgnameDump[0:maxColumnNameSize]
	}
	buffer = dumpLengthEncodedString(buffer, []byte("def"))
	buffer = dumpLengthEncodedString(buffer, []byte(column.Schema))
	buffer = dumpLengthEncodedString(buffer, []byte(column.Table))
	buffer = dumpLengthEncodedString(buffer, []byte(column.OrgTable))
	buffer = dumpLengthEncodedString(buffer, nameDump)
	buffer = dumpLengthEncodedString(buffer, orgnameDump)

	buffer = append(buffer, 0x0c)

	buffer = dumpUint16(buffer, column.Charset)
	buffer = dumpUint32(buffer, column.ColumnLength)
	buffer = append(buffer, dumpType(column.Type))
	buffer = dumpUint16(buffer, dumpFlag(column.Type, column.Flag))
	buffer = append(buffer, column.Decimal)
	buffer = append(buffer, 0, 0)

	if column.DefaultValue != nil {
		buffer = dumpUint64(buffer, uint64(len(column.DefaultValue)))
		buffer = append(buffer, column.DefaultValue...)
	}

	return buffer
}

func dumpFlag(tp byte, flag uint16) uint16 {
	switch tp {
	case mysql.TypeSet:
		return flag | uint16(mysql.SetFlag)
	case mysql.TypeEnum:
		return flag | uint16(mysql.EnumFlag)
	default:
		if mysql.HasBinaryFlag(uint(flag)) {
			return flag | uint16(mysql.NotNullFlag)
		}
		return flag
	}
}

func dumpType(tp byte) byte {
	switch tp {
	case mysql.TypeSet, mysql.TypeEnum:
		return mysql.TypeString
	default:
		return tp
	}
}
