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
	"github.com/pingcap/tidb/util/arena"
)

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
func (column *ColumnInfo) Dump(alloc arena.Allocator) []byte {
	l := len(column.Schema) + len(column.Table) + len(column.OrgTable) + len(column.Name) + len(column.OrgName) + len(column.DefaultValue) + 48

	data := make([]byte, 0, l)

	data = dumpLengthEncodedString(data, []byte("def"))
	data = dumpLengthEncodedString(data, []byte(column.Schema))
	data = dumpLengthEncodedString(data, []byte(column.Table))
	data = dumpLengthEncodedString(data, []byte(column.OrgTable))
	data = dumpLengthEncodedString(data, []byte(column.Name))
	data = dumpLengthEncodedString(data, []byte(column.OrgName))

	data = append(data, 0x0c)

	data = dumpUint16(data, column.Charset)
	data = dumpUint32(data, column.ColumnLength)
	data = append(data, column.Type)
	data = dumpUint16(data, column.Flag)
	data = append(data, column.Decimal)
	data = append(data, 0, 0)

	if column.DefaultValue != nil {
		data = dumpUint64(data, uint64(len(column.DefaultValue)))
		data = append(data, column.DefaultValue...)
	}

	return data
}
