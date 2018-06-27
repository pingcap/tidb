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

// ColumnInfo contains information of a column
type ColumnInfo struct {
	Schema       string
	Table        string
	OrgTable     string
	Name         string
	OrgName      string
	ColumnLength uint32
	Charset      uint16
	Flag         uint16
	Decimal      uint8
	Type         uint8
}

// DumpOpt is used to control dump execution.
type DumpOpt int

const (
	// Normal option indicts that dump with normal steps.
	Normal DumpOpt = 1 << iota
	// WithDefaultValue option indicts that dump with an addition defaultValues step.
	WithDefaultValue
)

// Dump dumps ColumnInfo to bytes.
func (column *ColumnInfo) Dump(buffer []byte, flags DumpOpt) []byte {
	buffer = dumpLengthEncodedString(buffer, []byte("def"))
	buffer = dumpLengthEncodedString(buffer, []byte(column.Schema))
	buffer = dumpLengthEncodedString(buffer, []byte(column.Table))
	buffer = dumpLengthEncodedString(buffer, []byte(column.OrgTable))
	buffer = dumpLengthEncodedString(buffer, []byte(column.Name))
	buffer = dumpLengthEncodedString(buffer, []byte(column.OrgName))

	buffer = append(buffer, 0x0c)

	buffer = dumpUint16(buffer, column.Charset)
	buffer = dumpUint32(buffer, column.ColumnLength)
	buffer = append(buffer, column.Type)
	buffer = dumpUint16(buffer, column.Flag)
	buffer = append(buffer, column.Decimal)
	buffer = append(buffer, 0, 0)

	if flags&WithDefaultValue > 0 {
		// Current we doesn't output defaultValue but reserve defaultValue length bit to make mariadb client happy.
		// https://dev.mysql.com/doc/internals/en/com-query-response.html#column-definition
		buffer = dumpUint64(buffer, 0)
	}

	return buffer
}
