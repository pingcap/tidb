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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package column

import (
	"fmt"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/server/internal/dump"
)

const maxColumnNameSize = 256

// Info contains information of a column
type Info struct {
	DefaultValue any
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

// Dump dumps Info to bytes.
func (column *Info) Dump(buffer []byte, d *ResultEncoder) []byte {
	return column.dump(buffer, d, false)
}

// DumpWithDefault dumps Info to bytes, including column defaults. This is used for ComFieldList responses.
func (column *Info) DumpWithDefault(buffer []byte, d *ResultEncoder) []byte {
	return column.dump(buffer, d, true)
}

func (column *Info) dump(buffer []byte, d *ResultEncoder, withDefault bool) []byte {
	if d == nil {
		d = NewResultEncoder(charset.CharsetUTF8MB4)
	}
	nameDump, orgnameDump := []byte(column.Name), []byte(column.OrgName)
	if len(nameDump) > maxColumnNameSize {
		nameDump = nameDump[0:maxColumnNameSize]
	}
	if len(orgnameDump) > maxColumnNameSize {
		orgnameDump = orgnameDump[0:maxColumnNameSize]
	}
	buffer = dump.LengthEncodedString(buffer, []byte("def"))
	buffer = dump.LengthEncodedString(buffer, d.EncodeMeta([]byte(column.Schema)))
	buffer = dump.LengthEncodedString(buffer, d.EncodeMeta([]byte(column.Table)))
	buffer = dump.LengthEncodedString(buffer, d.EncodeMeta([]byte(column.OrgTable)))
	buffer = dump.LengthEncodedString(buffer, d.EncodeMeta(nameDump))
	buffer = dump.LengthEncodedString(buffer, d.EncodeMeta(orgnameDump))

	buffer = append(buffer, 0x0c)
	buffer = dump.Uint16(buffer, d.ColumnTypeInfoCharsetID(column))
	buffer = dump.Uint32(buffer, column.ColumnLength)
	buffer = append(buffer, dumpType(column.Type))
	buffer = dump.Uint16(buffer, DumpFlag(column.Type, column.Flag))
	buffer = append(buffer, column.Decimal)
	buffer = append(buffer, 0, 0)

	if withDefault {
		switch column.DefaultValue {
		case "CURRENT_TIMESTAMP", "CURRENT_DATE", nil:
			buffer = append(buffer, 251) // NULL
		default:
			defaultValStr := fmt.Sprintf("%v", column.DefaultValue)
			buffer = dump.LengthEncodedString(buffer, []byte(defaultValStr))
		}
	}

	return buffer
}

// DumpFlag dumps flag of a column.
func DumpFlag(tp byte, flag uint16) uint16 {
	switch tp {
	case mysql.TypeSet:
		return flag | uint16(mysql.SetFlag)
	case mysql.TypeEnum:
		return flag | uint16(mysql.EnumFlag)
	default:
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
