// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// ConvertColumnInfo converts `*ast.ResultField` to `*Info`
func ConvertColumnInfo(fld *ast.ResultField) (ci *Info) {
	ci = &Info{
		Name:         fld.ColumnAsName.O,
		OrgName:      fld.Column.Name.O,
		Table:        fld.TableAsName.O,
		Schema:       fld.DBName.O,
		Flag:         uint16(fld.Column.GetFlag()),
		Charset:      uint16(mysql.CharsetNameToID(fld.Column.GetCharset())),
		Type:         fld.Column.GetType(),
		DefaultValue: fld.Column.GetDefaultValue(),
	}

	if fld.EmptyOrgName {
		ci.OrgName = ""
	}
	if fld.Table != nil {
		ci.OrgTable = fld.Table.Name.O
	}
	if fld.Column.GetFlen() != types.UnspecifiedLength {
		ci.ColumnLength = uint32(fld.Column.GetFlen())
	}
	if fld.Column.GetType() == mysql.TypeNewDecimal {
		// Consider the negative sign.
		ci.ColumnLength++
		if fld.Column.GetDecimal() > types.DefaultFsp {
			// Consider the decimal point.
			ci.ColumnLength++
		}
	} else if types.IsString(fld.Column.GetType()) ||
		fld.Column.GetType() == mysql.TypeEnum || fld.Column.GetType() == mysql.TypeSet { // issue #18870
		// Fix issue #4540.
		// The flen is a hint, not a precise value, so most client will not use the value.
		// But we found in rare MySQL client, like Navicat for MySQL(version before 12) will truncate
		// the `show create table` result. To fix this case, we must use a large enough flen to prevent
		// the truncation, in MySQL, it will multiply bytes length by a multiple based on character set.
		// For examples:
		// * latin, the multiple is 1
		// * gb2312, the multiple is 2
		// * Utf-8, the multiple is 3
		// * utf8mb4, the multiple is 4
		// We used to check non-string types to avoid the truncation problem in some MySQL
		// client such as Navicat. Now we only allow string type enter this branch.
		charsetDesc, err := charset.GetCharsetInfo(fld.Column.GetCharset())
		if err != nil {
			ci.ColumnLength *= 4
		} else {
			ci.ColumnLength *= uint32(charsetDesc.Maxlen)
		}
	}

	if fld.Column.GetDecimal() == types.UnspecifiedLength {
		if fld.Column.GetType() == mysql.TypeDuration {
			ci.Decimal = uint8(types.DefaultFsp)
		} else {
			ci.Decimal = mysql.NotFixedDec
		}
	} else {
		ci.Decimal = uint8(fld.Column.GetDecimal())
	}

	// Keep things compatible for old clients.
	// Refer to mysql-server/sql/protocol.cc send_result_set_metadata()
	if ci.Type == mysql.TypeVarchar {
		ci.Type = mysql.TypeVarString
	}
	return
}
