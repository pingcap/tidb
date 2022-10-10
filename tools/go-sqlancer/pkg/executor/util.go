// Copyright 2022 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
	"go.uber.org/zap"
)

// BufferOut parser ast node to SQL string
func BufferOut(node ast.Node) (string, error) {
	out := new(bytes.Buffer)
	err := node.Restore(format.NewRestoreCtx(format.RestoreStringDoubleQuotes, out))
	if err != nil {
		return "", err
	}
	return out.String(), nil
}

// TimeMustParse wrap time.Parse and panic when error
func TimeMustParse(layout, value string) time.Time {
	t, err := time.Parse(layout, value)
	if err != nil {
		log.Fatal("parse time err", zap.Error(err), zap.String("layout", layout), zap.String("value", value))
	}
	return t
}

// RdType rand data type
func RdType() string {
	switch util.Rd(6) {
	case 0:
		return "varchar"
	case 1:
		return "text"
	case 2:
		return "timestamp"
	case 3:
		return "datetime"
	}
	return "int"
}

// RdDataLen rand data with given type
func RdDataLen(t string) int {
	switch t {
	case "int":
		return int(util.RdRange(1, 20))
	case "varchar":
		return int(util.RdRange(1, 2047))
	case "float":
		return int(util.RdRange(16, 64))
	case "timestamp":
		return -1
	case "datetime":
		return -1
	case "text":
		return -1
	}
	return 10
}

// RdColumnOptions for rand column option with given type
func RdColumnOptions(t string) (options []ast.ColumnOptionType) {
	if util.Rd(3) == 0 {
		options = append(options, ast.ColumnOptionNotNull)
	} else if util.Rd(2) == 0 {
		options = append(options, ast.ColumnOptionNull)
	}
	switch t {
	case "varchar", "timestamp", "datetime", "int":
		if util.Rd(2) == 0 {
			options = append(options, ast.ColumnOptionDefaultValue)
		}
	}
	return
}

// RdCharset rand charset
func RdCharset() string {
	return "utf8"
}

// RdBool ...
func RdBool() bool {
	return util.Rd(2) == 0
}

// DataType2Len is to convert data type into length
func DataType2Len(t string) int {
	switch t {
	case "int":
		return 16
	case "bigint":
		return 64
	case "varchar":
		return 511
	case "timestamp":
		return 255
	case "datetime":
		return 255
	case "text":
		return 511
	case "float":
		return 53
	}
	return 16
}
