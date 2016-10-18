// Copyright 2016 PingCAP, Inc.
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

package types

import (
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

var (
	// ErrDataTooLong is returned when converts a string value that is longer than field type length.
	ErrDataTooLong *terror.Error = terror.ClassTypes.New(codeDataTooLong, "Data Too Long")
)

const (
	codeDataTooLong terror.ErrCode = terror.ErrCode(mysql.ErrDataTooLong)
)

func init() {
	typesMySQLErrCodes := map[terror.ErrCode]uint16{
		codeDataTooLong: mysql.ErrDataTooLong,
	}
	terror.ErrClassToMySQLCodes[terror.ClassTypes] = typesMySQLErrCodes
}
