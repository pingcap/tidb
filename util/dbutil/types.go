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

package dbutil

import (
	"github.com/pingcap/tidb/parser/mysql"
)

// IsNumberType returns true if tp is number type
func IsNumberType(tp byte) bool {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeYear:
		return true
	}

	return false
}

// IsFloatType returns true if tp is float type
func IsFloatType(tp byte) bool {
	switch tp {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		return true
	}

	return false
}

// IsTimeTypeAndNeedDecode returns true if tp is time type and encoded in tidb buckets.
func IsTimeTypeAndNeedDecode(tp byte) bool {
	if tp == mysql.TypeDatetime || tp == mysql.TypeTimestamp || tp == mysql.TypeDate {
		return true
	}
	return false
}
