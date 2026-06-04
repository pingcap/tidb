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
	"github.com/pingcap/tidb/pkg/format/textrow"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// ResultEncoder encodes a column value to a byte slice. The implementation lives
// in pkg/format/textrow so it can be shared with the distributed exporter.
type ResultEncoder = textrow.ResultEncoder

// NewResultEncoder creates a new ResultEncoder.
var NewResultEncoder = textrow.NewResultEncoder

// columnTypeInfoCharsetID returns the charset ID for the column type info. It
// stays in the server package because it depends on the server-only Info type.
func columnTypeInfoCharsetID(d *ResultEncoder, info *Info) uint16 {
	// Only replace the charset when @@character_set_results is valid and
	// the target column is a non-binary string.
	cs := info.dumpCharset()
	if d.IsResultCharsetNull() || !isStringColumnType(info.Type) {
		return cs
	}
	if cs == mysql.BinaryDefaultCollationID {
		return mysql.BinaryDefaultCollationID
	}
	return uint16(mysql.CharsetNameToID(d.ResultCharsetName()))
}

func isStringColumnType(tp byte) bool {
	switch tp {
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeBit,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob,
		mysql.TypeEnum, mysql.TypeSet, mysql.TypeJSON:
		return true
	case mysql.TypeTiDBVectorFloat32:
		// When passing Vector column to the SQL Client, pretend to be a non-binary String.
		return true
	}
	return false
}
