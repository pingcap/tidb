// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"encoding/binary"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

const (
	// spfreshMaxStoredValueBytes is the fixed cross-repository encoded datum
	// limit for one stored column value, excluding the column-id datum.
	spfreshMaxStoredValueBytes = 64 * 1024
	// spfreshMaxStoredPayloadBytes is the fixed cross-repository encoded
	// sidecar limit for one leaf row, including column-id datums.
	spfreshMaxStoredPayloadBytes = 256 * 1024
)

func validateSPFreshStoredColumnDDL(colInfo *model.ColumnInfo) (int, error) {
	if types.IsTypeBlob(colInfo.FieldType.GetType()) {
		return 0, dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(
			fmt.Sprintf("SPFresh stored column %s cannot be BLOB or TEXT", colInfo.Name.O))
	}
	if colInfo.FieldType.GetType() == mysql.TypeJSON {
		return 0, dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(
			fmt.Sprintf("SPFresh stored column %s cannot be JSON", colInfo.Name.O))
	}

	valueBytes, err := spfreshWorstEncodedStoredValueBytes(&colInfo.FieldType)
	if err != nil {
		return 0, errors.Annotatef(err, "check SPFresh stored column %s size", colInfo.Name.O)
	}
	if valueBytes > spfreshMaxStoredValueBytes {
		return 0, dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(
			fmt.Sprintf("SPFresh stored column %s worst-case encoded value size %d exceeds limit %d",
				colInfo.Name.O, valueBytes, spfreshMaxStoredValueBytes))
	}
	return valueBytes, nil
}

func spfreshWorstEncodedStoredValueBytes(ft *types.FieldType) (int, error) {
	if ft == nil {
		return 0, errors.New("field type is nil")
	}

	switch ft.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeYear, mysql.TypeEnum, mysql.TypeBit:
		return 1 + binary.MaxVarintLen64, nil
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeDate,
		mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDuration:
		return 1 + 8, nil
	case mysql.TypeNewDecimal:
		return 1 + types.MyDecimalStructSize, nil
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString:
		valueBytes, err := spfreshWorstStringValueBytes(ft)
		if err != nil {
			return 0, err
		}
		varintBytes := spfreshEncodedVarintBytes(valueBytes)
		return 1 + varintBytes + valueBytes, nil
	default:
		return 0, dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(
			fmt.Sprintf("SPFresh stored column type %s is unsupported", types.TypeToStr(ft.GetType(), ft.GetCharset())))
	}
}

func spfreshWorstStringValueBytes(ft *types.FieldType) (int, error) {
	flen := ft.GetFlen()
	if flen == types.UnspecifiedLength || flen < 0 {
		return 0, dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(
			"SPFresh stored string column must have a bounded length")
	}
	charsetName := ft.GetCharset()
	if charsetName == "" {
		charsetName, _ = types.DefaultCharsetForType(ft.GetType())
	}
	desc, err := charset.GetCharsetInfo(charsetName)
	if err != nil {
		return 0, errors.Trace(err)
	}
	valueBytes, err := types.MulInt64(int64(flen), int64(desc.Maxlen))
	if err != nil {
		return 0, errors.Errorf("SPFresh stored string length overflow: flen=%d maxlen=%d", flen, desc.Maxlen)
	}
	return int(valueBytes), nil
}

func spfreshEncodedVarintBytes(value int) int {
	var data [binary.MaxVarintLen64]byte
	return binary.PutVarint(data[:], int64(value))
}
