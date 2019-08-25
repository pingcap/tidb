// Copyright 2019 PingCAP, Inc.
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

package rowcodec

import (
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
)

// Decoder decodes the row to chunk.Chunk.
type Decoder struct {
	row
	requestColIDs []int64
	handleColID   int64
	requestTypes  []*types.FieldType
	origDefaults  [][]byte
	loc           *time.Location
}

// NewDecoder creates a NewDecoder.
// requestColIDs is the columnIDs to decode. tps is the field types for request columns.
// origDefault is the original default value in old format, if the column ID is not found in the row,
// the origDefault will be used.
func NewDecoder(requestColIDs []int64, handleColID int64, tps []*types.FieldType, origDefaults [][]byte,
	sc *stmtctx.StatementContext) (*Decoder, error) {
	xOrigDefaultVals := make([][]byte, len(origDefaults))
	for i := 0; i < len(origDefaults); i++ {
		if len(origDefaults[i]) == 0 {
			continue
		}
		xDefaultVal, err := convertDefaultValue(origDefaults[i], sc)
		if err != nil {
			return nil, err
		}
		xOrigDefaultVals[i] = xDefaultVal
	}
	return &Decoder{
		requestColIDs: requestColIDs,
		handleColID:   handleColID,
		requestTypes:  tps,
		origDefaults:  xOrigDefaultVals,
		loc:           sc.TimeZone,
	}, nil
}

func convertDefaultValue(defaultVal []byte, sc *stmtctx.StatementContext) (colVal []byte, err error) {
	var d types.Datum
	_, d, err = codec.DecodeOne(defaultVal)
	if err != nil {
		return
	}
	return encodeDatum(nil, d, sc)
}

// Decode decodes a row to chunk.
func (decoder *Decoder) Decode(rowData []byte, handle int64, chk *chunk.Chunk) error {
	err := decoder.setRowData(rowData)
	if err != nil {
		return err
	}
	for colIdx, colID := range decoder.requestColIDs {
		if colID == decoder.handleColID {
			chk.AppendInt64(colIdx, handle)
			continue
		}
		// Search the column in not-null columns array.
		i, j := 0, int(decoder.numNotNullCols)
		var found bool
		for i < j {
			h := int(uint(i+j) >> 1) // avoid overflow when computing h
			// i ≤ h < j
			var v int64
			if decoder.isLarge {
				v = int64(decoder.colIDs32[h])
			} else {
				v = int64(decoder.colIDs[h])
			}
			if v < colID {
				i = h + 1
			} else if v > colID {
				j = h
			} else {
				found = true
				colData := decoder.getData(h)
				err := decoder.decodeColData(colIdx, colData, chk)
				if err != nil {
					return err
				}
				break
			}
		}
		if found {
			continue
		}
		// Search the column in null columns array.
		i, j = int(decoder.numNotNullCols), int(decoder.numNotNullCols+decoder.numNullCols)
		for i < j {
			h := int(uint(i+j) >> 1) // avoid overflow when computing h
			// i ≤ h < j
			var v int64
			if decoder.isLarge {
				v = int64(decoder.colIDs32[h])
			} else {
				v = int64(decoder.colIDs[h])
			}
			if v < colID {
				i = h + 1
			} else if v > colID {
				j = h
			} else {
				found = true
				break
			}
		}
		if found || decoder.origDefaults[colIdx] == nil {
			chk.AppendNull(colIdx)
		} else {
			err := decoder.decodeColData(colIdx, decoder.origDefaults[colIdx], chk)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (decoder *Decoder) decodeColData(colIdx int, colData []byte, chk *chunk.Chunk) error {
	ft := decoder.requestTypes[colIdx]
	switch ft.Tp {
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny, mysql.TypeYear:
		if mysql.HasUnsignedFlag(ft.Flag) {
			chk.AppendUint64(colIdx, decodeUint(colData))
		} else {
			chk.AppendInt64(colIdx, decodeInt(colData))
		}
	case mysql.TypeFloat:
		chk.AppendFloat32(colIdx, float32(math.Float64frombits(decodeUint(colData))))
	case mysql.TypeDouble:
		chk.AppendFloat64(colIdx, math.Float64frombits(decodeUint(colData)))
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		chk.AppendBytes(colIdx, colData)
	case mysql.TypeNewDecimal:
		_, dec, _, _, err := codec.DecodeDecimal(colData)
		if err != nil {
			return err
		}
		chk.AppendMyDecimal(colIdx, dec)
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		var t types.Time
		t.Type = ft.Tp
		t.Fsp = int8(ft.Decimal)
		err := t.FromPackedUint(decodeUint(colData))
		if err != nil {
			return err
		}
		if ft.Tp == mysql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, decoder.loc)
			if err != nil {
				return err
			}
		}
		chk.AppendTime(colIdx, t)
	case mysql.TypeDuration:
		var dur types.Duration
		dur.Duration = time.Duration(decodeInt(colData))
		dur.Fsp = int8(ft.Decimal)
		chk.AppendDuration(colIdx, dur)
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(ft.Elems, decodeUint(colData))
		if err != nil {
			enum = types.Enum{}
		}
		chk.AppendEnum(colIdx, enum)
	case mysql.TypeSet:
		set, err := types.ParseSetValue(ft.Elems, decodeUint(colData))
		if err != nil {
			return err
		}
		chk.AppendSet(colIdx, set)
	case mysql.TypeBit:
		byteSize := (ft.Flen + 7) >> 3
		chk.AppendBytes(colIdx, types.NewBinaryLiteralFromUint(decodeUint(colData), byteSize))
	case mysql.TypeJSON:
		var j json.BinaryJSON
		j.TypeCode = colData[0]
		j.Value = colData[1:]
		chk.AppendJSON(colIdx, j)
	default:
		return errors.Errorf("unknown type %d", ft.Tp)
	}
	return nil
}
