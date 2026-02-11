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

package mydump

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func newParquetTargetColumnInfo(tp byte, flag uint, flen int, decimal int, charset string, collate string) *model.ColumnInfo {
	col := &model.ColumnInfo{}
	col.SetType(tp)
	col.SetFlag(flag)
	col.SetFlen(flen)
	col.SetDecimal(decimal)
	col.SetCharset(charset)
	col.SetCollate(collate)
	return col
}

func TestParquetSkipCastTimestampAlwaysCast(t *testing.T) {
	infos := buildParquetSkipCastInfos(
		[]convertedType{{converted: schema.ConvertedTypes.TimestampMicros, IsAdjustedToUTC: true}},
		[]parquet.Type{parquet.Types.Int64},
		[]*model.ColumnInfo{newParquetTargetColumnInfo(mysql.TypeTimestamp, 0, 19, 0, "", "")},
	)
	require.Len(t, infos, 1)
	require.False(t, infos[0].CanSkip)
	require.Equal(t, ParquetPostCheckNone, infos[0].PostCheck)
}

func TestParquetTemporalSetterUsesTargetType(t *testing.T) {
	converted := convertedType{converted: schema.ConvertedTypes.TimeMicros, IsAdjustedToUTC: true}
	target := newParquetTargetColumnInfo(mysql.TypeDate, 0, 0, 0, "", "")

	setter := getInt64Setter(&converted, time.UTC, target)
	var datum types.Datum
	require.NoError(t, setter(0, &datum))
	require.Equal(t, types.KindMysqlTime, datum.Kind())
	require.Equal(t, mysql.TypeDate, datum.GetMysqlTime().Type())
}

func TestParquetSkipCastInfoForStringAndDecimal(t *testing.T) {
	t.Run("float and double", func(t *testing.T) {
		infos := buildParquetSkipCastInfos(
			[]convertedType{{converted: schema.ConvertedTypes.None}, {converted: schema.ConvertedTypes.None}},
			[]parquet.Type{parquet.Types.Float, parquet.Types.Double},
			[]*model.ColumnInfo{
				newParquetTargetColumnInfo(mysql.TypeFloat, 0, 0, 0, "", ""),
				newParquetTargetColumnInfo(mysql.TypeDouble, 0, 0, 0, "", ""),
			},
		)
		require.True(t, infos[0].CanSkip)
		require.True(t, infos[1].CanSkip)
	})

	t.Run("utf8 string target", func(t *testing.T) {
		infos := buildParquetSkipCastInfos(
			[]convertedType{{converted: schema.ConvertedTypes.UTF8}},
			[]parquet.Type{parquet.Types.ByteArray},
			[]*model.ColumnInfo{
				newParquetTargetColumnInfo(mysql.TypeVarchar, 0, 20, 0, "utf8mb4", ""),
			},
		)
		require.True(t, infos[0].CanSkip)
		require.Equal(t, ParquetPostCheckStringLength, infos[0].PostCheck)
	})

	t.Run("binary charset string target", func(t *testing.T) {
		infos := buildParquetSkipCastInfos(
			[]convertedType{{converted: schema.ConvertedTypes.UTF8}},
			[]parquet.Type{parquet.Types.ByteArray},
			[]*model.ColumnInfo{
				newParquetTargetColumnInfo(mysql.TypeVarchar, 0, 20, 0, "binary", ""),
			},
		)
		require.False(t, infos[0].CanSkip)
	})

	t.Run("decimal byte array", func(t *testing.T) {
		infos := buildParquetSkipCastInfos(
			[]convertedType{{
				converted: schema.ConvertedTypes.Decimal,
				decimalMeta: schema.DecimalMetadata{
					IsSet:     true,
					Precision: 5,
					Scale:     2,
				},
			}},
			[]parquet.Type{parquet.Types.ByteArray},
			[]*model.ColumnInfo{
				newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 8, 2, "binary", "binary"),
			},
		)
		require.True(t, infos[0].CanSkip)
		require.Equal(t, ParquetPostCheckDecimal, infos[0].PostCheck)
	})

	t.Run("decimal scale mismatch", func(t *testing.T) {
		infos := buildParquetSkipCastInfos(
			[]convertedType{{
				converted: schema.ConvertedTypes.Decimal,
				decimalMeta: schema.DecimalMetadata{
					IsSet:     true,
					Precision: 5,
					Scale:     3,
				},
			}},
			[]parquet.Type{parquet.Types.FixedLenByteArray},
			[]*model.ColumnInfo{
				newParquetTargetColumnInfo(mysql.TypeNewDecimal, 0, 8, 2, "", ""),
			},
		)
		require.False(t, infos[0].CanSkip)
	})
}
