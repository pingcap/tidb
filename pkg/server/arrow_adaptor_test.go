// Copyright 2025 PingCAP, Inc.
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

package server

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

func TestAdaptFieldType(t *testing.T) {
	tests := []struct {
		name     string
		mysqlTyp byte
		flags    uint
		expected arrow.DataType
		wantErr  bool
	}{
		{
			name:     "TinyInt signed",
			mysqlTyp: mysql.TypeTiny,
			flags:    0,
			expected: arrow.PrimitiveTypes.Int64,
			wantErr:  false,
		},
		{
			name:     "TinyInt unsigned",
			mysqlTyp: mysql.TypeTiny,
			flags:    mysql.UnsignedFlag,
			expected: arrow.PrimitiveTypes.Int64,
			wantErr:  false,
		},
		{
			name:     "SmallInt signed",
			mysqlTyp: mysql.TypeShort,
			flags:    0,
			expected: arrow.PrimitiveTypes.Int64,
			wantErr:  false,
		},
		{
			name:     "Int signed",
			mysqlTyp: mysql.TypeLong,
			flags:    0,
			expected: arrow.PrimitiveTypes.Int64,
			wantErr:  false,
		},
		{
			name:     "BigInt unsigned",
			mysqlTyp: mysql.TypeLonglong,
			flags:    mysql.UnsignedFlag,
			expected: arrow.PrimitiveTypes.Int64,
			wantErr:  false,
		},
		{
			name:     "Float",
			mysqlTyp: mysql.TypeFloat,
			flags:    0,
			expected: arrow.PrimitiveTypes.Float32,
			wantErr:  false,
		},
		{
			name:     "Double",
			mysqlTyp: mysql.TypeDouble,
			flags:    0,
			expected: arrow.PrimitiveTypes.Float64,
			wantErr:  false,
		},
		{
			name:     "Decimal",
			mysqlTyp: mysql.TypeNewDecimal,
			flags:    0,
			expected: arrow.PrimitiveTypes.Float64,
			wantErr:  false,
		},
		{
			name:     "Date",
			mysqlTyp: mysql.TypeDate,
			flags:    0,
			expected: arrow.FixedWidthTypes.Date64,
			wantErr:  false,
		},
		{
			name:     "Datetime",
			mysqlTyp: mysql.TypeDatetime,
			flags:    0,
			expected: arrow.FixedWidthTypes.Date64,
			wantErr:  false,
		},
		{
			name:     "Timestamp",
			mysqlTyp: mysql.TypeTimestamp,
			flags:    0,
			expected: arrow.FixedWidthTypes.Date64,
			wantErr:  false,
		},
		{
			name:     "Duration",
			mysqlTyp: mysql.TypeDuration,
			flags:    0,
			expected: arrow.PrimitiveTypes.Int64,
			wantErr:  false,
		},
		{
			name:     "Varchar",
			mysqlTyp: mysql.TypeVarchar,
			flags:    0,
			expected: arrow.BinaryTypes.String,
			wantErr:  false,
		},
		{
			name:     "VarString",
			mysqlTyp: mysql.TypeVarString,
			flags:    0,
			expected: arrow.BinaryTypes.String,
			wantErr:  false,
		},
		{
			name:     "String",
			mysqlTyp: mysql.TypeString,
			flags:    0,
			expected: arrow.BinaryTypes.String,
			wantErr:  false,
		},
		{
			name:     "TinyBlob",
			mysqlTyp: mysql.TypeTinyBlob,
			flags:    0,
			expected: arrow.BinaryTypes.String,
			wantErr:  false,
		},
		{
			name:     "Blob",
			mysqlTyp: mysql.TypeBlob,
			flags:    0,
			expected: arrow.BinaryTypes.String,
			wantErr:  false,
		},
		{
			name:     "MediumBlob",
			mysqlTyp: mysql.TypeMediumBlob,
			flags:    0,
			expected: arrow.BinaryTypes.String,
			wantErr:  false,
		},
		{
			name:     "LongBlob",
			mysqlTyp: mysql.TypeLongBlob,
			flags:    0,
			expected: arrow.BinaryTypes.String,
			wantErr:  false,
		},
		{
			name:     "JSON",
			mysqlTyp: mysql.TypeJSON,
			flags:    0,
			expected: arrow.BinaryTypes.String,
			wantErr:  false,
		},
		{
			name:     "Bit",
			mysqlTyp: mysql.TypeBit,
			flags:    0,
			expected: arrow.PrimitiveTypes.Int64,
			wantErr:  false,
		},
		{
			name:     "Enum",
			mysqlTyp: mysql.TypeEnum,
			flags:    0,
			expected: arrow.BinaryTypes.String,
			wantErr:  false,
		},
		{
			name:     "Set",
			mysqlTyp: mysql.TypeSet,
			flags:    0,
			expected: arrow.BinaryTypes.String,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ft := &types.FieldType{}
			ft.SetType(tt.mysqlTyp)
			ft.SetFlag(tt.flags)

			result, err := adaptFieldType(ft)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestAdaptFieldTypeUnsupported(t *testing.T) {
	// Test unsupported type (using an invalid type code)
	ft := &types.FieldType{}
	ft.SetType(mysql.TypeNull) // TypeNull should be unsupported

	result, err := adaptFieldType(ft)
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrUnsupportedType)
}

func TestResultSetRecordReaderLifecycle(t *testing.T) {
	// This test verifies proper lifecycle management
	// We can't easily create a real ResultSet without a full TiDB setup,
	// so we test the lifecycle methods with nil checks

	reader := &ResultSetRecordReader{}

	// Test Release with nil fields (should not panic)
	reader.Release()

	// Test Schema returns nil when not initialized
	require.Nil(t, reader.Schema())

	// Test Err returns nil initially
	require.NoError(t, reader.Err())
}

func TestErrUnsupportedType(t *testing.T) {
	// Verify the error is properly defined
	require.NotNil(t, ErrUnsupportedType)
	require.Contains(t, ErrUnsupportedType.Error(), "unsupported")
}
