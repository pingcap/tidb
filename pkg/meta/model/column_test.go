// Copyright 2024 PingCAP, Inc.
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

package model

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/stretchr/testify/require"
)

func TestDefaultValue(t *testing.T) {
	srcCol := &ColumnInfo{
		ID: 1,
	}
	randPlainStr := "random_plain_string"

	oldPlainCol := srcCol.Clone()
	oldPlainCol.Name = ast.NewCIStr("oldPlainCol")
	oldPlainCol.FieldType = *types.NewFieldType(mysql.TypeLong)
	oldPlainCol.DefaultValue = randPlainStr
	oldPlainCol.OriginDefaultValue = randPlainStr

	newPlainCol := srcCol.Clone()
	newPlainCol.Name = ast.NewCIStr("newPlainCol")
	newPlainCol.FieldType = *types.NewFieldType(mysql.TypeLong)
	err := newPlainCol.SetDefaultValue(1)
	require.NoError(t, err)
	require.Equal(t, 1, newPlainCol.GetDefaultValue())
	err = newPlainCol.SetDefaultValue(randPlainStr)
	require.NoError(t, err)
	require.Equal(t, randPlainStr, newPlainCol.GetDefaultValue())

	randBitStr := string([]byte{25, 185})

	oldBitCol := srcCol.Clone()
	oldBitCol.Name = ast.NewCIStr("oldBitCol")
	oldBitCol.FieldType = *types.NewFieldType(mysql.TypeBit)
	oldBitCol.DefaultValue = randBitStr
	oldBitCol.OriginDefaultValue = randBitStr

	newBitCol := srcCol.Clone()
	newBitCol.Name = ast.NewCIStr("newBitCol")
	newBitCol.FieldType = *types.NewFieldType(mysql.TypeBit)
	err = newBitCol.SetDefaultValue(1)
	// Only string type is allowed in BIT column.
	require.Error(t, err)
	require.Contains(t, err.Error(), "Invalid default value")
	require.Equal(t, 1, newBitCol.GetDefaultValue())
	err = newBitCol.SetDefaultValue(randBitStr)
	require.NoError(t, err)
	require.Equal(t, randBitStr, newBitCol.GetDefaultValue())

	nullBitCol := srcCol.Clone()
	nullBitCol.Name = ast.NewCIStr("nullBitCol")
	nullBitCol.FieldType = *types.NewFieldType(mysql.TypeBit)
	err = nullBitCol.SetOriginDefaultValue(nil)
	require.NoError(t, err)
	require.Nil(t, nullBitCol.GetOriginDefaultValue())

	testCases := []struct {
		col          *ColumnInfo
		isConsistent bool
	}{
		{oldPlainCol, true},
		{oldBitCol, false},
		{newPlainCol, true},
		{newBitCol, true},
		{nullBitCol, true},
	}
	for _, tc := range testCases {
		col, isConsistent := tc.col, tc.isConsistent
		comment := fmt.Sprintf("%s assertion failed", col.Name.O)
		bytes, err := json.Marshal(col)
		require.NoError(t, err, comment)
		var newCol ColumnInfo
		err = json.Unmarshal(bytes, &newCol)
		require.NoError(t, err, comment)
		if isConsistent {
			require.Equal(t, col.GetDefaultValue(), newCol.GetDefaultValue(), comment)
			require.Equal(t, col.GetOriginDefaultValue(), newCol.GetOriginDefaultValue(), comment)
		} else {
			require.NotEqual(t, col.GetDefaultValue(), newCol.GetDefaultValue(), comment)
			require.NotEqual(t, col.GetOriginDefaultValue(), newCol.GetOriginDefaultValue(), comment)
		}
	}
	extraPhysTblIDCol := NewExtraPhysTblIDColInfo()
	require.Equal(t, mysql.NotNullFlag, extraPhysTblIDCol.GetFlag())
	require.Equal(t, mysql.TypeLonglong, extraPhysTblIDCol.GetType())
}

func TestBinaryDefaultValueJSONRoundTrip(t *testing.T) {
	raw := "\x00\xffA"
	for _, tp := range []byte{mysql.TypeVarchar, mysql.TypeString} {
		col := &ColumnInfo{ID: 1, Name: ast.NewCIStr("b")}
		col.FieldType = *types.NewFieldType(tp)
		col.SetCharset("binary")
		col.SetCollate("binary")
		require.NoError(t, col.SetDefaultValue(raw))
		require.NoError(t, col.SetOriginDefaultValue(raw))

		data, err := json.Marshal(col)
		require.NoError(t, err)
		var got ColumnInfo
		require.NoError(t, json.Unmarshal(data, &got))
		require.Equal(t, raw, got.GetDefaultValue())
		require.Equal(t, raw, got.GetOriginDefaultValue())
	}

	// A non-binary string column keeps the old behavior and stores no raw bytes.
	col := &ColumnInfo{ID: 2, Name: ast.NewCIStr("s")}
	col.FieldType = *types.NewFieldType(mysql.TypeVarchar)
	col.SetCharset("utf8mb4")
	col.SetCollate("utf8mb4_bin")
	require.NoError(t, col.SetDefaultValue("abc"))
	require.Nil(t, col.DefaultValueBit)

	// A nil or non-string default of a binary column is accepted as before.
	bin := &ColumnInfo{ID: 3, Name: ast.NewCIStr("n")}
	bin.FieldType = *types.NewFieldType(mysql.TypeVarchar)
	bin.SetCollate("binary")
	require.NoError(t, bin.SetDefaultValue(nil))
	require.NoError(t, bin.SetDefaultValue(1))
	require.Nil(t, bin.DefaultValueBit)

	// Replacing a raw-bytes default by nil or a non-string value drops the stale bytes.
	for _, v := range []any{nil, 1} {
		require.NoError(t, bin.SetDefaultValue(raw))
		require.NoError(t, bin.SetOriginDefaultValue(raw))
		require.NoError(t, bin.SetDefaultValue(v))
		require.NoError(t, bin.SetOriginDefaultValue(v))
		require.Equal(t, v, bin.GetDefaultValue())
		require.Equal(t, v, bin.GetOriginDefaultValue())
		require.Nil(t, bin.DefaultValueBit)
		require.Nil(t, bin.OriginDefaultValueBit)
	}
}
