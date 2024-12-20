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

	"github.com/pingcap/tidb/pkg/parser/model"
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
	oldPlainCol.Name = model.NewCIStr("oldPlainCol")
	oldPlainCol.FieldType = *types.NewFieldType(mysql.TypeLong)
	oldPlainCol.DefaultValue = randPlainStr
	oldPlainCol.OriginDefaultValue = randPlainStr

	newPlainCol := srcCol.Clone()
	newPlainCol.Name = model.NewCIStr("newPlainCol")
	newPlainCol.FieldType = *types.NewFieldType(mysql.TypeLong)
	err := newPlainCol.SetDefaultValue(1)
	require.NoError(t, err)
	require.Equal(t, 1, newPlainCol.GetDefaultValue())
	err = newPlainCol.SetDefaultValue(randPlainStr)
	require.NoError(t, err)
	require.Equal(t, randPlainStr, newPlainCol.GetDefaultValue())

	randBitStr := string([]byte{25, 185})

	oldBitCol := srcCol.Clone()
	oldBitCol.Name = model.NewCIStr("oldBitCol")
	oldBitCol.FieldType = *types.NewFieldType(mysql.TypeBit)
	oldBitCol.DefaultValue = randBitStr
	oldBitCol.OriginDefaultValue = randBitStr

	newBitCol := srcCol.Clone()
	newBitCol.Name = model.NewCIStr("newBitCol")
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
	nullBitCol.Name = model.NewCIStr("nullBitCol")
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
