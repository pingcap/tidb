// Copyright 2021 PingCAP, Inc.
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

package statistics

import (
	"github.com/pingcap/tidb/session"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestEstimateTypeWidth(t *testing.T) {
	t.Parallel()

	var colType *types.FieldType

	colType = &types.FieldType{Tp: mysql.TypeLonglong}
	require.Equal(t, 8, EstimateTypeWidthForChunk(colType)) // fixed-witch type

	colType = &types.FieldType{Tp: mysql.TypeString, Flen: 31}
	require.Equal(t, 31, EstimateTypeWidthForChunk(colType)) // colLen <= 32

	colType = &types.FieldType{Tp: mysql.TypeString, Flen: 999}
	require.Equal(t, 515, EstimateTypeWidthForChunk(colType)) // colLen < 1000

	colType = &types.FieldType{Tp: mysql.TypeString, Flen: 2000}
	require.Equal(t, 516, EstimateTypeWidthForChunk(colType)) // colLen < 1000

	colType = &types.FieldType{Tp: mysql.TypeString}
	require.Equal(t, 32, EstimateTypeWidthForChunk(colType)) // value after guessing
}

func TestGetAvgRowSizeWithoutColStats(t *testing.T) {
	t.Parallel()

	session.CreateSession4Test(nil)

}
