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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBuildSPFreshStoredColumnIDsRejectsArrayColumn(t *testing.T) {
	arrayType := types.NewFieldType(mysql.TypeLonglong)
	arrayType.SetArray(true)
	tblInfo := &model.TableInfo{
		Name: pmodel.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{
				ID:        1,
				Name:      pmodel.NewCIStr("tags"),
				Offset:    0,
				FieldType: *arrayType,
				State:     model.StatePublic,
			},
		},
	}

	_, err := buildSPFreshStoredColumnIDs([]pmodel.CIStr{pmodel.NewCIStr("tags")}, tblInfo, 2)

	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot be an ARRAY column")
}

func TestBuildSPFreshStoredColumnIDsRejectsUnboundedStoredColumnTypes(t *testing.T) {
	cases := []struct {
		name    string
		tp      byte
		wantErr string
	}{
		{name: "blob", tp: mysql.TypeBlob, wantErr: "cannot be BLOB or TEXT"},
		{name: "text", tp: mysql.TypeLongBlob, wantErr: "cannot be BLOB or TEXT"},
		{name: "json", tp: mysql.TypeJSON, wantErr: "cannot be JSON"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tblInfo := &model.TableInfo{
				Name: pmodel.NewCIStr("t"),
				Columns: []*model.ColumnInfo{
					{
						ID:        1,
						Name:      pmodel.NewCIStr("filter_col"),
						Offset:    0,
						FieldType: *types.NewFieldType(tc.tp),
						State:     model.StatePublic,
					},
				},
			}

			_, err := buildSPFreshStoredColumnIDs([]pmodel.CIStr{pmodel.NewCIStr("filter_col")}, tblInfo, 2)

			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestBuildSPFreshStoredColumnIDsChecksWorstCaseStoredValueBoundary(t *testing.T) {
	cases := []struct {
		name      string
		charset   string
		collation string
		okFlen    int
		badFlen   int
	}{
		{
			name:      "binary",
			charset:   charset.CharsetBin,
			collation: charset.CharsetBin,
			okFlen:    spfreshMaxStoredValueBytes - 1 - 3,
			badFlen:   spfreshMaxStoredValueBytes - 1 - 3 + 1,
		},
		{
			name:      "utf8mb4",
			charset:   charset.CharsetUTF8MB4,
			collation: charset.CollationUTF8MB4,
			okFlen:    (spfreshMaxStoredValueBytes - 1 - 3) / 4,
			badFlen:   (spfreshMaxStoredValueBytes-1-3)/4 + 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			okType := boundedVarcharFieldType(tc.okFlen, tc.charset, tc.collation)
			okSize, err := spfreshWorstEncodedStoredValueBytes(okType)
			require.NoError(t, err)
			require.LessOrEqual(t, okSize, spfreshMaxStoredValueBytes)

			_, err = validateSPFreshStoredColumnDDL(&model.ColumnInfo{
				Name:      pmodel.NewCIStr("filter_col"),
				FieldType: *okType,
			})
			require.NoError(t, err)

			badType := boundedVarcharFieldType(tc.badFlen, tc.charset, tc.collation)
			badSize, err := spfreshWorstEncodedStoredValueBytes(badType)
			require.NoError(t, err)
			require.Greater(t, badSize, spfreshMaxStoredValueBytes)

			_, err = validateSPFreshStoredColumnDDL(&model.ColumnInfo{
				Name:      pmodel.NewCIStr("filter_col"),
				FieldType: *badType,
			})
			require.ErrorContains(t, err, "worst-case encoded value size")
		})
	}
}

func TestSPFreshStoredColumnDDLRejectsUnsupportedEvaluatorTypes(t *testing.T) {
	for _, tp := range []byte{mysql.TypeSet, mysql.TypeNewDate, mysql.TypeNull} {
		ft := types.NewFieldType(tp)
		_, err := validateSPFreshStoredColumnDDL(&model.ColumnInfo{
			Name:      pmodel.NewCIStr("filter_col"),
			FieldType: *ft,
		})
		require.ErrorContains(t, err, "unsupported")
	}
}

func TestBuildSPFreshStoredColumnIDsChecksSidecarBoundary(t *testing.T) {
	okType := boundedVarcharFieldType(spfreshMaxStoredValueBytes-1-3-2, charset.CharsetBin, charset.CharsetBin)
	okValueBytes, err := spfreshWorstEncodedStoredValueBytes(okType)
	require.NoError(t, err)
	require.Equal(t, spfreshMaxStoredValueBytes-2, okValueBytes)

	okTableInfo := newSPFreshStoredColumnBoundaryTableInfo(okType)
	_, err = buildSPFreshStoredColumnIDs(
		[]pmodel.CIStr{pmodel.NewCIStr("filter_1"), pmodel.NewCIStr("filter_2"), pmodel.NewCIStr("filter_3"), pmodel.NewCIStr("filter_4")},
		okTableInfo,
		100,
	)
	require.NoError(t, err)

	tooLargeType := boundedVarcharFieldType(spfreshMaxStoredValueBytes-1-3-1, charset.CharsetBin, charset.CharsetBin)
	tooLargeTableInfo := newSPFreshStoredColumnBoundaryTableInfo(tooLargeType)
	_, err = buildSPFreshStoredColumnIDs(
		[]pmodel.CIStr{pmodel.NewCIStr("filter_1"), pmodel.NewCIStr("filter_2"), pmodel.NewCIStr("filter_3"), pmodel.NewCIStr("filter_4")},
		tooLargeTableInfo,
		100,
	)
	require.ErrorContains(t, err, "worst-case encoded sidecar size")
}

func boundedVarcharFieldType(flen int, charsetName string, collation string) *types.FieldType {
	ft := types.NewFieldType(mysql.TypeVarchar)
	ft.SetFlen(flen)
	ft.SetCharset(charsetName)
	ft.SetCollate(collation)
	return ft
}

func newSPFreshStoredColumnBoundaryTableInfo(ft *types.FieldType) *model.TableInfo {
	columns := make([]*model.ColumnInfo, 4)
	for i := range columns {
		columns[i] = &model.ColumnInfo{
			ID:        int64(i + 1),
			Name:      pmodel.NewCIStr(fmt.Sprintf("filter_%d", i+1)),
			Offset:    i,
			FieldType: *ft,
			State:     model.StatePublic,
		}
	}
	return &model.TableInfo{
		Name:    pmodel.NewCIStr("t"),
		Columns: columns,
	}
}
