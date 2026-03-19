// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/table/tables"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func createDataFiles(t *testing.T, dir string, fileCount, rowsPerFile, rowLen int) {
	t.Helper()
	require.NoError(t, os.Mkdir(dir, 0o755))
	padLen := rowLen - 4
	require.GreaterOrEqual(t, padLen, 3)
	padding := strings.Repeat("a", padLen/3)
	var rowSB strings.Builder
	rowSB.WriteString("1111")
	for range 3 {
		rowSB.WriteString(",")
		rowSB.WriteString(padding)
	}
	rowSB.WriteString("\n")
	rowData := rowSB.String()
	var fileSB strings.Builder
	for j := 0; j < rowsPerFile; j++ {
		fileSB.WriteString(rowData)
	}
	for i := 0; i < fileCount; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%03d.csv", i)), []byte(fileSB.String()), 0o644))
	}
}

type caseTp struct {
	fileCount, rowsPerFile, rowLen int
	ksCodec                        []byte
	sql                            string
	ratio                          float64
}

func runCaseFn(t *testing.T, i int, c caseTp) {
	dir := filepath.Join(t.TempDir(), fmt.Sprintf("case-%d", i))
	createDataFiles(t, dir, c.fileCount, c.rowsPerFile, c.rowLen)
	p := parser.New()
	node, err := p.ParseOneStmt(c.sql, "", "")
	require.NoError(t, err)
	sctx := utilmock.NewContext()
	tblInfo, err := ddl.MockTableInfo(sctx, node.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	tblInfo.State = model.StatePublic
	table := tables.MockTableFromMeta(tblInfo)
	ctrl, err := NewLoadDataController(&Plan{
		Path:           filepath.Join(dir, "*.csv"),
		Format:         DataFormatCSV,
		LineFieldsInfo: newDefaultLineFieldsInfo(),
		InImportInto:   true,
	}, table, &ASTArgs{})
	require.NoError(t, err)
	ctrl.logger = zap.Must(zap.NewDevelopment())
	ctx := context.Background()
	require.NoError(t, ctrl.InitDataFiles(ctx))
	ratio, err := ctrl.sampleIndexSizeRatio(ctx, c.ksCodec)
	require.NoError(t, err)
	require.InDelta(t, c.ratio, ratio, 0.001)
}

func TestSampleIndexSizeRatio(t *testing.T) {
	ksCodec := []byte{'x', 0x00, 0x00, 0x01}
	simpleTbl := `create table t (a int, b text, c text, d text, index idx(a));`
	cases := []caseTp{
		// without ks codec
		// no file
		{0, 20, 100, nil, simpleTbl, 0},
		// < 3 files
		{1, 20, 100, nil, simpleTbl, 0.287},
		{2, 20, 100, nil, simpleTbl, 0.287},
		// < 3 files, not enough rows
		{2, 8, 100, nil, simpleTbl, 0.287},
		// enough files
		{10, 20, 100, nil, simpleTbl, 0.287},
		{10, 20, 100, nil,
			`create table t (a int, b text, c text, d text, index idx(b(1024)));`, 0.568},
		{10, 20, 100, nil,
			`create table t (a int, b text, c text, d text, index idx1(a), index idx2(a), index idx3(a), index idx4(a));`, 1.151},
		// enough files, not enough rows
		{10, 5, 100, nil, simpleTbl, 0.287},
		// longer rows
		{10, 20, 400, nil, simpleTbl, 0.087},
		{10, 12, 2000, nil, simpleTbl, 0.018},
		{10, 12, 2000, nil,
			`create table t (a int, b text, c text, d text, index idx1(a), index idx2(a), index idx3(a), index idx4(a));`, 0.074},

		// with ks codec
		{10, 20, 100, ksCodec, simpleTbl, 0.308},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			runCaseFn(t, i, c)
		})
	}
}
func TestSampleIndexSizeRatioVeryLongRows(t *testing.T) {
	simpleTbl := `create table t (a int, b text, c text, d text, index idx(a));`
	bak := maxSampleFileSize
	maxSampleFileSize = 2 * units.MiB
	t.Cleanup(func() {
		maxSampleFileSize = bak
	})
	// early return when reach maxSampleFileSize
	longRowCase := caseTp{10, 10, units.MiB + 100*units.KiB, nil, simpleTbl, 0}
	runCaseFn(t, -1, longRowCase)
}
