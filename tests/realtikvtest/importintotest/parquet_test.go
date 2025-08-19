// Copyright 2023 PingCAP, Inc.
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

package importintotest

import (
	_ "embed"
	"fmt"
	"os"
	"path"

	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

//go:embed part0.parquet
var part0Content []byte

//go:embed part1.parquet
var part1Content []byte

func (s *mockGCSSuite) TestImportParquetWithClusteredIndex() {
	// Each file contains 50 rows, we manually set the row count to 10 when we skip reading the file.
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/lightning/mydump/mockParquetRowCount", "return(10)")

	tempDir := s.T().TempDir()
	s.NoError(os.WriteFile(path.Join(tempDir, "test.0.parquet"), part0Content, 0o644))
	s.NoError(os.WriteFile(path.Join(tempDir, "test.1.parquet"), part1Content, 0o644))
	importPath := path.Join(tempDir, "*.parquet")

	type testCase struct {
		createSQL string
		importSQL string
		readSQL   string
	}

	testCases := []testCase{
		{
			createSQL: "CREATE TABLE test.sbtest(id bigint NOT NULL PRIMARY KEY, k bigint NOT NULL, c char(16), pad char(16))",
			importSQL: "IMPORT INTO test.sbtest FROM '%s' FORMAT 'parquet'",
			readSQL:   "SELECT id FROM test.sbtest ORDER BY id",
		},
		{
			createSQL: "CREATE TABLE test.sbtest(id bigint NOT NULL, k bigint NOT NULL, c char(16), pad char(16))",
			importSQL: "IMPORT INTO test.sbtest FROM '%s' FORMAT 'parquet'",
			readSQL:   "SELECT _tidb_rowid FROM test.sbtest",
		},
		{
			createSQL: "CREATE TABLE test.sbtest(id bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, k bigint NOT NULL, c char(16), pad char(16))",
			importSQL: "IMPORT INTO test.sbtest(@1, k, c, pad) FROM '%s' FORMAT 'parquet'",
			readSQL:   "SELECT id FROM test.sbtest",
		},
	}

	s.tk.MustExec("USE test;")
	for _, tc := range testCases {
		s.tk.MustExec("DROP TABLE IF EXISTS sbtest;")
		s.tk.MustExec(tc.createSQL)
		s.tk.MustQuery(fmt.Sprintf(tc.importSQL, importPath))

		rs := s.tk.MustQuery(tc.readSQL).Rows()
		require.Len(s.T(), rs, 100)
		for i := range 100 {
			s.EqualValues(fmt.Sprintf("%d", i+1), rs[i][0])
		}
	}
}
