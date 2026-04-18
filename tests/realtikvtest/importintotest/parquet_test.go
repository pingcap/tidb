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

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

//go:embed part0.parquet
var part0Content []byte

//go:embed part1.parquet
var part1Content []byte

//go:embed spark-legacy-date.gz.parquet
var sparkLegacyDateContent []byte

func (s *mockGCSSuite) TestImportParquet() {
	// Each file contains 10 rows, we manually set the row count to 5 when we skip reading the file.
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/lightning/mydump/mockParquetRowCount", "return(5)")

	tempDir := s.T().TempDir()
	s.NoError(os.WriteFile(path.Join(tempDir, "test.0.parquet"), part0Content, 0o644))
	s.NoError(os.WriteFile(path.Join(tempDir, "test.1.parquet"), part1Content, 0o644))
	importPath := path.Join(tempDir, "*.parquet")

	type testCase struct {
		createSQL      string
		importSQL      string
		readSQL        string
		checkCountOnly bool
	}

	testCases := []testCase{
		{
			createSQL: "CREATE TABLE test.sbtest(id bigint NOT NULL PRIMARY KEY, k bigint NOT NULL, c char(16), pad char(16))",
			importSQL: "IMPORT INTO test.sbtest FROM '%s' FORMAT 'parquet'",
			// The ID of data generated is starting from 0, plus 1 to make it same as _tidb_rowid
			readSQL:        "SELECT id + 1 FROM test.sbtest ORDER BY id",
			checkCountOnly: false,
		},
		{
			createSQL:      "CREATE TABLE test.sbtest(id bigint NOT NULL, k bigint NOT NULL, c char(16), pad char(16))",
			importSQL:      "IMPORT INTO test.sbtest FROM '%s' FORMAT 'parquet'",
			readSQL:        "SELECT _tidb_rowid FROM test.sbtest",
			checkCountOnly: false,
		},
		{
			// AUTO_INCREMENT primary key, ID generated might not start from 1 in this case, just check the count
			createSQL:      "CREATE TABLE test.sbtest(id bigint NOT NULL PRIMARY KEY AUTO_INCREMENT, k bigint NOT NULL, c char(16), pad char(16))",
			importSQL:      "IMPORT INTO test.sbtest(@1, k, c, pad) FROM '%s' FORMAT 'parquet'",
			readSQL:        "SELECT id FROM test.sbtest",
			checkCountOnly: true,
		},
		{
			// AUTO_RANDOM primary key
			createSQL:      "CREATE TABLE test.sbtest(id bigint NOT NULL PRIMARY KEY AUTO_RANDOM, k bigint NOT NULL, c char(16), pad char(16))",
			importSQL:      "IMPORT INTO test.sbtest(@1, k, c, pad) FROM '%s' FORMAT 'parquet'",
			readSQL:        "SELECT id FROM test.sbtest",
			checkCountOnly: true,
		},
		{
			// Non-clustered primary key
			createSQL:      "CREATE TABLE test.sbtest(id bigint NOT NULL PRIMARY KEY NONCLUSTERED, k bigint NOT NULL, c char(16), pad char(16))",
			importSQL:      "IMPORT INTO test.sbtest(id, k, c, pad) FROM '%s' FORMAT 'parquet'",
			readSQL:        "SELECT _tidb_rowid FROM test.sbtest",
			checkCountOnly: false,
		},
	}

	s.tk.MustExec("USE test;")
	for _, tc := range testCases {
		s.tk.MustExec("DROP TABLE IF EXISTS sbtest;")
		s.tk.MustExec(tc.createSQL)
		s.tk.MustQuery(fmt.Sprintf(tc.importSQL, importPath))

		rs := s.tk.MustQuery(tc.readSQL).Rows()
		require.Len(s.T(), rs, 20)
		if !tc.checkCountOnly {
			for i := range 20 {
				s.EqualValues(fmt.Sprintf("%d", i+1), rs[i][0])
			}
		}
	}
}

func (s *mockGCSSuite) TestImportParquetWithSparkLegacyDates() {
	tempDir := s.T().TempDir()
	// this parquet has below spark metadata:
	// org.apache.spark.timeZone UTC
	// org.apache.spark.legacyDateTime
	importPath := path.Join(tempDir, "spark-legacy-date.gz.parquet")
	s.NoError(os.WriteFile(importPath, sparkLegacyDateContent, 0o644))

	s.tk.MustExec("USE test;")
	s.tk.MustExec("DROP TABLE IF EXISTS t;")
	s.tk.MustExec("CREATE TABLE t (d DATE NOT NULL);")
	s.tk.MustQuery(fmt.Sprintf("IMPORT INTO test.t FROM '%s' FORMAT 'parquet'", importPath))
	s.tk.MustQuery("SELECT d FROM test.t ORDER BY d").Check(testkit.Rows(
		"0001-01-01", "0100-02-28", "0100-03-01", "0200-02-28", "0200-03-01",
		"0300-03-01", "0300-03-02", "0500-03-02", "0500-03-03", "0600-03-03",
		"0600-03-04", "0700-03-04", "0700-03-05", "0900-03-05", "0900-03-06",
		"1000-03-06", "1000-03-07", "1100-03-07", "1100-03-08", "1300-03-08",
		"1300-03-09", "1400-03-09", "1400-03-10", "1500-03-10", "1500-03-11",
		"1582-10-04", "1582-10-15", "1582-10-16", "1700-03-01", "1900-01-01",
		"1970-01-01", "2000-02-29", "9999-12-31",
	))
}
