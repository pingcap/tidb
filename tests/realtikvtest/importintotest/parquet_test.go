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

func (s *mockGCSSuite) TestImportInvalidParquet() {
	tempDir := s.T().TempDir()

	s.tk.MustExec("CREATE TABLE IF NOT EXISTS test.t_invalid_parquet(id bigint NOT NULL PRIMARY KEY, val varchar(64))")
	s.T().Cleanup(func() {
		s.tk.MustExec("DROP TABLE IF EXISTS test.t_invalid_parquet")
	})

	// Build a corrupt parquet: valid PAR1 magic at end, plausible footer
	// length, but garbage Thrift metadata that cannot be deserialized.
	// Layout: [4-byte garbage body] [footer_len=4 LE] [PAR1]
	corruptParquet := []byte{
		0xDE, 0xAD, 0xBE, 0xEF, // garbage footer body (4 bytes)
		0x04, 0x00, 0x00, 0x00, // footer length = 4 (little-endian)
		'P', 'A', 'R', '1', // valid footer magic
	}
	// Build an encrypted parquet: ends with PARE magic.
	encryptedParquet := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x04, 0x00, 0x00, 0x00,
		'P', 'A', 'R', 'E',
	}

	cases := []struct {
		name    string
		content []byte
		errMsg  string
	}{
		{"csv content", []byte("1,\"hello\"\n2,\"world\"\n"), "not a valid Parquet file"},
		{"sql content", []byte("INSERT INTO t VALUES (1, 'hello'), (2, 'world');\n"), "not a valid Parquet file"},
		{"empty file", []byte{}, "not a valid Parquet file"},
		{"too small (< 12 bytes)", []byte("PAR1PAR1"), "not a valid Parquet file"},
		{"corrupt metadata", corruptParquet, "Parquet file is corrupt"},
		{"encrypted parquet", encryptedParquet, "encrypted Parquet is not supported"},
	}
	for _, tc := range cases {
		filePath := path.Join(tempDir, tc.name+".parquet")
		s.NoError(os.WriteFile(filePath, tc.content, 0o644))
		sql := fmt.Sprintf("IMPORT INTO test.t_invalid_parquet FROM '%s' FORMAT 'parquet'", filePath)
		err := s.tk.QueryToErr(sql)
		require.ErrorContains(s.T(), err, tc.errMsg, tc.name)
	}
}

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
