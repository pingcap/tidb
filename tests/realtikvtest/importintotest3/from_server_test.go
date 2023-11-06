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
	"fmt"
	"os"
	"path"

	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/testkit"
)

func (s *mockGCSSuite) TestImportFromServer() {
	tempDir := s.T().TempDir()
	var allData []string
	for i := 0; i < 3; i++ {
		fileName := fmt.Sprintf("server-%d.csv", i)
		var content []byte
		rowCnt := 2
		for j := 0; j < rowCnt; j++ {
			content = append(content, []byte(fmt.Sprintf("%d,test-%d\n", i*rowCnt+j, i*rowCnt+j))...)
			allData = append(allData, fmt.Sprintf("%d test-%d", i*rowCnt+j, i*rowCnt+j))
		}
		s.NoError(os.WriteFile(path.Join(tempDir, fileName), content, 0o644))
	}

	s.prepareAndUseDB("from_server")
	s.tk.MustExec("create table t (a bigint, b varchar(100));")

	s.tk.MustQuery(fmt.Sprintf("IMPORT INTO t FROM '%s'", path.Join(tempDir, "server-0.csv")))
	s.tk.MustQuery("SELECT * FROM t;").Sort().Check(testkit.Rows([]string{"0 test-0", "1 test-1"}...))

	s.tk.MustExec("truncate table t")
	s.tk.MustQuery(fmt.Sprintf("IMPORT INTO t FROM '%s'", path.Join(tempDir, "server-*.csv")))
	s.tk.MustQuery("SELECT * FROM t;").Sort().Check(testkit.Rows(allData...))

	// try a gzip file
	s.NoError(os.WriteFile(
		path.Join(tempDir, "test.csv.gz"),
		s.getCompressedData(mydump.CompressionGZ, []byte("1,test1\n2,test2")),
		0o644))
	s.tk.MustExec("truncate table t")
	s.tk.MustQuery(fmt.Sprintf("IMPORT INTO t FROM '%s'", path.Join(tempDir, "test.csv.gz")))
	s.tk.MustQuery("SELECT * FROM t;").Sort().Check(testkit.Rows([]string{"1 test1", "2 test2"}...))
}
