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

	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
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
	// directory without permission
	s.NoError(os.MkdirAll(path.Join(tempDir, "no-perm"), 0o700))
	s.NoError(os.WriteFile(path.Join(tempDir, "no-perm", "no-perm.csv"), []byte("1,1"), 0o644))
	s.NoError(os.Chmod(path.Join(tempDir, "no-perm"), 0o000))
	s.T().Cleanup(func() {
		// make sure TempDir RemoveAll cleanup works
		_ = os.Chmod(path.Join(tempDir, "no-perm"), 0o700)
	})
	// file without permission
	s.NoError(os.WriteFile(path.Join(tempDir, "no-perm.csv"), []byte("1,1"), 0o644))
	s.NoError(os.Chmod(path.Join(tempDir, "no-perm.csv"), 0o000))

	s.prepareAndUseDB("from_server")
	s.tk.MustExec("create table t (a bigint, b varchar(100));")

	// relative path
	s.ErrorIs(s.tk.QueryToErr("IMPORT INTO t FROM '~/file.csv'"), exeerrors.ErrLoadDataInvalidURI)
	// no suffix or wrong suffix
	s.ErrorIs(s.tk.QueryToErr("IMPORT INTO t FROM '/file'"), exeerrors.ErrLoadDataInvalidURI)
	s.ErrorIs(s.tk.QueryToErr("IMPORT INTO t FROM '/file.txt'"), exeerrors.ErrLoadDataInvalidURI)
	// non-exist parent directory
	err := s.tk.QueryToErr("IMPORT INTO t FROM '/path/to/non/exists/file.csv'")
	s.ErrorIs(err, exeerrors.ErrLoadDataInvalidURI)
	s.ErrorContains(err, "no such file or directory")
	// without permission to parent dir
	err = s.tk.QueryToErr(fmt.Sprintf("IMPORT INTO t FROM '%s'", path.Join(tempDir, "no-perm", "no-perm.csv")))
	s.ErrorIs(err, exeerrors.ErrLoadDataCantRead)
	s.ErrorContains(err, "permission denied")
	// file not exists
	err = s.tk.QueryToErr(fmt.Sprintf("IMPORT INTO t FROM '%s'", path.Join(tempDir, "not-exists.csv")))
	s.ErrorIs(err, exeerrors.ErrLoadDataCantRead)
	s.ErrorContains(err, "no such file or directory")
	// file without permission
	err = s.tk.QueryToErr(fmt.Sprintf("IMPORT INTO t FROM '%s'", path.Join(tempDir, "no-perm.csv")))
	s.ErrorIs(err, exeerrors.ErrLoadDataCantRead)
	s.ErrorContains(err, "permission denied")

	s.tk.MustQuery(fmt.Sprintf("IMPORT INTO t FROM '%s'", path.Join(tempDir, "server-0.csv")))
	s.tk.MustQuery("SELECT * FROM t;").Sort().Check(testkit.Rows([]string{"0 test-0", "1 test-1"}...))

	s.tk.MustExec("truncate table t")
	// we don't have read access to 'no-perm' directory, so walk-dir fails
	err = s.tk.QueryToErr(fmt.Sprintf("IMPORT INTO t FROM '%s'", path.Join(tempDir, "server-*.csv")))
	s.ErrorIs(err, exeerrors.ErrLoadDataCantRead)
	s.ErrorContains(err, "permission denied")

	s.NoError(os.Chmod(path.Join(tempDir, "no-perm"), 0o400))
	s.tk.MustQuery(fmt.Sprintf("IMPORT INTO t FROM '%s'", path.Join(tempDir, "server-*.csv")))
	s.tk.MustQuery("SELECT * FROM t;").Sort().Check(testkit.Rows(allData...))
}
