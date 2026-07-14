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

package importintotest

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	drivererr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
)

func (s *mockGCSSuite) TestCleanupOnDataEngineImportError() {
	s.prepareAndUseDB("cleanup_on_error")
	s.tk.MustExec("create table t (a int primary key, b varchar(32));")

	tempDir := s.T().TempDir()
	dataPath := filepath.Join(tempDir, "data.csv")
	content := []byte("1,a\n2,b\n")
	s.NoError(os.WriteFile(dataPath, content, 0o644))

	var enterCnt atomic.Int32
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/executor/importer/mockDataEngineImportErr", func(errP *error) {
		if enterCnt.Add(1) == 1 {
			*errP = drivererr.ErrPDServerTimeout
		}
	})
	s.tk.MustQuery(fmt.Sprintf("IMPORT INTO t FROM '%s'", dataPath))
	s.GreaterOrEqual(enterCnt.Load(), int32(2), "the import should retry on data engine import error")

	s.tk.MustQuery("select * from t;").Sort().Check(testkit.Rows("1 a", "2 b"))
}
