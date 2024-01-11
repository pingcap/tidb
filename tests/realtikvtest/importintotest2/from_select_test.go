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

package importintotest

import (
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
)

func (s *mockGCSSuite) TestImportFromSelectBasic() {
	s.prepareAndUseDB("from_select")
	s.tk.MustExec("create table src(id int, v varchar(64))")
	s.tk.MustExec("create table dst(id int, v varchar(64))")
	s.tk.MustExec("insert into src values(4, 'aaaaaa'), (5, 'bbbbbb'), (6, 'cccccc'), (7, 'dddddd')")

	s.ErrorIs(s.tk.ExecToErr(`import into dst FROM select id from src`), core.ErrWrongValueCountOnRow)
	s.ErrorIs(s.tk.ExecToErr(`import into dst(id) FROM select * from src`), core.ErrWrongValueCountOnRow)

	s.tk.MustExec(`import into dst FROM select * from src`)
	s.Equal(uint64(4), s.tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	s.Contains(s.tk.Session().LastMessage(), "Records: 4,")
	s.tk.MustQuery("select * from dst").Check(testkit.Rows("4 aaaaaa", "5 bbbbbb", "6 cccccc", "7 dddddd"))

	// non-empty table
	s.ErrorContains(s.tk.ExecToErr(`import into dst FROM select * from src`), "target table is not empty")

	s.tk.MustExec("truncate table dst")
	s.tk.MustExec(`import into dst FROM select * from src where id > 5 with thread = 4`)
	s.Equal(uint64(2), s.tk.Session().GetSessionVars().StmtCtx.AffectedRows())
	s.Contains(s.tk.Session().LastMessage(), "Records: 2,")
	s.tk.MustQuery("select * from dst").Check(testkit.Rows("6 cccccc", "7 dddddd"))
}

func (s *mockGCSSuite) TestWriteAfterImportFromSelect() {
	s.prepareAndUseDB("from_select")
	s.tk.MustExec("create table dt(id int, v varchar(64))")
	s.tk.MustExec("insert into dt values(4, 'aaaaaa'), (5, 'bbbbbb'), (6, 'cccccc'), (7, 'dddddd')")
	s.testWriteAfterImport(`import into t FROM select * from from_select.dt`, importer.DataSourceTypeQuery)
}
