// Copyright 2022 PingCAP, Inc.
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

package issuetest

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

func TestIssue43645(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1(id int,col1 varchar(10),col2 varchar(10),col3 varchar(10));")
	tk.MustExec("CREATE TABLE t2(id int,col1 varchar(10),col2 varchar(10),col3 varchar(10));")
	tk.MustExec("INSERT INTO t1 values(1,NULL,NULL,null),(2,NULL,NULL,null),(3,NULL,NULL,null);")
	tk.MustExec("INSERT INTO t2 values(1,'a','aa','aaa'),(2,'b','bb','bbb'),(3,'c','cc','ccc');")

	rs := tk.MustQuery("WITH tmp AS (SELECT t2.* FROM t2) select (SELECT tmp.col1 FROM tmp WHERE tmp.id=t1.id ) col1, (SELECT tmp.col2 FROM tmp WHERE tmp.id=t1.id ) col2, (SELECT tmp.col3 FROM tmp WHERE tmp.id=t1.id ) col3 from t1;")
	rs.Sort().Check(testkit.Rows("a aa aaa", "b bb bbb", "c cc ccc"))
}
