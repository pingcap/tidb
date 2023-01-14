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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestIssue40535(t *testing.T) {
	store := testkit.CreateMockStore(t)
	var cfg kv.InjectionConfig
	tk := testkit.NewTestKit(t, kv.NewInjectedStore(store, &cfg))
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1; drop table if exists t2;")
	tk.MustExec("CREATE TABLE `t1`(`c1` bigint(20) NOT NULL DEFAULT '-2312745469307452950', `c2` datetime DEFAULT '5316-02-03 06:54:49', `c3` tinyblob DEFAULT NULL, PRIMARY KEY (`c1`) /*T![clustered_index] CLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;")
	tk.MustExec("CREATE TABLE `t2`(`c1` set('kn8pu','7et','vekx6','v3','liwrh','q14','1met','nnd5i','5o0','8cz','l') DEFAULT '7et,vekx6,liwrh,q14,1met', `c2` float DEFAULT '1.683167', KEY `k1` (`c2`,`c1`), KEY `k2` (`c2`)) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci;")
	tk.MustExec("(select /*+ agg_to_cop()*/ locate(t1.c3, t1.c3) as r0, t1.c3 as r1 from t1 where not( IsNull(t1.c1)) order by r0,r1) union all (select concat_ws(',', t2.c2, t2.c1) as r0, t2.c1 as r1 from t2 order by r0, r1) order by 1 limit 273;")
	require.Empty(t, tk.Session().LastMessage())
}
