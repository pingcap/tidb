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

package core_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestFTSRequiresStarterMode(t *testing.T) {
	if kerneltype.IsNextGen() {
		originDeployMode := deploymode.Get()
		require.NoError(t, deploymode.Set(deploymode.Premium))
		defer func() {
			require.NoError(t, deploymode.Set(originDeployMode))
		}()
	}

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustContainErrMsg("create table fts_blocked(id int primary key, title text, fulltext key ft_title(title))", "FULLTEXT index is only supported in starter deployment mode")
	tk.MustExec("create table fts_t(id int primary key, title text)")
	tk.MustContainErrMsg("alter table fts_t add fulltext index ft_title(title)", "FULLTEXT index is only supported in starter deployment mode")
	tk.MustContainErrMsg("explain select * from fts_t where fts_match_word('hello', title)", "FTS_MATCH_WORD() is only supported in starter deployment mode")
}

func TestTiFlashFTSMatchWordPushDown(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("starter deploy mode is nextgen-only")
	}
	originDeployMode := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	defer func() {
		require.NoError(t, deploymode.Set(originDeployMode))
	}()

	testkit.RunTestUnderCascadesAndDomainWithSchemaLease(t, 600*time.Millisecond, []mockstore.MockTiKVStoreOption{mockstore.WithMockTiFlash(2)}, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tiflash := infosync.NewMockTiFlash()
		infosync.SetMockTiFlash(tiflash)
		defer func() {
			tiflash.Lock()
			tiflash.StatusServer.Close()
			tiflash.Unlock()
		}()
		tk.MustExec("use test")
		tk.MustExec("set global tidb_redact_log=OFF")
		defer tk.MustExec("set global tidb_redact_log=OFF")
		tk.MustExec("create table fts_t(id int primary key, title text, body text, fulltext key ft_title(title))")
		tk.MustExec("alter table fts_t set tiflash replica 1")
		testkit.SetTiFlashReplica(t, dom, "test", "fts_t")

		tk.MustQuery("explain format = 'plan_tree' select * from fts_t where fts_match_word('hello', title)").MultiCheckContain([]string{
			"tiflash]",
			"ftsIndex:hello IN test.fts_t.title",
		})
		tk.MustQuery("explain format = 'plan_tree' select * from fts_t where fts_match_word('hello', title)").MultiCheckNotContain([]string{
			"ftsIndex:hello IN test.fts_t.title->",
			"_FTS_SCORE",
		})
		tk.MustQuery("explain format = 'plan_tree' select * from fts_t where fts_match_word('hello', title) order by fts_match_word('hello', title) desc limit 10").MultiCheckContain([]string{
			"tiflash]",
			"ftsIndex:top10 hello IN test.fts_t.title->_FTS_SCORE",
		})
		tk.MustQuery("explain format = 'plan_tree' select id, fts_match_word('hello', title) as score from fts_t where fts_match_word('hello', title) order by score desc limit 10").MultiCheckContain([]string{
			"tiflash]",
			"ftsIndex:top10 hello IN test.fts_t.title->_FTS_SCORE",
		})
		tk.MustQuery("explain format = 'plan_tree' select * from fts_t where fts_match_word('hello', title) order by fts_match_word('hello', title) desc, id limit 10").MultiCheckContain([]string{
			"tiflash]",
			"ftsIndex:hello IN test.fts_t.title->_FTS_SCORE",
		})
		tk.MustQuery("explain format = 'plan_tree' select * from fts_t where fts_match_word('hello', title) order by fts_match_word('hello', title) desc, id limit 10").CheckNotContain("ftsIndex:top10")
		tk.MustQuery("explain format = 'plan_tree' select fts_match_word('hello', title) from fts_t where fts_match_word('hello', title)").MultiCheckContain([]string{
			"tiflash]",
			"ftsIndex:hello IN test.fts_t.title->_FTS_SCORE",
		})
		tk.MustQuery("explain format = 'plan_tree' select fts_match_word('hello', title), id from fts_t where fts_match_word('hello', title) order by fts_match_word('hello', title) desc limit 10").MultiCheckContain([]string{
			"tiflash]",
			"ftsIndex:top10 hello IN test.fts_t.title->_FTS_SCORE",
		})
		tk.MustExec("set global tidb_redact_log=ON")
		tk.MustQuery("explain format = 'plan_tree' select * from fts_t where fts_match_word('hello', title)").MultiCheckContain([]string{
			"tiflash]",
			"ftsIndex:? IN test.fts_t.title",
		})
		tk.MustQuery("explain format = 'plan_tree' select * from fts_t where fts_match_word('hello', title)").CheckNotContain("ftsIndex:? IN test.fts_t.title->")
		tk.MustQuery("explain format = 'plan_tree' select * from fts_t where fts_match_word('hello', title)").CheckNotContain("ftsIndex:hello")
		tk.MustExec("set global tidb_redact_log=OFF")

		tk.MustContainErrMsg("explain select fts_match_word('hello', title) from fts_t", "'FTS_MATCH_WORD()' in SELECT requires a matching 'FTS_MATCH_WORD()' in WHERE")
		tk.MustContainErrMsg("explain select fts_match_word('hello', title) * 2 from fts_t where fts_match_word('hello', title)", "'FTS_MATCH_WORD()' in SELECT must not be wrapped in expressions")
		tk.MustContainErrMsg("explain select fts_match_word('world', title) from fts_t where fts_match_word('hello', title)", "'FTS_MATCH_WORD()' in SELECT must match the one in WHERE")
		tk.MustContainErrMsg("explain select * from fts_t where fts_match_word('hello', body)", "Full text search can only be used with a matching fulltext index")
		tk.MustContainErrMsg("explain select * from fts_t order by fts_match_word('hello', title)", "Currently 'FTS_MATCH_WORD()' in ORDER BY without a LIMIT clause is not supported")
	})
}
