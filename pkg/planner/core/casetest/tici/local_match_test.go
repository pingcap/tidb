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

package tici

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestLocalMatchIndexConfig(t *testing.T) {
	for _, name := range []string{"MockCreateTiCIIndexSuccess", "MockFinishIndexUpload", "MockCheckAddIndexProgress"} {
		path := "github.com/pingcap/tidb/pkg/tici/" + name
		require.NoError(t, failpoint.Enable(path, `return(true)`))
		t.Cleanup(func() { require.NoError(t, failpoint.Disable(path)) })
	}
	store := testkit.CreateMockStoreWithSchemaLease(t, time.Second, mockstore.WithMockTiFlash(2))
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)
	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()
	tk.MustExec("use test")
	oldSizes := tk.MustQuery("select @@global.innodb_ft_min_token_size, @@global.innodb_ft_max_token_size, @@global.ngram_token_size").Rows()[0]
	defer func() {
		tk.MustExec(fmt.Sprintf("set global innodb_ft_min_token_size = %v", oldSizes[0]))
		tk.MustExec(fmt.Sprintf("set global innodb_ft_max_token_size = %v", oldSizes[1]))
		tk.MustExec(fmt.Sprintf("set global ngram_token_size = %v", oldSizes[2]))
	}()
	tk.MustExec("set global innodb_ft_min_token_size = 2")
	tk.MustExec("set global innodb_ft_max_token_size = 8")
	tk.MustExec("set global ngram_token_size = 2")
	tk.MustExec("set innodb_ft_enable_stopword = off")
	tk.MustExec("create table local_fts(id int primary key, body text, fulltext index ft(body))")
	tk.MustExec("insert into local_fts values (1, 'an cat'), (2, 'elephant'), (3, 'encyclopedia'), (4, null)")
	tk.MustExec("create table local_ngram(id int primary key, body text, fulltext index ft(body) with parser ngram)")
	tk.MustExec("insert into local_ngram values (1, '数据库'), (2, '数据'), (3, '库')")
	// Query-time variables deliberately disagree with both index snapshots.
	tk.MustExec("set global innodb_ft_min_token_size = 4")
	tk.MustExec("set global innodb_ft_max_token_size = 5")
	tk.MustExec("set global ngram_token_size = 3")
	tk.MustExec("set innodb_ft_enable_stopword = on")
	tk.MustExec("set tidb_enable_local_match_against = on")
	tk.MustQuery("select id from local_fts where match(body) against('+an' in boolean mode)").Check(testkit.Rows("1"))
	tk.MustQuery("select id from local_fts where match(body) against('+elephant' in boolean mode)").Check(testkit.Rows("2"))
	tk.MustQuery("select id from local_fts where match(body) against('+encyclopedia' in boolean mode)").Check(testkit.Rows())
	tk.MustQuery("select id from local_ngram where match(body) against('+数据' in boolean mode) order by id").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select id from local_fts where not match(body) against('+cat' in boolean mode) order by id").Check(testkit.Rows("2", "3", "4"))
	tk.MustQuery("select id from local_fts where id=3 or match(body) against('+cat' in boolean mode) order by id").Check(testkit.Rows("1", "3"))
	tk.MustQuery("select id, body from local_fts having match(body) against('+cat' in boolean mode)").Check(testkit.Rows("1 an cat"))
	tk.MustQuery("select a.id from local_fts a join local_fts b on a.id=b.id and match(a.body) against('+cat' in boolean mode)").Check(testkit.Rows("1"))
	require.Error(t, tk.ExecToErr("select match(body) against('+cat' in boolean mode) from local_fts"))
	require.Error(t, tk.ExecToErr("select id from local_fts where match(body) against('+cat' in boolean mode) > 0.5"))
	// Search parameters must not retain the previous execution's compiled query.
	tk.MustExec("prepare local_stmt from 'select id from local_fts where match(body) against(? in boolean mode)'")
	for _, value := range []string{"+an", "+elephant", "+encyclopedia", "+cat"} {
		tk.MustExec("set @search = '" + value + "'")
		expected := map[string][]string{"+an": {"1"}, "+elephant": {"2"}, "+encyclopedia": {}, "+cat": {"1"}}
		tk.MustQuery("execute local_stmt using @search").Check(testkit.Rows(expected[value]...))
	}
	prepared, err := tk.Session().GetSessionVars().GetPreparedStmtByName("local_stmt")
	require.NoError(t, err)
	localKey, _, cacheable, _, err := core.NewPlanCacheKey(tk.Session(), prepared.(*core.PlanCacheStmt))
	require.NoError(t, err)
	require.True(t, cacheable)
	tk.Session().GetSessionVars().EnableLocalMatchAgainst = false
	nativeKey, _, cacheable, _, err := core.NewPlanCacheKey(tk.Session(), prepared.(*core.PlanCacheStmt))
	tk.Session().GetSessionVars().EnableLocalMatchAgainst = true
	require.NoError(t, err)
	require.True(t, cacheable)
	require.NotEqual(t, localKey, nativeKey)
	tk.MustExec("begin")
	tk.MustExec("insert into local_fts values (5, 'cat')")
	tk.MustQuery("select id from local_fts where match(body) against('+cat' in boolean mode) order by id").Check(testkit.Rows("1", "5"))
	tk.MustExec("rollback")
	localPlan := fmt.Sprint(tk.MustQuery("explain select id from local_fts where match(body) against('+cat' in boolean mode)").Rows())
	require.Contains(t, localPlan, "match_against(")
	require.NotContains(t, strings.ToLower(localPlan), "search func:")
	require.Contains(t, localPlan, "cop[tikv]")
	testkit.SetTiFlashReplica(t, domain.GetDomain(tk.Session()), "test", "local_fts")
	indexInfo := external.GetTableByName(t, tk, "test", "local_fts").Meta().FindIndexByName("ft").FullTextInfo
	config := indexInfo.ParserConfig
	indexInfo.ParserConfig = nil
	legacyPlan := fmt.Sprint(tk.MustQuery("explain select id from local_fts where match(body) against('+cat' in boolean mode)").Rows())
	indexInfo.ParserConfig = config
	require.Contains(t, legacyPlan, "index:ft(body)")
	require.Contains(t, legacyPlan, "search func:fts_match_word")
	tk.MustExec("set tidb_enable_local_match_against = off")
	nativePlan := fmt.Sprint(tk.MustQuery("explain select id from local_fts where match(body) against('+cat' in boolean mode)").Rows())
	require.Contains(t, nativePlan, "index:ft(body)")
	require.Contains(t, nativePlan, "search func:fts_match_word")
}
