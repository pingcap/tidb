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

package casetest

import (
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestVerboseExplain(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec(`set tidb_opt_limit_push_down_threshold=0`)
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("create table t3(a int, b int, index c(b))")
	tk.MustExec("insert into t1 values(1,2)")
	tk.MustExec("insert into t1 values(3,4)")
	tk.MustExec("insert into t1 values(5,6)")
	tk.MustExec("insert into t2 values(1,2)")
	tk.MustExec("insert into t2 values(3,4)")
	tk.MustExec("insert into t2 values(5,6)")
	tk.MustExec("insert into t3 values(1,2)")
	tk.MustExec("insert into t3 values(3,4)")
	tk.MustExec("insert into t3 values(5,6)")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")
	tk.MustExec("analyze table t3")

	// Default RPC encoding may cause statistics explain result differ and then the test unstable.
	tk.MustExec("set @@tidb_enable_chunk_rpc = on")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t1" || tblInfo.Name.L == "t2" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIsolationReadTiFlashNotChoosePointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key (a))")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestIsolationReadDoNotFilterSystemDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_isolation_read_engines = \"tiflash\"")
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestPartitionPruningForInExpr(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(11) not null, b int) partition by range (a) (partition p0 values less than (4), partition p1 values less than(10), partition p2 values less than maxvalue);")
	tk.MustExec("insert into t values (1, 1),(10, 10),(11, 11)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestPartitionExplain(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table pt (id int, c int, key i_id(id), key i_c(c)) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)

	tk.MustExec("set @@tidb_enable_index_merge = 1;")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain " + tt).Rows())
		})
		tk.MustQuery("explain " + tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestMergeContinuousSelections(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ts")
	tk.MustExec("create table ts (col_char_64 char(64), col_varchar_64_not_null varchar(64) not null, col_varchar_key varchar(1), id int primary key, col_varchar_64 varchar(64),col_char_64_not_null char(64) not null);")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "ts" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec(" set @@tidb_allow_mpp=1;")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIssue31240(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t31240(a int, b int);")
	tk.MustExec("set @@tidb_allow_mpp = 0")
	tk.MustExec("set tidb_cost_model_version=2")
	// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t31240", L: "t31240"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") {
			tk.MustExec(tt)
			continue
		}
		testdata.OnRecord(func() {
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
	tk.MustExec("drop table if exists t31240")
}

func TestIssue32632(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `partsupp` (" +
		" `PS_PARTKEY` bigint(20) NOT NULL," +
		"`PS_SUPPKEY` bigint(20) NOT NULL," +
		"`PS_AVAILQTY` bigint(20) NOT NULL," +
		"`PS_SUPPLYCOST` decimal(15,2) NOT NULL," +
		"`PS_COMMENT` varchar(199) NOT NULL," +
		"PRIMARY KEY (`PS_PARTKEY`,`PS_SUPPKEY`) /*T![clustered_index] NONCLUSTERED */)")
	tk.MustExec("CREATE TABLE `supplier` (" +
		"`S_SUPPKEY` bigint(20) NOT NULL," +
		"`S_NAME` char(25) NOT NULL," +
		"`S_ADDRESS` varchar(40) NOT NULL," +
		"`S_NATIONKEY` bigint(20) NOT NULL," +
		"`S_PHONE` char(15) NOT NULL," +
		"`S_ACCTBAL` decimal(15,2) NOT NULL," +
		"`S_COMMENT` varchar(101) NOT NULL," +
		"PRIMARY KEY (`S_SUPPKEY`) /*T![clustered_index] CLUSTERED */)")
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tk.MustExec("set @@tidb_enforce_mpp = 1")

	tbl1, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "partsupp", L: "partsupp"})
	require.NoError(t, err)
	tbl2, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "supplier", L: "supplier"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tbl2.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	statsTbl1 := h.GetTableStats(tbl1.Meta())
	statsTbl1.RealtimeCount = 800000
	statsTbl2 := h.GetTableStats(tbl2.Meta())
	statsTbl2.RealtimeCount = 10000
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
	tk.MustExec("drop table if exists partsupp")
	tk.MustExec("drop table if exists supplier")
}

func TestTiFlashPartitionTableScan(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=1")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@tidb_enforce_mpp = on")
	tk.MustExec("set @@tidb_allow_batch_cop = 2")
	tk.MustExec("drop table if exists rp_t;")
	tk.MustExec("drop table if exists hp_t;")
	tk.MustExec("create table rp_t(a int) partition by RANGE (a) (PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11), PARTITION p2 VALUES LESS THAN (16), PARTITION p3 VALUES LESS THAN (21));")
	tk.MustExec("create table hp_t(a int) partition by hash(a) partitions 4;")
	tbl1, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "rp_t", L: "rp_t"})
	require.NoError(t, err)
	tbl2, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "hp_t", L: "hp_t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tbl2.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
	tk.MustExec("drop table rp_t;")
	tk.MustExec("drop table hp_t;")
}

func TestTiFlashFineGrainedShuffle(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@tidb_enforce_mpp = on")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int, c2 int)")

	tbl1, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t1", L: "t1"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIssue51873(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`CREATE TABLE h1 (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  position_date date NOT NULL,
  asset_id varchar(32) DEFAULT NULL,
  portfolio_code varchar(50) DEFAULT NULL,
  PRIMARY KEY (id,position_date) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY uidx_posi_asset_balance_key (position_date,portfolio_code,asset_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=30002
PARTITION BY RANGE COLUMNS(position_date)
(PARTITION p202401 VALUES LESS THAN ('2024-02-01'))`)
	tk.MustExec(`create table h2 like h1`)
	tk.MustExec(`insert into h1 values(1,'2024-01-01',1,1)`)
	tk.MustExec(`insert into h2 values(1,'2024-01-01',1,1)`)
	tk.MustExec(`analyze table h1`)
	tk.MustExec(`set @@tidb_skip_missing_partition_stats=0`)
	tk.MustQuery(`with assetBalance AS
    (SELECT asset_id, portfolio_code FROM h1 pab WHERE pab.position_date = '2024-01-01' ),
cashBalance AS (SELECT portfolio_code, asset_id
    FROM h2 pcb WHERE pcb.position_date = '2024-01-01' ),
assetIdList AS (SELECT DISTINCT asset_id AS assetId
    FROM assetBalance )
SELECT main.portfolioCode
FROM (SELECT DISTINCT balance.portfolio_code AS portfolioCode
    FROM assetBalance balance
    LEFT JOIN assetIdList
        ON balance.asset_id = assetIdList.assetId ) main`).Check(testkit.Rows("1"))
}

func TestIssue50926(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create or replace definer='root'@'localhost' view v (a,b) AS select 1 as a, json_object('k', '0') as b from t")
	tk.MustQuery("select sum(json_extract(b, '$.path')) from v group by a").Check(testkit.Rows()) // no error
}

func TestFixControl45132(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a))`)
	values := make([]string, 0, 101)
	for i := 0; i < 100; i++ {
		values = append(values, "(1, 1)")
	}
	values = append(values, "(2, 2)") // count(1) : count(2) == 100 : 1
	tk.MustExec(`insert into t values ` + strings.Join(values, ","))
	for i := 0; i < 7; i++ {
		tk.MustExec(`insert into t select * from t`)
	}
	tk.MustExec(`analyze table t`)
	// the cost model prefers to use TableScan instead of IndexLookup to avoid double requests.
	tk.MustHavePlan(`select * from t where a=2`, `TableFullScan`)

	tk.MustExec(`set @@tidb_opt_fix_control = "45132:99"`)
	tk.MustExec(`analyze table t`)
	tk.EventuallyMustIndexLookup(`select * from t where a=2`) // index lookup

	tk.MustExec(`set @@tidb_opt_fix_control = "45132:500"`)
	tk.MustHavePlan(`select * from t where a=2`, `TableFullScan`)

	tk.MustExec(`set @@tidb_opt_fix_control = "45132:0"`)
	tk.MustHavePlan(`select * from t where a=2`, `TableFullScan`)
}

func TestIssue41957(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("CREATE TABLE `github_events` (\n  `id` bigint(20) NOT NULL DEFAULT '0',\n  `type` varchar(29) NOT NULL DEFAULT 'Event',\n  `created_at` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',\n  `repo_id` bigint(20) NOT NULL DEFAULT '0',\n  `repo_name` varchar(140) NOT NULL DEFAULT '',\n  `actor_id` bigint(20) NOT NULL DEFAULT '0',\n  `actor_login` varchar(40) NOT NULL DEFAULT '',\n  `language` varchar(26) NOT NULL DEFAULT '',\n  `additions` bigint(20) NOT NULL DEFAULT '0',\n  `deletions` bigint(20) NOT NULL DEFAULT '0',\n  `action` varchar(11) NOT NULL DEFAULT '',\n  `number` int(11) NOT NULL DEFAULT '0',\n  `commit_id` varchar(40) NOT NULL DEFAULT '',\n  `comment_id` bigint(20) NOT NULL DEFAULT '0',\n  `org_login` varchar(40) NOT NULL DEFAULT '',\n  `org_id` bigint(20) NOT NULL DEFAULT '0',\n  `state` varchar(6) NOT NULL DEFAULT '',\n  `closed_at` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',\n  `comments` int(11) NOT NULL DEFAULT '0',\n  `pr_merged_at` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',\n  `pr_merged` tinyint(1) NOT NULL DEFAULT '0',\n  `pr_changed_files` int(11) NOT NULL DEFAULT '0',\n  `pr_review_comments` int(11) NOT NULL DEFAULT '0',\n  `pr_or_issue_id` bigint(20) NOT NULL DEFAULT '0',\n  `event_day` date NOT NULL,\n  `event_month` date NOT NULL,\n  `event_year` int(11) NOT NULL,\n  `push_size` int(11) NOT NULL DEFAULT '0',\n  `push_distinct_size` int(11) NOT NULL DEFAULT '0',\n  `creator_user_login` varchar(40) NOT NULL DEFAULT '',\n  `creator_user_id` bigint(20) NOT NULL DEFAULT '0',\n  `pr_or_issue_created_at` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',\n  KEY `index_github_events_on_id` (`id`),\n  KEY `index_github_events_on_created_at` (`created_at`),\n  KEY `index_github_events_on_repo_id_type_action_month_actor_login` (`repo_id`,`type`,`action`,`event_month`,`actor_login`),\n  KEY `index_ge_on_repo_id_type_action_pr_merged_created_at_add_del` (`repo_id`,`type`,`action`,`pr_merged`,`created_at`,`additions`,`deletions`),\n  KEY `index_ge_on_creator_id_type_action_merged_created_at_add_del` (`creator_user_id`,`type`,`action`,`pr_merged`,`created_at`,`additions`,`deletions`),\n  KEY `index_ge_on_actor_id_type_action_created_at_repo_id_commits` (`actor_id`,`type`,`action`,`created_at`,`repo_id`,`push_distinct_size`),\n  KEY `index_ge_on_repo_id_type_action_created_at_number_pdsize_psize` (`repo_id`,`type`,`action`,`created_at`,`number`,`push_distinct_size`,`push_size`),\n  KEY `index_ge_on_repo_id_type_action_created_at_actor_login` (`repo_id`,`type`,`action`,`created_at`,`actor_login`),\n  KEY `index_ge_on_repo_name_type` (`repo_name`,`type`),\n  KEY `index_ge_on_actor_login_type` (`actor_login`,`type`),\n  KEY `index_ge_on_org_login_type` (`org_login`,`type`),\n  KEY `index_ge_on_language` (`language`),\n  KEY `index_ge_on_org_id_type` (`org_id`,`type`),\n  KEY `index_ge_on_actor_login_lower` ((lower(`actor_login`))),\n  KEY `index_ge_on_repo_name_lower` ((lower(`repo_name`))),\n  KEY `index_ge_on_language_lower` ((lower(`language`))),\n  KEY `index_ge_on_type_action` (`type`,`action`) /*!80000 INVISIBLE */,\n  KEY `index_ge_on_repo_id_type_created_at` (`repo_id`,`type`,`created_at`),\n  KEY `index_ge_on_repo_id_created_at` (`repo_id`,`created_at`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\nPARTITION BY LIST COLUMNS(`type`)\n(PARTITION `push_event` VALUES IN ('PushEvent'),\n PARTITION `create_event` VALUES IN ('CreateEvent'),\n PARTITION `pull_request_event` VALUES IN ('PullRequestEvent'),\n PARTITION `watch_event` VALUES IN ('WatchEvent'),\n PARTITION `issue_comment_event` VALUES IN ('IssueCommentEvent'),\n PARTITION `issues_event` VALUES IN ('IssuesEvent'),\n PARTITION `delete_event` VALUES IN ('DeleteEvent'),\n PARTITION `fork_event` VALUES IN ('ForkEvent'),\n PARTITION `pull_request_review_comment_event` VALUES IN ('PullRequestReviewCommentEvent'),\n PARTITION `pull_request_review_event` VALUES IN ('PullRequestReviewEvent'),\n PARTITION `gollum_event` VALUES IN ('GollumEvent'),\n PARTITION `release_event` VALUES IN ('ReleaseEvent'),\n PARTITION `member_event` VALUES IN ('MemberEvent'),\n PARTITION `commit_comment_event` VALUES IN ('CommitCommentEvent'),\n PARTITION `public_event` VALUES IN ('PublicEvent'),\n PARTITION `gist_event` VALUES IN ('GistEvent'),\n PARTITION `follow_event` VALUES IN ('FollowEvent'),\n PARTITION `event` VALUES IN ('Event'),\n PARTITION `download_event` VALUES IN ('DownloadEvent'),\n PARTITION `team_add_event` VALUES IN ('TeamAddEvent'),\n PARTITION `fork_apply_event` VALUES IN ('ForkApplyEvent'))\n")
	tk.MustQuery("SELECT\n    repo_id, GROUP_CONCAT(\n      DISTINCT actor_login\n      ORDER BY cnt DESC\n      SEPARATOR ','\n    ) AS actor_logins\nFROM (\n    SELECT\n        ge.repo_id AS repo_id,\n        ge.actor_login AS actor_login,\n        COUNT(*) AS cnt\n    FROM github_events ge\n    WHERE\n        type = 'PullRequestEvent' AND action = 'opened'\n        AND (ge.created_at >= DATE_SUB(NOW(), INTERVAL 1 DAY) AND ge.created_at <= NOW())\n    GROUP BY ge.repo_id, ge.actor_login\n    ORDER BY cnt DESC\n) sub\nGROUP BY repo_id").Check(testkit.Rows())
}
