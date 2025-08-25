// Copyright 2025 PingCAP, Inc.
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

package job

import (
	"flag"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

var testDataMap = make(testdata.BookKeeper)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	flag.Parse()
	testDataMap.LoadTestSuiteData("testdata", "job_suite", true)
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	callback := func(i int) int {
		testDataMap.GenerateOutputIfNeeded()
		return i
	}

	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

func GetJOBSuiteData() testdata.TestData {
	return testDataMap["job_suite"]
}

func createCompanyName(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`
CREATE TABLE company_name (
  id int NOT NULL AUTO_INCREMENT,
  name text NOT NULL,
  country_code varchar(255) DEFAULT NULL,
  imdb_id int DEFAULT NULL,
  name_pcode_nf varchar(5) DEFAULT NULL,
  name_pcode_sf varchar(5) DEFAULT NULL,
  md5sum varchar(32) DEFAULT NULL,
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  KEY company_name_idx_name (name(6)),
  KEY company_name_idx_ccode (country_code(5)),
  KEY company_name_idx_imdb_id (imdb_id),
  KEY company_name_idx_pcodenf (name_pcode_nf),
  KEY company_name_idx_pcodesf (name_pcode_sf),
  KEY company_name_idx_md5 (md5sum(5))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=420016;
`)
	testkit.SetTiFlashReplica(t, dom, "imdb", "company_name")
}

func createInfoType(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`
CREATE TABLE info_type (
  id int NOT NULL AUTO_INCREMENT,
  info varchar(32) NOT NULL,
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  KEY info_type_info (info(5))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=1053837;
`)
	testkit.SetTiFlashReplica(t, dom, "imdb", "info_type")
}

func createKindype(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`
CREATE TABLE kind_type (
  id int NOT NULL AUTO_INCREMENT,
  kind varchar(15) DEFAULT NULL,
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  KEY kind_type_kind (kind(5))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=1047674`)
	testkit.SetTiFlashReplica(t, dom, "imdb", "kind_type")
}

func createLinkType(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`
CREATE TABLE link_type (
  id int NOT NULL AUTO_INCREMENT,
  link varchar(32) NOT NULL,
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  KEY link_type_link (link(5))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=1058243`)
	testkit.SetTiFlashReplica(t, dom, "imdb", "link_type")
}

func createMovieCompanies(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`
CREATE TABLE movie_companies (
  id int NOT NULL AUTO_INCREMENT,
  movie_id int NOT NULL,
  company_id int NOT NULL,
  company_type_id int NOT NULL,
  note text DEFAULT NULL,
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  KEY movie_companies_idx_mid (movie_id),
  KEY movie_companies_idx_cid (company_id),
  KEY movie_companies_idx_ctypeid (company_type_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=5130837`)
	testkit.SetTiFlashReplica(t, dom, "imdb", "movie_companies")
}

func createMovieInfoIdx(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`
CREATE TABLE movie_info_idx (
  id int NOT NULL,
  movie_id int NOT NULL,
  info_type_id int NOT NULL,
  info text NOT NULL,
  note text DEFAULT NULL,
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  KEY info_type_id_movie_info_idx (info_type_id),
  KEY movie_id_movie_info_idx (movie_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, dom, "imdb", "movie_info_idx")
}

func createMovieLink(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`
CREATE TABLE movie_link (
  id int NOT NULL AUTO_INCREMENT,
  movie_id int NOT NULL,
  linked_movie_id int NOT NULL,
  link_type_id int NOT NULL,
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  KEY movie_link_idx_mid (movie_id),
  KEY movie_link_idx_lmid (linked_movie_id),
  KEY movie_link_idx_ltypeid (link_type_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=2811633`)
	testkit.SetTiFlashReplica(t, dom, "imdb", "movie_link")
}

func createTitle(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`
CREATE TABLE title (
  id int NOT NULL AUTO_INCREMENT,
  title text NOT NULL,
  imdb_index varchar(12) DEFAULT NULL,
  kind_id int NOT NULL,
  production_year int DEFAULT NULL,
  imdb_id int DEFAULT NULL,
  phonetic_code varchar(5) DEFAULT NULL,
  episode_of_id int DEFAULT NULL,
  season_nr int DEFAULT NULL,
  episode_nr int DEFAULT NULL,
  series_years varchar(49) DEFAULT NULL,
  md5sum varchar(32) DEFAULT NULL,
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */,
  KEY title_idx_title (title(10)),
  KEY title_idx_kindid (kind_id),
  KEY title_idx_year (production_year),
  KEY title_idx_imdb_id (imdb_id),
  KEY title_idx_pcode (phonetic_code),
  KEY title_idx_epof (episode_of_id),
  KEY title_idx_season_nr (season_nr),
  KEY title_idx_episode_nr (episode_nr),
  KEY title_idx_md5 (md5sum(5))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=4770161`)
	testkit.SetTiFlashReplica(t, dom, "imdb", "title")
}

// check the cost trace's cost and verbose's cost. they should be the same.
// it is from https://github.com/pingcap/tidb/issues/61155
func checkCost(t *testing.T, tk *testkit.TestKit, q4 string) {
	costTraceFormat := `explain format='cost_trace' `
	verboseFormat := `explain format='verbose' `
	costTraceRows := tk.MustQuery(costTraceFormat + q4)
	verboseRows := tk.MustQuery(verboseFormat + q4)
	require.Equal(t, len(costTraceRows.Rows()), len(verboseRows.Rows()))
	for i := 0; i < len(costTraceRows.Rows()); i++ {
		// check id / estRows / estCost. they should be the same one
		require.Equal(t, costTraceRows.Rows()[i][:3], verboseRows.Rows()[i][:3])
	}
}
