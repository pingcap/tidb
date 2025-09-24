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

package ch

import (
	"flag"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testmain"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"go.uber.org/goleak"
)

var testDataMap = make(testdata.BookKeeper)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	flag.Parse()
	testDataMap.LoadTestSuiteData("testdata", "ch_suite", true)
	testsetup.SetupForCommonTest()

	flag.Parse()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	callback := func(i int) int {
		testDataMap.GenerateOutputIfNeeded()
		return i
	}

	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

func GetCHSuiteData() testdata.TestData {
	return testDataMap["ch_suite"]
}

func createCustomer(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`CREATE TABLE customer (
  c_id int NOT NULL,
  c_d_id int NOT NULL,
  c_w_id int NOT NULL,
  c_first varchar(16) DEFAULT NULL,
  c_middle char(2) DEFAULT NULL,
  c_last varchar(16) DEFAULT NULL,
  c_street_1 varchar(20) DEFAULT NULL,
  c_street_2 varchar(20) DEFAULT NULL,
  c_city varchar(20) DEFAULT NULL,
  c_state char(2) DEFAULT NULL,
  c_zip char(9) DEFAULT NULL,
  c_phone char(16) DEFAULT NULL,
  c_since datetime DEFAULT NULL,
  c_credit char(2) DEFAULT NULL,
  c_credit_lim decimal(12,2) DEFAULT NULL,
  c_discount decimal(4,4) DEFAULT NULL,
  c_balance decimal(12,2) DEFAULT NULL,
  c_ytd_payment decimal(12,2) DEFAULT NULL,
  c_payment_cnt int DEFAULT NULL,
  c_delivery_cnt int DEFAULT NULL,
  c_data varchar(500) DEFAULT NULL,
  PRIMARY KEY (c_w_id,c_d_id,c_id) /*T![clustered_index] NONCLUSTERED */,
  KEY idx_customer (c_w_id,c_d_id,c_last,c_first)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	testkit.SetTiFlashReplica(t, dom, "tpcc", "customer")
}

func createItem(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`CREATE TABLE item (
  i_id int NOT NULL,
  i_im_id int DEFAULT NULL,
  i_name varchar(24) DEFAULT NULL,
  i_price decimal(5,2) DEFAULT NULL,
  i_data varchar(50) DEFAULT NULL,
  PRIMARY KEY (i_id) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, dom, "tpcc", "item")
}

func createNation(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`CREATE TABLE nation (
  N_NATIONKEY bigint NOT NULL,
  N_NAME char(25) NOT NULL,
  N_REGIONKEY bigint NOT NULL,
  N_COMMENT varchar(152) DEFAULT NULL,
  PRIMARY KEY (N_NATIONKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, dom, "tpcc", "nation")
}

func createOrders(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`
CREATE TABLE orders (
  o_id int NOT NULL,
  o_d_id int NOT NULL,
  o_w_id int NOT NULL,
  o_c_id int DEFAULT NULL,
  o_entry_d datetime DEFAULT NULL,
  o_carrier_id int DEFAULT NULL,
  o_ol_cnt int DEFAULT NULL,
  o_all_local int DEFAULT NULL,
  PRIMARY KEY (o_w_id,o_d_id,o_id) /*T![clustered_index] NONCLUSTERED */,
  KEY idx_order (o_w_id,o_d_id,o_c_id,o_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	testkit.SetTiFlashReplica(t, dom, "tpcc", "orders")
}

func createOrderLine(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`
CREATE TABLE order_line (
  ol_o_id int NOT NULL,
  ol_d_id int NOT NULL,
  ol_w_id int NOT NULL,
  ol_number int NOT NULL,
  ol_i_id int NOT NULL,
  ol_supply_w_id int DEFAULT NULL,
  ol_delivery_d datetime DEFAULT NULL,
  ol_quantity int DEFAULT NULL,
  ol_amount decimal(6,2) DEFAULT NULL,
  ol_dist_info char(24) DEFAULT NULL,
  PRIMARY KEY (ol_w_id,ol_d_id,ol_o_id,ol_number) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, dom, "tpcc", "order_line")
}

func createSupplier(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`CREATE TABLE supplier (
  S_SUPPKEY bigint NOT NULL,
  S_NAME char(25) NOT NULL,
  S_ADDRESS varchar(40) NOT NULL,
  S_NATIONKEY bigint NOT NULL,
  S_PHONE char(15) NOT NULL,
  S_ACCTBAL decimal(15,2) NOT NULL,
  S_COMMENT varchar(101) NOT NULL,
  PRIMARY KEY (S_SUPPKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, dom, "tpcc", "supplier")
}

func createRegion(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`CREATE TABLE region (
  R_REGIONKEY bigint NOT NULL,
  R_NAME char(25) NOT NULL,
  R_COMMENT varchar(152) DEFAULT NULL,
  PRIMARY KEY (R_REGIONKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, dom, "tpcc", "region")
}

func createStock(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`CREATE TABLE stock (
  s_i_id int NOT NULL,
  s_w_id int NOT NULL,
  s_quantity int DEFAULT NULL,
  s_dist_01 char(24) DEFAULT NULL,
  s_dist_02 char(24) DEFAULT NULL,
  s_dist_03 char(24) DEFAULT NULL,
  s_dist_04 char(24) DEFAULT NULL,
  s_dist_05 char(24) DEFAULT NULL,
  s_dist_06 char(24) DEFAULT NULL,
  s_dist_07 char(24) DEFAULT NULL,
  s_dist_08 char(24) DEFAULT NULL,
  s_dist_09 char(24) DEFAULT NULL,
  s_dist_10 char(24) DEFAULT NULL,
  s_ytd int DEFAULT NULL,
  s_order_cnt int DEFAULT NULL,
  s_remote_cnt int DEFAULT NULL,
  s_data varchar(50) DEFAULT NULL,
  PRIMARY KEY (s_w_id,s_i_id) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, dom, "tpcc", "stock")
}
