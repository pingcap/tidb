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

package tpch

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
	testDataMap.LoadTestSuiteData("testdata", "tpch_suite", true)
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

func GetTPCHSuiteData() testdata.TestData {
	return testDataMap["tpch_suite"]
}

func createLineItem(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`
CREATE TABLE lineitem (
    L_ORDERKEY bigint NOT NULL,
    L_PARTKEY bigint NOT NULL,
    L_SUPPKEY bigint NOT NULL,
    L_LINENUMBER bigint NOT NULL,
    L_QUANTITY decimal(15,2) NOT NULL,
    L_EXTENDEDPRICE decimal(15,2) NOT NULL,
    L_DISCOUNT decimal(15,2) NOT NULL,
    L_TAX decimal(15,2) NOT NULL,
    L_RETURNFLAG char(1) NOT NULL,
    L_LINESTATUS char(1) NOT NULL,
    L_SHIPDATE date NOT NULL,
    L_COMMITDATE date NOT NULL,
    L_RECEIPTDATE date NOT NULL,
    L_SHIPINSTRUCT char(25) NOT NULL,
    L_SHIPMODE char(10) NOT NULL,
    L_COMMENT varchar(44) NOT NULL,
    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`)
	testkit.SetTiFlashReplica(t, dom, "test", "lineitem")
}

func createCustomer(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`CREATE TABLE customer (
    C_CUSTKEY bigint NOT NULL,
    C_NAME varchar(25) NOT NULL,
    C_ADDRESS varchar(40) NOT NULL,
    C_NATIONKEY bigint NOT NULL,
    C_PHONE char(15) NOT NULL,
    C_ACCTBAL decimal(15,2) NOT NULL,
    C_MKTSEGMENT char(10) NOT NULL,
    C_COMMENT varchar(117) NOT NULL,
    PRIMARY KEY (C_CUSTKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	testkit.SetTiFlashReplica(t, dom, "test", "customer")
}

func createOrders(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`
CREATE TABLE orders (
    O_ORDERKEY bigint NOT NULL,
    O_CUSTKEY bigint NOT NULL,
    O_ORDERSTATUS char(1) NOT NULL,
    O_TOTALPRICE decimal(15,2) NOT NULL,
    O_ORDERDATE date NOT NULL,
    O_ORDERPRIORITY char(15) NOT NULL,
    O_CLERK char(15) NOT NULL,
    O_SHIPPRIORITY bigint NOT NULL,
    O_COMMENT varchar(79) NOT NULL,
    PRIMARY KEY (O_ORDERKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	testkit.SetTiFlashReplica(t, dom, "test", "orders")
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
	testkit.SetTiFlashReplica(t, dom, "test", "supplier")
}

func createNation(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`CREATE TABLE nation (
  N_NATIONKEY bigint NOT NULL,
  N_NAME char(25) NOT NULL,
  N_REGIONKEY bigint NOT NULL,
  N_COMMENT varchar(152) DEFAULT NULL,
  PRIMARY KEY (N_NATIONKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, dom, "test", "nation")
}

func createPart(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`CREATE TABLE part (
  P_PARTKEY bigint NOT NULL,
  P_NAME varchar(55) NOT NULL,
  P_MFGR char(25) NOT NULL,
  P_BRAND char(10) NOT NULL,
  P_TYPE varchar(25) NOT NULL,
  P_SIZE bigint NOT NULL,
  P_CONTAINER char(10) NOT NULL,
  P_RETAILPRICE decimal(15,2) NOT NULL,
  P_COMMENT varchar(23) NOT NULL,
  PRIMARY KEY (P_PARTKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, dom, "test", "part")
}

func createPartsupp(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`CREATE TABLE partsupp (
  PS_PARTKEY bigint NOT NULL,
  PS_SUPPKEY bigint NOT NULL,
  PS_AVAILQTY bigint NOT NULL,
  PS_SUPPLYCOST decimal(15,2) NOT NULL,
  PS_COMMENT varchar(199) NOT NULL,
  PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, dom, "test", "partsupp")
}

func createRegion(t testing.TB, tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec(`CREATE TABLE region (
  R_REGIONKEY bigint NOT NULL,
  R_NAME char(25) NOT NULL,
  R_COMMENT varchar(152) DEFAULT NULL,
  PRIMARY KEY (R_REGIONKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	testkit.SetTiFlashReplica(t, dom, "test", "region")
}
