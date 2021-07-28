// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"testing"

	"github.com/pingcap/tidb/util/testbridge"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/dgraph-io/ristretto.(*Cache).processItems"),
		goleak.IgnoreTopFunction("github.com/dgraph-io/ristretto.(*defaultPolicy).processItems"),
		goleak.IgnoreTopFunction("github.com/pingcap/badger.(*DB).runFlushMemTable"),
		goleak.IgnoreTopFunction("github.com/pingcap/badger.(*DB).updateSize"),
		goleak.IgnoreTopFunction("github.com/pingcap/badger.(*blobGCHandler).run"),
		goleak.IgnoreTopFunction("github.com/pingcap/badger.(*levelsController).runWorker"),
		goleak.IgnoreTopFunction("github.com/pingcap/badger.(*writeWorker).runMergeLSM"),
		goleak.IgnoreTopFunction("github.com/pingcap/badger.(*writeWorker).runWriteLSM"),
		goleak.IgnoreTopFunction("github.com/pingcap/badger.(*writeWorker).runWriteVLog"),
		goleak.IgnoreTopFunction("github.com/pingcap/badger.Open.func4"),
		goleak.IgnoreTopFunction("github.com/pingcap/badger/epoch.(*ResourceManager).collectLoop"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/ddl.(*ddl).limitDDLJobs"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/ddl.(*delRange).startEmulator"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/ddl.(*worker).start"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/domain.(*Domain).LoadPrivilegeLoop.func1"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/domain.(*Domain).LoadSysVarCacheLoop.func1"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/domain.(*Domain).TelemetryReportLoop.func1"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/domain.(*Domain).TelemetryRotateSubWindowLoop.func1"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/domain.(*Domain).globalBindHandleWorkerLoop.func1"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/domain.(*Domain).handleEvolvePlanTasksLoop.func1"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/domain.(*Domain).infoSyncerKeeper"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/domain.(*Domain).topNSlowQueryLoop"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/domain.(*Domain).topologySyncerKeeper"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/executor_test.testSerialSuite.TestTemporaryTableNoNetwork.func2"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/store/mockstore/unistore/tikv.(*MVCCStore).StartDeadlockDetection.func1"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/store/mockstore/unistore/tikv.(*MVCCStore).runUpdateSafePointLoop"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/store/mockstore/unistore/tikv.writeDBWorker.run"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/store/mockstore/unistore/tikv.writeLockWorker.run"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/internal/locate.(*RegionCache).asyncCheckAndResolveLoop"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/oracle/oracles.(*pdOracle).updateTS"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/tikv.(*KVStore).runSafePointChecker"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/tikv.(*KVStore).safeTSUpdater"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/pkg/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("gopkg.in/natefinch/lumberjack%2ev2.(*Logger).millRun"),
	}
	goleak.VerifyTestMain(m, opts...)
}
