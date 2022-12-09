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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/testkit/testmain"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/atomic"
	"go.uber.org/goleak"
)

var testDataMap = make(testdata.BookKeeper, 1)

func TestMain(m *testing.M) {
	testmain.ShortCircuitForBench(m)

	testsetup.SetupForCommonTest()

	flag.Parse()
	testDataMap.LoadTestSuiteData("testdata", "clustered_index_suite")

	SetSchemaLease(20 * time.Millisecond)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.AsyncCommit.SafeWindow = 0
		conf.TiKVClient.AsyncCommit.AllowedClockDrift = 0
	})
	tikv.EnableFailpoints()
	opts := []goleak.Option{
		// TODO: figure the reason and shorten this list
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/internal/retry.newBackoffFn.func1"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/v3.waitRetryBackoff"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*controlBuffer).get"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*http2Client).keepalive"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("github.com/tikv/client-go/v2/txnkv/transaction.keepAlive"),
	}
	callback := func(i int) int {
		// wait for MVCCLevelDB to close, MVCCLevelDB will be closed in one second
		time.Sleep(time.Second)
		testDataMap.GenerateOutputIfNeeded()
		return i
	}
	goleak.VerifyTestMain(testmain.WrapTestingM(m, callback), opts...)
}

func GetClusteredIndexSuiteData() testdata.TestData {
	return testDataMap["clustered_index_suite"]
}

func createStoreAndBootstrap(t *testing.T) (kv.Storage, *domain.Domain) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	dom, err := BootstrapSession(store)
	require.NoError(t, err)
	return store, dom
}

var sessionKitIDGenerator atomic.Uint64

func createSessionAndSetID(t *testing.T, store kv.Storage) Session {
	se, err := CreateSession4Test(store)
	se.SetConnectionID(sessionKitIDGenerator.Inc())
	require.NoError(t, err)
	return se
}

func mustExec(t *testing.T, se Session, sql string, args ...interface{}) {
	rs, err := exec(se, sql, args...)
	require.NoError(t, err)
	if rs != nil {
		require.NoError(t, rs.Close())
	}
}

func mustExecToRecodeSet(t *testing.T, se Session, sql string, args ...interface{}) sqlexec.RecordSet {
	rs, err := exec(se, sql, args...)
	require.NoError(t, err)
	return rs
}

func exec(se Session, sql string, args ...interface{}) (sqlexec.RecordSet, error) {
	ctx := context.Background()
	if len(args) == 0 {
		rs, err := se.Execute(ctx, sql)
		if err == nil && len(rs) > 0 {
			return rs[0], nil
		}
		return nil, err
	}
	stmtID, _, _, err := se.PrepareStmt(sql)
	if err != nil {
		return nil, err
	}
	params := expression.Args2Expressions4Test(args...)
	rs, err := se.ExecutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func match(t *testing.T, row []types.Datum, expected ...interface{}) {
	require.Len(t, row, len(expected))
	for i := range row {
		if _, ok := expected[i].(time.Time); ok {
			// Since password_last_changed is set to default current_timestamp, we pass this check.
			continue
		}
		got := fmt.Sprintf("%v", row[i].GetValue())
		need := fmt.Sprintf("%v", expected[i])
		require.Equal(t, need, got, i)
	}
}
