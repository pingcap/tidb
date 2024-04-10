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

package servertestkit

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	srv "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/internal/testserverclient"
	"github.com/pingcap/tidb/pkg/server/internal/testutil"
	"github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/cpuprofile"
	"github.com/pingcap/tidb/pkg/util/topsql/collector/mock"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
)

// TidbTestSuite is a test suite for tidb
type TidbTestSuite struct {
	*testserverclient.TestServerClient
	Tidbdrv *srv.TiDBDriver
	Server  *srv.Server
	Domain  *domain.Domain
	Store   kv.Storage
}

// CreateTidbTestSuite creates a test suite for tidb
func CreateTidbTestSuite(t *testing.T) *TidbTestSuite {
	cfg := util.NewTestConfig()
	cfg.Port = 0
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = 0
	cfg.Status.RecordDBLabel = true
	cfg.Performance.TCPKeepAlive = true
	return CreateTidbTestSuiteWithCfg(t, cfg)
}

// CreateTidbTestSuiteWithCfg creates a test suite for tidb with config
func CreateTidbTestSuiteWithCfg(t *testing.T, cfg *config.Config) *TidbTestSuite {
	ts := &TidbTestSuite{TestServerClient: testserverclient.NewTestServerClient()}

	// setup tidbTestSuite
	var err error
	ts.Store, err = mockstore.NewMockStore()
	session.DisableStats4Test()
	require.NoError(t, err)
	ts.Domain, err = session.BootstrapSession(ts.Store)
	require.NoError(t, err)
	ts.Tidbdrv = srv.NewTiDBDriver(ts.Store)

	server, err := srv.NewServer(cfg, ts.Tidbdrv)
	require.NoError(t, err)

	ts.Server = server
	ts.Server.SetDomain(ts.Domain)
	ts.Domain.InfoSyncer().SetSessionManager(ts.Server)
	go func() {
		err := ts.Server.Run(nil)
		require.NoError(t, err)
	}()
	<-srv.RunInGoTestChan
	ts.Port = testutil.GetPortFromTCPAddr(server.ListenAddr())
	ts.StatusPort = testutil.GetPortFromTCPAddr(server.StatusListenerAddr())
	ts.WaitUntilServerOnline()

	t.Cleanup(func() {
		if ts.Domain != nil {
			ts.Domain.Close()
		}
		if ts.Server != nil {
			ts.Server.Close()
		}
		if ts.Store != nil {
			require.NoError(t, ts.Store.Close())
		}
		view.Stop()
	})
	return ts
}

type tidbTestTopSQLSuite struct {
	*TidbTestSuite
}

// CreateTidbTestTopSQLSuite creates a test suite for top-sql test.
func CreateTidbTestTopSQLSuite(t *testing.T) *tidbTestTopSQLSuite {
	base := CreateTidbTestSuite(t)

	ts := &tidbTestTopSQLSuite{base}

	// Initialize global variable for top-sql test.
	db, err := sql.Open("mysql", ts.GetDSN())
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	dbt := testkit.NewDBTestKit(t, db)
	topsqlstate.GlobalState.PrecisionSeconds.Store(1)
	topsqlstate.GlobalState.ReportIntervalSeconds.Store(2)
	dbt.MustExec("set @@global.tidb_top_sql_max_time_series_count=5;")

	require.NoError(t, cpuprofile.StartCPUProfiler())
	t.Cleanup(func() {
		cpuprofile.StopCPUProfiler()
		topsqlstate.GlobalState.PrecisionSeconds.Store(topsqlstate.DefTiDBTopSQLPrecisionSeconds)
		topsqlstate.GlobalState.ReportIntervalSeconds.Store(topsqlstate.DefTiDBTopSQLReportIntervalSeconds)
		view.Stop()
	})
	return ts
}

// TestCase is to run the test case for top-sql test.
func (ts *tidbTestTopSQLSuite) TestCase(t *testing.T, mc *mock.TopSQLCollector, execFn func(db *sql.DB), checkFn func()) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		ts.loopExec(ctx, t, execFn)
	}()

	checkFn()
	cancel()
	wg.Wait()
	mc.Reset()
}

func (ts *tidbTestTopSQLSuite) loopExec(ctx context.Context, t *testing.T, fn func(db *sql.DB)) {
	db, err := sql.Open("mysql", ts.GetDSN())
	require.NoError(t, err, "Error connecting")
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()
	dbt := testkit.NewDBTestKit(t, db)
	dbt.MustExec("use topsql;")
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		fn(db)
	}
}
