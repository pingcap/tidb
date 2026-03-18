// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/coreos/go-semver/semver"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestDumpExit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	mock.ExpectQuery(fmt.Sprintf("SHOW CREATE DATABASE `%s`", escapeString(database))).
		WillDelayFor(time.Second).
		WillReturnRows(sqlmock.NewRows([]string{"Database", "Create Database"}).
			AddRow("test", "CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */"))
	mock.ExpectQuery(fmt.Sprintf("SELECT DEFAULT_COLLATION_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s'", escapeString(database))).
		WillReturnRows(sqlmock.NewRows([]string{"DEFAULT_COLLATION_NAME"}).
			AddRow("utf8mb4_bin"))

	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, true, nil)

	d := &Dumper{
		tctx:      tctx,
		conf:      DefaultConfig(),
		cancelCtx: cancel,
	}
	wg, writingCtx := errgroup.WithContext(tctx)
	writerErr := errors.New("writer error")

	wg.Go(func() error {
		return errors.Trace(writerErr)
	})
	wg.Go(func() error {
		time.Sleep(time.Second)
		return context.Canceled
	})

	writerCtx := tctx.WithContext(writingCtx)
	taskChan := make(chan Task, 1)
	taskChan <- &TaskDatabaseMeta{}
	d.conf.Tables = DatabaseTables{}.AppendTable(database, nil)
	d.conf.ServerInfo.ServerType = version.ServerTypeMySQL
	require.ErrorIs(t, wg.Wait(), writerErr)
	// if writerCtx is canceled , QuerySQL in `dumpDatabases` will return sqlmock.ErrCancelled
	require.ErrorIs(t, d.dumpDatabases(writerCtx, baseConn, taskChan), sqlmock.ErrCancelled)
}

func TestTiDBResolveKeyspaceMetaForGC(t *testing.T) {
	cases := []struct {
		name          string
		keyspaceMeta  []string // [name,id]
		queryErr      error
		confPD        string
		expectErr     string
		expectKSPName string
		expectKSPID   uint32
	}{
		{
			name:          "premium_ok",
			keyspaceMeta:  []string{"ks1", "123"},
			confPD:        "pd1:2379,pd2:2379",
			expectKSPName: "ks1",
			expectKSPID:   123,
		},
		{
			name:         "premium_missing_pd",
			keyspaceMeta: []string{"ks1", "123"},
			confPD:       "",
			expectErr:    "requires --pd",
		},
		{
			name:         "classical_ok",
			keyspaceMeta: []string{"", ""},
			confPD:       "",
		},
		{
			name:         "classical_with_pd_is_error",
			keyspaceMeta: []string{"", ""},
			confPD:       "pd1:2379",
			expectErr:    "classical cluster must not specify",
		},
		{
			name:     "classical_no_keyspace_meta_table",
			queryErr: &mysql.MySQLError{Number: ErrNoSuchTable, Message: "Table 'information_schema.KEYSPACE_META' doesn't exist"},
			confPD:   "",
		},
		{
			name:      "premium_no_keyspace_meta_table_is_error",
			queryErr:  &mysql.MySQLError{Number: ErrNoSuchTable, Message: "Table 'information_schema.KEYSPACE_META' doesn't exist"},
			confPD:    "pd1:2379",
			expectErr: "KEYSPACE_META",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			query := "SELECT KEYSPACE_NAME, KEYSPACE_ID FROM information_schema.KEYSPACE_META;"
			if tc.queryErr != nil {
				mock.ExpectQuery(query).WillReturnError(tc.queryErr)
			} else {
				mock.ExpectQuery(query).WillReturnRows(
					sqlmock.NewRows([]string{"KEYSPACE_NAME", "KEYSPACE_ID"}).AddRow(tc.keyspaceMeta[0], tc.keyspaceMeta[1]),
				)
			}

			tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
			defer cancel()
			d := &Dumper{
				tctx:      tctx,
				cancelCtx: cancel,
				conf:      DefaultConfig(),
				dbHandle:  db,
			}
			d.conf.ServerInfo = version.ServerInfo{
				ServerType:    version.ServerTypeTiDB,
				ServerVersion: gcSafePointVersion,
			}
			d.conf.PDAddr = tc.confPD
			err = tidbResolveKeyspaceMetaForGC(d)
			if tc.expectErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectKSPName, d.tidbKeyspaceName)
				require.Equal(t, tc.expectKSPID, d.tidbKeyspaceID)
			}
			mock.ExpectClose()
			require.NoError(t, db.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestResolveKeyspaceMetaGCAPIChoice verifies that the resolved keyspace
// metadata determines which GC API (global vs keyspace-level) dumpling will
// use.  For a premium cluster the dispatching function must launch
// updateKeyspaceGCBarrier; for a classical cluster it must launch
// updateServiceSafePoint.
func TestResolveKeyspaceMetaGCAPIChoice(t *testing.T) {
	cases := []struct {
		name         string
		keyspaceMeta []string // [name, id] from KEYSPACE_META
		confPD       string
		// After resolving we expect these on the Dumper.
		expectKeyspace   string
		expectID         uint32
		useKeyspaceGC    bool
		expectBarrierAPI bool
	}{
		{
			name:             "premium_uses_keyspace_barrier_api",
			keyspaceMeta:     []string{"ks1", "42"},
			confPD:           "pd1:2379",
			expectKeyspace:   "ks1",
			expectID:         42,
			useKeyspaceGC:    true,
			expectBarrierAPI: true,
		},
		{
			name:             "resolved_keyspace_meta_without_keyspace_gc_mode_uses_global_safepoint_api",
			keyspaceMeta:     []string{"ks1", "42"},
			confPD:           "pd1:2379",
			expectKeyspace:   "ks1",
			expectID:         42,
			useKeyspaceGC:    false,
			expectBarrierAPI: false,
		},
		{
			name:             "classical_uses_global_safepoint_api",
			keyspaceMeta:     []string{"", ""},
			confPD:           "",
			expectKeyspace:   "",
			expectID:         0,
			useKeyspaceGC:    false,
			expectBarrierAPI: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer func() {
				mock.ExpectClose()
				require.NoError(t, db.Close())
			}()

			query := "SELECT KEYSPACE_NAME, KEYSPACE_ID FROM information_schema.KEYSPACE_META;"
			mock.ExpectQuery(query).WillReturnRows(
				sqlmock.NewRows([]string{"KEYSPACE_NAME", "KEYSPACE_ID"}).
					AddRow(tc.keyspaceMeta[0], tc.keyspaceMeta[1]),
			)

			tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
			defer cancel()

			d := &Dumper{
				tctx:      tctx,
				cancelCtx: cancel,
				conf:      DefaultConfig(),
				dbHandle:  db,
			}
			d.conf.ServerInfo = version.ServerInfo{
				ServerType:    version.ServerTypeTiDB,
				ServerVersion: gcSafePointVersion,
			}
			d.conf.PDAddr = tc.confPD
			err = tidbResolveKeyspaceMetaForGC(d)
			require.NoError(t, err)
			require.Equal(t, tc.expectKeyspace, d.tidbKeyspaceName)
			require.Equal(t, tc.expectID, d.tidbKeyspaceID)

			// Simulate the PD client being set already (we don't test actual
			// PD connections here, just the dispatch decision).
			mockPD := newMockPDClientForGC()
			d.tidbPDClientForGC = mockPD
			d.tidbUseKeyspaceGC = tc.useKeyspaceGC
			d.conf.Snapshot = "438324008696225793"

			// Mock parseSnapshotToTSO: conf.Snapshot is a numeric TSO so it
			// parses without DB roundtrip.
			err = tidbStartGCSavepointUpdateService(d)
			require.NoError(t, err)

			// Give the background goroutine a moment to make its first call.
			time.Sleep(200 * time.Millisecond)

			if tc.expectBarrierAPI {
				// Keyspace barrier path: SetGCBarrier must have been called.
				mockPD.gcStatesClient.mu.Lock()
				require.Greater(t, mockPD.gcStatesClient.setCalls, 0,
					"expected SetGCBarrier to be called for premium cluster")
				mockPD.gcStatesClient.mu.Unlock()

				mockPD.mu.Lock()
				require.Equal(t, 0, mockPD.updateSafePointCalls,
					"UpdateServiceGCSafePoint must NOT be called for premium cluster")
				mockPD.mu.Unlock()
			} else {
				// Global safe point path: UpdateServiceGCSafePoint must have been called.
				mockPD.mu.Lock()
				require.Greater(t, mockPD.updateSafePointCalls, 0,
					"expected UpdateServiceGCSafePoint to be called for classical cluster")
				mockPD.mu.Unlock()

				mockPD.gcStatesClient.mu.Lock()
				require.Equal(t, 0, mockPD.gcStatesClient.setCalls,
					"SetGCBarrier must NOT be called for classical cluster")
				mockPD.gcStatesClient.mu.Unlock()
			}

			// Cancel to stop the background goroutine.
			cancel()
		})
	}
}

func TestPDSecurityOptionForGC(t *testing.T) {
	cases := []struct {
		name               string
		sqlCAPath          string
		sqlClientCertPath  string
		sqlClientKeyPath   string
		clusterSSLCAPath   string
		clusterSSLCertPath string
		clusterSSLKeyPath  string
		expectedCAPath     string
		expectedCert       string
		expectedKeyPath    string
	}{
		{
			name:              "reuse_sql_tls_when_cluster_tls_not_set",
			sqlCAPath:         "/tmp/sql-ca.pem",
			sqlClientCertPath: "/tmp/client-cert.pem",
			sqlClientKeyPath:  "/tmp/client-key.pem",
			expectedCAPath:    "/tmp/sql-ca.pem",
			expectedCert:      "/tmp/client-cert.pem",
			expectedKeyPath:   "/tmp/client-key.pem",
		},
		{
			name:              "override_cluster_ca_but_reuse_existing_client_cert_and_key",
			sqlCAPath:         "/tmp/sql-ca.pem",
			sqlClientCertPath: "/tmp/client-cert.pem",
			sqlClientKeyPath:  "/tmp/client-key.pem",
			clusterSSLCAPath:  "/tmp/cluster-ca.pem",
			expectedCAPath:    "/tmp/cluster-ca.pem",
			expectedCert:      "/tmp/client-cert.pem",
			expectedKeyPath:   "/tmp/client-key.pem",
		},
		{
			name:               "override_all_cluster_tls_material",
			sqlCAPath:          "/tmp/sql-ca.pem",
			sqlClientCertPath:  "/tmp/client-cert.pem",
			sqlClientKeyPath:   "/tmp/client-key.pem",
			clusterSSLCAPath:   "/tmp/cluster-ca.pem",
			clusterSSLCertPath: "/tmp/cluster-cert.pem",
			clusterSSLKeyPath:  "/tmp/cluster-key.pem",
			expectedCAPath:     "/tmp/cluster-ca.pem",
			expectedCert:       "/tmp/cluster-cert.pem",
			expectedKeyPath:    "/tmp/cluster-key.pem",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conf := DefaultConfig()
			conf.Security.CAPath = tc.sqlCAPath
			conf.Security.CertPath = tc.sqlClientCertPath
			conf.Security.KeyPath = tc.sqlClientKeyPath
			conf.ClusterSSLCA = tc.clusterSSLCAPath
			conf.ClusterSSLCert = tc.clusterSSLCertPath
			conf.ClusterSSLKey = tc.clusterSSLKeyPath

			securityOpt := pdSecurityOptionForGC(conf)
			require.Equal(t, tc.expectedCAPath, securityOpt.CAPath)
			require.Equal(t, tc.expectedCert, securityOpt.CertPath)
			require.Equal(t, tc.expectedKeyPath, securityOpt.KeyPath)
		})
	}
}

func TestParseClusterSSLFlags(t *testing.T) {
	conf := DefaultConfig()
	flags := pflag.NewFlagSet("dumpling", pflag.ContinueOnError)
	conf.DefineFlags(flags)
	oldCommandLine := pflag.CommandLine
	pflag.CommandLine = flags
	defer func() {
		pflag.CommandLine = oldCommandLine
	}()
	require.NoError(t, flags.Parse([]string{
		"--pd", "pd1:2379",
		"--cluster-ssl-ca", "/tmp/cluster-ca.pem",
		"--cluster-ssl-cert", "/tmp/cluster-cert.pem",
		"--cluster-ssl-key", "/tmp/cluster-key.pem",
	}))
	require.NoError(t, conf.ParseFromFlags(flags))
	require.Equal(t, "pd1:2379", conf.PDAddr)
	require.Equal(t, "/tmp/cluster-ca.pem", conf.ClusterSSLCA)
	require.Equal(t, "/tmp/cluster-cert.pem", conf.ClusterSSLCert)
	require.Equal(t, "/tmp/cluster-key.pem", conf.ClusterSSLKey)
}

// TestUpdateServiceSafePointRetryAndCancel verifies that the global-GC safe
// point updater retries on transient failures and performs cleanup when the
// context is cancelled.
func TestUpdateServiceSafePointRetryAndCancel(t *testing.T) {
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()

	mockPD := newMockPDClientForGC()

	// Inject a transient error for the first 3 calls, then succeed.
	var callCount atomic.Int32
	transientErr := errors.New("transient PD error")
	mockPD.mu.Lock()
	mockPD.updateSafePointErr = transientErr
	mockPD.mu.Unlock()

	go func() {
		// After 3 calls, clear the error so the retry loop can succeed.
		for {
			if callCount.Load() >= 3 {
				mockPD.mu.Lock()
				mockPD.updateSafePointErr = nil
				mockPD.mu.Unlock()
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wrap UpdateServiceGCSafePoint to count calls including failures.
	origUpdate := mockPD.UpdateServiceGCSafePoint
	_ = origUpdate // ensure the method exists
	// We can't easily wrap the method, so instead track via the mock's counter
	// and poll it. The mock already counts calls.

	snapshotTS := uint64(100)
	// Use a very short TTL so the update interval (ttl/2) is small.
	ttl := int64(2) // 2 seconds → update interval = 1s

	go updateServiceSafePoint(tctx, mockPD, ttl, snapshotTS)

	// Wait for retries + at least one success.
	require.Eventually(t, func() bool {
		mockPD.mu.Lock()
		defer mockPD.mu.Unlock()
		callCount.Store(int32(mockPD.updateSafePointCalls))
		return mockPD.updateSafePointCalls >= 4 && mockPD.updateSafePointErr == nil
	}, 15*time.Second, 100*time.Millisecond, "expected retry then success")

	// Verify the safe point TS was snapshotTS - 1.
	mockPD.mu.Lock()
	require.Equal(t, snapshotTS-1, mockPD.lastSafePointTS)
	require.Equal(t, ttl, mockPD.lastSafePointTTL)
	mockPD.mu.Unlock()

	// Cancel context — the updater should exit and do cleanup (TTL=0 call).
	cancel()
	time.Sleep(300 * time.Millisecond)

	// The cleanup call uses TTL=0 and safePoint=0.
	mockPD.mu.Lock()
	require.Equal(t, int64(0), mockPD.lastSafePointTTL, "cleanup must set TTL to 0")
	require.Equal(t, uint64(0), mockPD.lastSafePointTS, "cleanup must set safePoint to 0")
	mockPD.mu.Unlock()
}

// TestUpdateKeyspaceGCBarrierRetryAndCancel verifies that the keyspace-level
// GC barrier updater retries on transient failures and performs cleanup
// (DeleteGCBarrier) when the context is cancelled.
func TestUpdateKeyspaceGCBarrierRetryAndCancel(t *testing.T) {
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()

	mockPD := newMockPDClientForGC()
	gcClient := mockPD.gcStatesClient

	// Inject a transient error for the first 2 SetGCBarrier calls.
	transientErr := errors.New("transient barrier error")
	gcClient.mu.Lock()
	gcClient.setBarrierErr = transientErr
	gcClient.mu.Unlock()

	go func() {
		// After 2 calls, clear the error.
		for {
			gcClient.mu.Lock()
			calls := gcClient.setCalls
			gcClient.mu.Unlock()
			if calls >= 2 {
				gcClient.mu.Lock()
				gcClient.setBarrierErr = nil
				gcClient.mu.Unlock()
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	snapshotTS := uint64(200)
	keyspaceID := uint32(42)
	ttl := int64(2) // 2 seconds

	go updateKeyspaceGCBarrier(tctx, mockPD, keyspaceID, ttl, snapshotTS)

	// Wait for retries + at least one success.
	require.Eventually(t, func() bool {
		gcClient.mu.Lock()
		defer gcClient.mu.Unlock()
		return gcClient.setCalls >= 3 && gcClient.setBarrierErr == nil
	}, 15*time.Second, 100*time.Millisecond, "expected retry then success for barrier")

	// Verify the barrier TS was snapshotTS - 1.
	gcClient.mu.Lock()
	require.NotNil(t, gcClient.setBarrierInfo)
	require.Equal(t, snapshotTS-1, gcClient.setBarrierInfo.BarrierTS)
	gcClient.mu.Unlock()

	// Cancel context — updater should exit and call DeleteGCBarrier for cleanup.
	cancel()
	time.Sleep(300 * time.Millisecond)

	gcClient.mu.Lock()
	require.Greater(t, gcClient.delCalls, 0, "DeleteGCBarrier must be called on cancel")
	gcClient.mu.Unlock()
}

// TestUpdateServiceSafePointSnapshotZero verifies that when snapshotTS == 0, the
// safe point TS stays 0 (no underflow).
func TestUpdateServiceSafePointSnapshotZero(t *testing.T) {
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()

	mockPD := newMockPDClientForGC()
	ttl := int64(2)

	go updateServiceSafePoint(tctx, mockPD, ttl, 0)

	require.Eventually(t, func() bool {
		mockPD.mu.Lock()
		defer mockPD.mu.Unlock()
		return mockPD.updateSafePointCalls >= 1
	}, 5*time.Second, 50*time.Millisecond)

	mockPD.mu.Lock()
	require.Equal(t, uint64(0), mockPD.lastSafePointTS, "snapshotTS=0 must not underflow")
	mockPD.mu.Unlock()

	cancel()
}

// TestUpdateKeyspaceGCBarrierSnapshotZero verifies that when snapshotTS == 0,
// the barrier TS stays 0 (no underflow).
func TestUpdateKeyspaceGCBarrierSnapshotZero(t *testing.T) {
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()

	mockPD := newMockPDClientForGC()
	ttl := int64(2)

	go updateKeyspaceGCBarrier(tctx, mockPD, 1, ttl, 0)

	require.Eventually(t, func() bool {
		mockPD.gcStatesClient.mu.Lock()
		defer mockPD.gcStatesClient.mu.Unlock()
		return mockPD.gcStatesClient.setCalls >= 1
	}, 5*time.Second, 50*time.Millisecond)

	mockPD.gcStatesClient.mu.Lock()
	require.NotNil(t, mockPD.gcStatesClient.setBarrierInfo)
	require.Equal(t, uint64(0), mockPD.gcStatesClient.setBarrierInfo.BarrierTS,
		"snapshotTS=0 must not underflow")
	mockPD.gcStatesClient.mu.Unlock()

	cancel()
}

// TestUpdateKeyspaceGCBarrierCancelDuringRetry verifies that cancelling the
// context mid-retry causes the updater to exit promptly and still perform
// cleanup via DeleteGCBarrier.
func TestUpdateKeyspaceGCBarrierCancelDuringRetry(t *testing.T) {
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()

	mockPD := newMockPDClientForGC()
	gcClient := mockPD.gcStatesClient
	// Make SetGCBarrier always fail so we stay in the retry loop.
	gcClient.setBarrierErr = errors.New("permanent failure")

	done := make(chan struct{})
	go func() {
		updateKeyspaceGCBarrier(tctx, mockPD, 99, 2, 500)
		close(done)
	}()

	// Wait until at least one retry attempt.
	require.Eventually(t, func() bool {
		gcClient.mu.Lock()
		defer gcClient.mu.Unlock()
		return gcClient.setCalls >= 1
	}, 5*time.Second, 50*time.Millisecond)

	// Cancel during retries.
	cancel()
	select {
	case <-done:
		// Goroutine exited.
	case <-time.After(5 * time.Second):
		t.Fatal("updateKeyspaceGCBarrier did not exit after cancel")
	}

	// Cleanup must have been called.
	gcClient.mu.Lock()
	require.Greater(t, gcClient.delCalls, 0, "DeleteGCBarrier must be called even on mid-retry cancel")
	gcClient.mu.Unlock()
}

// TestUpdateServiceSafePointCancelDuringRetry verifies that cancelling the
// context mid-retry causes the global safe point updater to exit promptly and
// still perform cleanup (TTL=0 call).
func TestUpdateServiceSafePointCancelDuringRetry(t *testing.T) {
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()

	mockPD := newMockPDClientForGC()
	// Make all updates fail so we stay in the retry loop.
	mockPD.updateSafePointErr = errors.New("permanent failure")

	done := make(chan struct{})
	go func() {
		updateServiceSafePoint(tctx, mockPD, 2, 500)
		close(done)
	}()

	// Wait until at least one retry attempt.
	require.Eventually(t, func() bool {
		mockPD.mu.Lock()
		defer mockPD.mu.Unlock()
		return mockPD.updateSafePointCalls >= 1
	}, 5*time.Second, 50*time.Millisecond)

	// Cancel during retries.
	cancel()
	select {
	case <-done:
		// Goroutine exited.
	case <-time.After(5 * time.Second):
		t.Fatal("updateServiceSafePoint did not exit after cancel")
	}

	// Cleanup call uses TTL=0.
	mockPD.mu.Lock()
	require.Equal(t, int64(0), mockPD.lastSafePointTTL, "cleanup must set TTL to 0")
	mockPD.mu.Unlock()
}

func TestDumpTableMeta(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()
	conn, err := db.Conn(tctx)
	require.NoError(t, err)
	baseConn := newBaseConn(conn, true, nil)

	conf := DefaultConfig()
	conf.NoSchemas = true

	for serverType := version.ServerTypeUnknown; serverType < version.ServerTypeAll; serverType++ {
		conf.ServerInfo.ServerType = version.ServerType(serverType)
		hasImplicitRowID := false
		mock.ExpectQuery("SHOW COLUMNS FROM").
			WillReturnRows(sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
				AddRow("id", "int(11)", "NO", "PRI", nil, ""))
		if serverType == version.ServerTypeTiDB {
			mock.ExpectExec("SELECT _tidb_rowid from").
				WillReturnResult(sqlmock.NewResult(0, 0))
			hasImplicitRowID = true
		}
		mock.ExpectQuery(fmt.Sprintf("SELECT \\* FROM `%s`.`%s`", database, table)).
			WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
		meta, err := dumpTableMeta(tctx, conf, baseConn, database, &TableInfo{Type: TableTypeBase, Name: table})
		require.NoError(t, err)
		require.Equal(t, database, meta.DatabaseName())
		require.Equal(t, table, meta.TableName())
		require.Equal(t, "*", meta.SelectedField())
		require.Equal(t, 1, meta.SelectedLen())
		require.Equal(t, "", meta.ShowCreateTable())
		require.Equal(t, hasImplicitRowID, meta.HasImplicitRowID())
	}
}

func TestGetListTableTypeByConf(t *testing.T) {
	conf := defaultConfigForTest(t)
	cases := []struct {
		serverInfo  version.ServerInfo
		consistency string
		expected    listTableType
	}{
		{version.ParseServerInfo("5.7.25-TiDB-3.0.6"), ConsistencyTypeSnapshot, listTableByShowTableStatus},
		// no bug version
		{version.ParseServerInfo("8.0.2"), ConsistencyTypeLock, listTableByInfoSchema},
		{version.ParseServerInfo("8.0.2"), ConsistencyTypeFlush, listTableByShowTableStatus},
		{version.ParseServerInfo("8.0.23"), ConsistencyTypeNone, listTableByShowTableStatus},

		// bug version
		{version.ParseServerInfo("8.0.3"), ConsistencyTypeLock, listTableByInfoSchema},
		{version.ParseServerInfo("8.0.3"), ConsistencyTypeFlush, listTableByShowFullTables},
		{version.ParseServerInfo("8.0.3"), ConsistencyTypeNone, listTableByShowTableStatus},
	}

	for _, x := range cases {
		conf.Consistency = x.consistency
		conf.ServerInfo = x.serverInfo
		require.Equalf(t, x.expected, getListTableTypeByConf(conf), "server info: %s, consistency: %s", x.serverInfo, x.consistency)
	}
}

func TestAdjustDatabaseCollation(t *testing.T) {
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()
	parser1 := parser.New()

	originSQLs := []string{
		"create database `test` CHARACTER SET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"create database `test` CHARACTER SET=utf8mb4",
	}

	expectedSQLs := []string{
		"create database `test` CHARACTER SET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"CREATE DATABASE `test` CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci",
	}
	charsetAndDefaultCollationMap := map[string]string{"utf8mb4": "utf8mb4_general_ci"}

	for _, originSQL := range originSQLs {
		newSQL, err := adjustDatabaseCollation(tctx, LooseCollationCompatible, parser1, originSQL, charsetAndDefaultCollationMap)
		require.NoError(t, err)
		require.Equal(t, originSQL, newSQL)
	}

	for i, originSQL := range originSQLs {
		newSQL, err := adjustDatabaseCollation(tctx, StrictCollationCompatible, parser1, originSQL, charsetAndDefaultCollationMap)
		require.NoError(t, err)
		require.Equal(t, expectedSQLs[i], newSQL)
	}
}

func TestAdjustTableCollation(t *testing.T) {
	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()

	parser1 := parser.New()

	originSQLs := []string{
		"create table `test`.`t1` (id int) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"create table `test`.`t1` (id int) CHARSET=utf8mb4",
		"create table `test`.`t1` (id int, name varchar(20) CHARACTER SET utf8mb4, work varchar(20)) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ",
		"create table `test`.`t1` (id int, name varchar(20), work varchar(20)) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"create table `test`.`t1` (id int, name varchar(20) COLLATE utf8mb4_general_ci, work varchar(20)) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"create table `test`.`t1` (id int, name varchar(20) COLLATE utf8mb4_general_ci, work varchar(20) CHARACTER SET utf8mb4) CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		"create table `test`.`t1` (id int, name varchar(20) CHARACTER SET utf8mb4, work varchar(20)) CHARSET=utf8mb4 ",
		"create table `test`.`t1` (id int, name varchar(20), work varchar(20)) CHARSET=utf8mb4",
		"create table `test`.`t1` (id int, name varchar(20) COLLATE utf8mb4_general_ci, work varchar(20)) CHARSET=utf8mb4",
		"create table `test`.`t1` (id int, name varchar(20) COLLATE utf8mb4_general_ci, work varchar(20) CHARACTER SET utf8mb4) CHARSET=utf8mb4",
		"create table `test`.`t1` (name varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin) CHARSET=latin1 COLLATE=latin1_bin",
	}

	expectedSQLs := []string{
		"CREATE TABLE `test`.`t1` (`id` INT) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20) CHARACTER SET UTF8MB4 COLLATE utf8mb4_general_ci,`work` VARCHAR(20)) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20),`work` VARCHAR(20)) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20) COLLATE utf8mb4_general_ci,`work` VARCHAR(20)) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20) COLLATE utf8mb4_general_ci,`work` VARCHAR(20) CHARACTER SET UTF8MB4 COLLATE utf8mb4_general_ci) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20) CHARACTER SET UTF8MB4 COLLATE utf8mb4_general_ci,`work` VARCHAR(20)) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20),`work` VARCHAR(20)) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20) COLLATE utf8mb4_general_ci,`work` VARCHAR(20)) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`id` INT,`name` VARCHAR(20) COLLATE utf8mb4_general_ci,`work` VARCHAR(20) CHARACTER SET UTF8MB4 COLLATE utf8mb4_general_ci) DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_GENERAL_CI",
		"CREATE TABLE `test`.`t1` (`name` VARCHAR(20) CHARACTER SET UTF8MB4 COLLATE utf8mb4_bin) DEFAULT CHARACTER SET = LATIN1 DEFAULT COLLATE = LATIN1_BIN",
	}

	charsetAndDefaultCollationMap := map[string]string{"utf8mb4": "utf8mb4_general_ci"}

	for _, originSQL := range originSQLs {
		newSQL, err := adjustTableCollation(tctx, LooseCollationCompatible, parser1, originSQL, charsetAndDefaultCollationMap)
		require.NoError(t, err)
		require.Equal(t, originSQL, newSQL)
	}

	for i, originSQL := range originSQLs {
		newSQL, err := adjustTableCollation(tctx, StrictCollationCompatible, parser1, originSQL, charsetAndDefaultCollationMap)
		require.NoError(t, err)
		require.Equal(t, expectedSQLs[i], newSQL)
	}
}

func TestUnregisterMetrics(t *testing.T) {
	ctx := context.Background()
	conf := &Config{
		SQL:          "not empty",
		Where:        "not empty",
		PromFactory:  promutil.NewDefaultFactory(),
		PromRegistry: promutil.NewDefaultRegistry(),
	}

	_, err := NewDumper(ctx, conf)
	require.Error(t, err)
	_, err = NewDumper(ctx, conf)
	// should not panic
	require.Error(t, err)
}

func TestSetDefaultSessionParams(t *testing.T) {
	testCases := []struct {
		si             version.ServerInfo
		sessionParams  map[string]any
		expectedParams map[string]any
	}{
		{
			si: version.ServerInfo{
				ServerType:    version.ServerTypeTiDB,
				HasTiKV:       true,
				ServerVersion: semver.New("6.1.0"),
			},
			sessionParams: map[string]any{
				"tidb_snapshot": "2020-01-01 00:00:00",
			},
			expectedParams: map[string]any{
				"tidb_snapshot": "2020-01-01 00:00:00",
			},
		},
		{
			si: version.ServerInfo{
				ServerType:    version.ServerTypeTiDB,
				HasTiKV:       true,
				ServerVersion: semver.New("6.2.0"),
			},
			sessionParams: map[string]any{
				"tidb_snapshot": "2020-01-01 00:00:00",
			},
			expectedParams: map[string]any{
				"tidb_enable_paging": "ON",
				"tidb_snapshot":      "2020-01-01 00:00:00",
			},
		},
		{
			si: version.ServerInfo{
				ServerType:    version.ServerTypeTiDB,
				HasTiKV:       true,
				ServerVersion: semver.New("6.2.0"),
			},
			sessionParams: map[string]any{
				"tidb_enable_paging": "OFF",
				"tidb_snapshot":      "2020-01-01 00:00:00",
			},
			expectedParams: map[string]any{
				"tidb_enable_paging": "OFF",
				"tidb_snapshot":      "2020-01-01 00:00:00",
			},
		},
		{
			si: version.ServerInfo{
				ServerType:    version.ServerTypeMySQL,
				ServerVersion: semver.New("8.0.32"),
			},
			sessionParams:  map[string]any{},
			expectedParams: map[string]any{},
		},
	}

	for _, testCase := range testCases {
		setDefaultSessionParams(testCase.si, testCase.sessionParams)
		require.Equal(t, testCase.expectedParams, testCase.sessionParams)
	}
}

func TestSetSessionParams(t *testing.T) {
	// case 1: fail to set tidb_snapshot, should return error with hint
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	mock.ExpectQuery("SELECT @@tidb_config").
		WillReturnError(errors.New("mock error"))
	mock.ExpectQuery("SELECT COUNT\\(1\\) as c FROM MYSQL.TiDB WHERE VARIABLE_NAME='tikv_gc_safe_point'").
		WillReturnError(errors.New("mock error"))
	tikvErr := &mysql.MySQLError{
		Number:  1105,
		Message: "can not get 'tikv_gc_safe_point'",
	}
	mock.ExpectExec("SET SESSION tidb_snapshot").
		WillReturnError(tikvErr)

	testfailpoint.Enable(t, "github.com/pingcap/tidb/dumpling/export/SkipResetDB", "return(true)")

	tctx, cancel := tcontext.Background().WithLogger(appLogger).WithCancel()
	defer cancel()

	conf := DefaultConfig()
	conf.ServerInfo = version.ServerInfo{
		ServerType: version.ServerTypeTiDB,
		HasTiKV:    false,
	}
	conf.Snapshot = "439153276059648000"
	conf.Consistency = ConsistencyTypeSnapshot
	d := &Dumper{
		tctx:      tctx,
		conf:      conf,
		cancelCtx: cancel,
		dbHandle:  db,
	}
	err = setSessionParam(d)
	require.ErrorContains(t, err, "consistency=none")

	// case 2: fail to set other
	conf.ServerInfo = version.ServerInfo{
		ServerType: version.ServerTypeMySQL,
		HasTiKV:    false,
	}
	conf.Snapshot = ""
	conf.Consistency = ConsistencyTypeFlush
	conf.SessionParams = map[string]any{
		"mock": "UTC",
	}
	d.dbHandle = db
	mock.ExpectExec("SET SESSION mock").
		WillReturnError(errors.New("Unknown system variable mock"))
	mock.ExpectClose()
	mock.ExpectClose()

	err = setSessionParam(d)
	require.NoError(t, err)
}
