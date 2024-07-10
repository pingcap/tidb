// Copyright 2020 PingCAP, Inc.
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

package globalkilltest

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

var (
	logLevel       = flag.String("L", "info", "test log level")
	serverLogLevel = flag.String("server_log_level", "info", "server log level")
	tmpPath        = flag.String("tmp", "/tmp/tidb_globalkilltest", "temporary files path")

	tidbBinaryPath = flag.String("s", "bin/globalkilltest_tidb-server", "tidb server binary path")
	pdBinaryPath   = flag.String("p", "bin/pd-server", "pd server binary path")
	tikvBinaryPath = flag.String("k", "bin/tikv-server", "tikv server binary path")
	tidbStartPort  = flag.Int("tidb_start_port", 5000, "first tidb server listening port")
	tidbStatusPort = flag.Int("tidb_status_port", 8000, "first tidb server status port")

	pdClientPath = flag.String("pd", "127.0.0.1:2379", "pd client path")

	// nolint: unused, deadcode
	lostConnectionToPDTimeout = flag.Int("conn_lost", 5, "lost connection to PD timeout, should be the same as TiDB ldflag <ldflagLostConnectionToPDTimeout>")

	// nolint: unused, deadcode
	timeToCheckPDConnectionRestored = flag.Int("conn_restored", 1, "time to check PD connection restored, should be the same as TiDB ldflag <ldflagServerIDTimeToCheckPDConnectionRestored>")
)

const (
	waitToStartup    = 500 * time.Millisecond
	msgErrConnectPD  = "connect PD err: %v. Establish a cluster with PD & TiKV, and provide PD client path by `--pd=<ip:port>[,<ip:port>]"
	timeoutConnectDB = 20 * time.Second
)

// GlobalKillSuite is used for automated test of "Global Kill" feature.
// See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md.
type GlobalKillSuite struct {
	enable32Bits bool

	pdCli *clientv3.Client
	pdErr error

	clusterID string
	pdProc    *exec.Cmd
	tikvProc  *exec.Cmd
}

func createGlobalKillSuite(t *testing.T, enable32bits bool) *GlobalKillSuite {
	s := new(GlobalKillSuite)
	s.enable32Bits = enable32bits

	err := logutil.InitLogger(&logutil.LogConfig{Config: log.Config{Level: *logLevel}})
	require.NoError(t, err)

	s.clusterID = time.Now().Format(time.RFC3339Nano)
	err = s.startCluster()
	require.NoError(t, err)
	s.pdCli, s.pdErr = s.connectPD()
	t.Cleanup(func() {
		if s.pdCli != nil {
			require.NoError(t, err)
		}
		require.NoError(t, s.cleanCluster())
	})

	return s
}

// Conn is wrapper of DB connection.
type Conn struct {
	db     *sql.DB
	conn   *sql.Conn
	connID uint64
}

func (c *Conn) Close() {
	c.conn.Close()
	c.db.Close()
}

func (c *Conn) mustBe32(t *testing.T) {
	require.Lessf(t, c.connID, uint64(1<<32), "connID %x", c.connID)
}

func (c *Conn) mustBe64(t *testing.T) {
	require.Greaterf(t, c.connID, uint64(1<<32), "connID %x", c.connID)
}

func (s *GlobalKillSuite) connectPD() (cli *clientv3.Client, err error) {
	etcdLogCfg := zap.NewProductionConfig()
	etcdLogCfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	wait := 250 * time.Millisecond
	backoffConfig := backoff.DefaultConfig
	backoffConfig.MaxDelay = 3 * time.Second
	for i := 0; i < 5; i++ {
		log.Info(fmt.Sprintf("trying to connect pd, attempt %d", i))
		cli, err = clientv3.New(clientv3.Config{
			LogConfig:        &etcdLogCfg,
			Endpoints:        strings.Split(*pdClientPath, ","),
			AutoSyncInterval: 30 * time.Second,
			DialTimeout:      5 * time.Second,
			DialOptions: []grpc.DialOption{
				grpc.WithConnectParams(grpc.ConnectParams{
					Backoff: backoffConfig,
				}),
			},
		})
		if err == nil {
			break
		}
		time.Sleep(wait)
		wait = wait * 2
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // use `Sync` to test connection, and get current members.
	err = cli.Sync(ctx)
	cancel()
	if err != nil {
		cli.Close()
		return nil, errors.Trace(err)
	}
	log.Info("pd connected")
	return cli, nil
}

func (s *GlobalKillSuite) startTiKV(dataDir string) (err error) {
	s.tikvProc = exec.Command(*tikvBinaryPath,
		fmt.Sprintf("--pd=%s", *pdClientPath),
		fmt.Sprintf("--data-dir=%s/tikv-%s", *tmpPath, dataDir),
		"--addr=127.0.0.1:20160",
		fmt.Sprintf("--log-file=%s/tikv.log", *tmpPath),
		"--advertise-addr=127.0.0.1:20160",
		"--config=tikv.toml",
	)
	log.Info("starting tikv", zap.Any("cmd", s.tikvProc))
	err = s.tikvProc.Start()
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(checkTiKVStatus())
}

func (s *GlobalKillSuite) startPD(dataDir string) (err error) {
	s.pdProc = exec.Command(*pdBinaryPath,
		"--name=pd",
		fmt.Sprintf("--log-file=%s/pd.log", *tmpPath),
		fmt.Sprintf("--client-urls=http://%s", *pdClientPath),
		fmt.Sprintf("--data-dir=%s/pd-%s", *tmpPath, dataDir))
	log.Info("starting pd", zap.Any("cmd", s.pdProc))
	err = s.pdProc.Start()
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(checkPDHealth(*pdClientPath))
}

func (s *GlobalKillSuite) startCluster() (err error) {
	err = s.startPD(s.clusterID)
	if err != nil {
		return errors.Trace(err)
	}

	err = s.startTiKV(s.clusterID)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (s *GlobalKillSuite) stopPD() (err error) {
	if s.pdProc == nil {
		log.Info("PD already killed")
		return nil
	}
	if err = s.pdProc.Process.Kill(); err != nil {
		return errors.Trace(err)
	}
	if err = s.pdProc.Wait(); err != nil && err.Error() != "signal: killed" {
		return errors.Trace(err)
	}
	s.pdProc = nil
	return nil
}

func (s *GlobalKillSuite) stopTiKV() (err error) {
	if s.tikvProc == nil {
		log.Info("TiKV already killed")
		return nil
	}
	if err = s.tikvProc.Process.Kill(); err != nil {
		return errors.Trace(err)
	}
	if err = s.tikvProc.Wait(); err != nil && err.Error() != "signal: killed" {
		return errors.Trace(err)
	}
	s.tikvProc = nil
	return nil
}

func (s *GlobalKillSuite) cleanCluster() (err error) {
	if err = s.stopPD(); err != nil {
		return errors.Trace(err)
	}
	if err = s.stopTiKV(); err != nil {
		return errors.Trace(err)
	}
	log.Info("cluster cleaned")
	return nil
}

func (s *GlobalKillSuite) getTiDBConfigPath() string {
	if s.enable32Bits {
		return "./config.toml"
	}
	return "./config-64.toml"
}

func (s *GlobalKillSuite) startTiDBWithoutPD(port int, statusPort int) (cmd *exec.Cmd, err error) {
	cmd = exec.Command(*tidbBinaryPath,
		"--store=mocktikv",
		fmt.Sprintf("-L=%s", *serverLogLevel),
		fmt.Sprintf("--path=%s/mocktikv", *tmpPath),
		fmt.Sprintf("-P=%d", port),
		fmt.Sprintf("--status=%d", statusPort),
		fmt.Sprintf("--log-file=%s/tidb%d.log", *tmpPath, port),
		fmt.Sprintf("--log-slow-query=%s/tidb-slow%d.log", *tmpPath, port),
		fmt.Sprintf("--config=%s", s.getTiDBConfigPath()))
	log.Info("starting tidb", zap.Any("cmd", cmd))
	err = cmd.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cmd, errors.Trace(checkTiDBStatus(statusPort))
}

func (s *GlobalKillSuite) startTiDBWithPD(port int, statusPort int, pdPath string) (cmd *exec.Cmd, err error) {
	cmd = exec.Command(*tidbBinaryPath,
		"--store=tikv",
		fmt.Sprintf("-L=%s", *serverLogLevel),
		fmt.Sprintf("--path=%s", pdPath),
		fmt.Sprintf("-P=%d", port),
		fmt.Sprintf("--status=%d", statusPort),
		fmt.Sprintf("--log-file=%s/tidb%d.log", *tmpPath, port),
		fmt.Sprintf("--log-slow-query=%s/tidb-slow%d.log", *tmpPath, port),
		fmt.Sprintf("--config=%s", s.getTiDBConfigPath()))
	log.Info("starting tidb", zap.Any("cmd", cmd))
	err = cmd.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cmd, errors.Trace(checkTiDBStatus(statusPort))
}

func (s *GlobalKillSuite) mustStartTiDBWithPD(t *testing.T, port int, statusPort int, pdPath string) *exec.Cmd {
	cmd, err := s.startTiDBWithPD(port, statusPort, pdPath)
	require.Nil(t, err)
	return cmd
}

func (s *GlobalKillSuite) stopService(name string, cmd *exec.Cmd, graceful bool) (err error) {
	log.Info("stopping: " + cmd.String())
	defer func() {
		log.Info("stopped: " + cmd.String())
	}()
	if graceful {
		if err = cmd.Process.Signal(os.Interrupt); err != nil {
			return errors.Trace(err)
		}
		ch := make(chan error)
		go func() {
			ch <- cmd.Wait()
		}()
		select {
		case err = <-ch:
			if err != nil {
				return err
			}
			log.Info(fmt.Sprintf("service \"%s\" stopped gracefully", name))
			return nil
		case <-time.After(60 * time.Second):
			err = fmt.Errorf("service \"%s\" can't gracefully stop in time", name)
			log.Info(err.Error())
			return err
		}
	}

	if err = cmd.Process.Kill(); err != nil {
		return errors.Trace(err)
	}
	time.Sleep(1 * time.Second)
	log.Info("service killed", zap.String("name", name))
	return nil
}

func (s *GlobalKillSuite) connectTiDB(port int) (db *sql.DB, err error) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	dsn := fmt.Sprintf("root@(%s)/test", addr)
	sleepTime := 250 * time.Millisecond
	sleepTimeLimit := 1 * time.Second
	startTime := time.Now()
	for i := 0; time.Since(startTime) < timeoutConnectDB; i++ {
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			log.Warn("open addr failed",
				zap.String("addr", addr),
				zap.Int("retry count", i),
				zap.Error(err),
			)
			continue
		}
		err = db.Ping()
		if err == nil {
			break
		}
		log.Warn("ping addr failed",
			zap.String("addr", addr),
			zap.Int("retry count", i),
			zap.Error(err),
		)

		db.Close()
		time.Sleep(sleepTime)
		if sleepTime < sleepTimeLimit {
			sleepTime += sleepTime
		}
	}
	if err != nil {
		log.Error("connect to server addr failed",
			zap.String("addr", addr),
			zap.Duration("take time", time.Since(startTime)),
			zap.Error(err),
		)
		return nil, errors.Trace(err)
	}
	db.SetMaxOpenConns(10)

	log.Info("connect to server ok", zap.String("addr", addr))
	return db, nil
}

func (s *GlobalKillSuite) mustConnectTiDB(t *testing.T, port int) Conn {
	ctx := context.TODO()

	db, err := s.connectTiDB(port)
	require.Nil(t, err)

	conn, err := db.Conn(ctx)
	require.NoError(t, err)

	var connID uint64
	err = conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connID)
	require.NoError(t, err)

	log.Info("connect to server ok", zap.Int("port", port), zap.Uint64("connID", connID))
	return Conn{db, conn, connID}
}

type sleepResult struct {
	elapsed time.Duration
	err     error
}

func (s *GlobalKillSuite) testKillByCtrlC(t *testing.T, port int, sleepTime int) time.Duration {
	cli := exec.Command("mysql",
		"-h127.0.0.1",
		fmt.Sprintf("-P%d", port),
		"-uroot",
		"-e", fmt.Sprintf("SELECT SLEEP(%d);", sleepTime))
	log.Info("run mysql cli", zap.Any("cli", cli))

	ch := make(chan sleepResult)
	go func() {
		startTS := time.Now()
		err := cli.Run()
		if err != nil {
			ch <- sleepResult{err: errors.Trace(err)}
			return
		}

		elapsed := time.Since(startTS)
		log.Info("mysql cli takes", zap.Duration("elapsed", elapsed))
		ch <- sleepResult{elapsed: elapsed}
	}()

	time.Sleep(waitToStartup)               // wait before mysql cli running.
	err := cli.Process.Signal(os.Interrupt) // send "CTRL-C".
	require.NoError(t, err)

	r := <-ch
	require.NoError(t, err)
	if s.enable32Bits {
		require.Less(t, r.elapsed, time.Duration(sleepTime)*time.Second)
	} else {
		require.GreaterOrEqual(t, r.elapsed, time.Duration(sleepTime)*time.Second)
	}
	return r.elapsed
}

func sleepRoutine(ctx context.Context, sleepTime int, conn *sql.Conn, connID uint64, ch chan<- sleepResult) {
	var err error
	startTS := time.Now()
	sql := fmt.Sprintf("SELECT SLEEP(%d);", sleepTime)
	if connID > 0 {
		log.Info("exec sql", zap.String("sql", sql), zap.String("conn", "0x"+strconv.FormatUint(connID, 16)))
	} else {
		log.Info("exec sql", zap.String("sql", sql))
	}
	rows, err := conn.QueryContext(ctx, sql)
	if err != nil {
		ch <- sleepResult{err: err}
		return
	}
	rows.Next()
	if err := rows.Err(); err != nil {
		ch <- sleepResult{err: err}
		return
	}
	if err = rows.Close(); err != nil {
		ch <- sleepResult{err: err}
	}

	elapsed := time.Since(startTS)
	log.Info("sleepRoutine takes", zap.Duration("elapsed", elapsed))
	ch <- sleepResult{elapsed: elapsed}
}

// NOTICE: db1 & db2 can be the same object, for getting conn1 & conn2 from the same TiDB instance.
func (s *GlobalKillSuite) killByKillStatement(t *testing.T, db1 *sql.DB, db2 *sql.DB, sleepTime int) time.Duration {
	ctx := context.TODO()

	conn1, err := db1.Conn(ctx)
	require.NoError(t, err)
	defer conn1.Close()

	var connID1 uint64
	err = conn1.QueryRowContext(ctx, "SELECT CONNECTION_ID();").Scan(&connID1)
	require.NoError(t, err)
	log.Info("connID1", zap.String("connID1", "0x"+strconv.FormatUint(connID1, 16)))

	ch := make(chan sleepResult)
	go sleepRoutine(ctx, sleepTime, conn1, connID1, ch)

	time.Sleep(waitToStartup) // wait go-routine to start.
	conn2, err := db2.Conn(ctx)
	require.NoError(t, err)
	defer conn2.Close()

	var connID2 uint64
	err = conn2.QueryRowContext(ctx, "SELECT CONNECTION_ID();").Scan(&connID2)
	require.NoError(t, err)
	log.Info("connID2", zap.String("connID2", "0x"+strconv.FormatUint(connID2, 16)))

	log.Info("exec: KILL QUERY",
		zap.String("connID1", "0x"+strconv.FormatUint(connID1, 16)),
		zap.String("connID2", "0x"+strconv.FormatUint(connID2, 16)),
	)
	_, err = conn2.ExecContext(ctx, fmt.Sprintf("KILL QUERY %v", connID1))
	require.NoError(t, err)

	r := <-ch
	require.NoError(t, err)
	return r.elapsed
}

// [Test Scenario 1] A TiDB without PD, killed by Ctrl+C, and killed by KILL.
func TestWithoutPD(t *testing.T) {
	doTestWithoutPD(t, false)
}

func TestWithoutPD32(t *testing.T) {
	doTestWithoutPD(t, true)
}

func doTestWithoutPD(t *testing.T, enable32Bits bool) {
	s := createGlobalKillSuite(t, enable32Bits)
	var err error
	port := *tidbStartPort
	tidb, err := s.startTiDBWithoutPD(port, *tidbStatusPort)
	require.NoError(t, err)
	defer s.stopService("tidb", tidb, true)

	db, err := s.connectTiDB(port)
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	// Test mysql client CTRL-C
	// mysql client "CTRL-C" truncate connection id to 32bits, and is ignored by TiDB.
	s.testKillByCtrlC(t, port, 2)

	// Test KILL statement
	elapsed := s.killByKillStatement(t, db, db, 2)
	require.Less(t, elapsed, 2*time.Second)
}

// [Test Scenario 2] One TiDB with PD, killed by Ctrl+C, and killed by KILL.
func TestOneTiDB(t *testing.T) {
	doTestOneTiDB(t, false)
}

func TestOneTiDB32(t *testing.T) {
	doTestOneTiDB(t, true)
}

func doTestOneTiDB(t *testing.T, enable32Bits bool) {
	s := createGlobalKillSuite(t, enable32Bits)
	port := *tidbStartPort + 1
	tidb, err := s.startTiDBWithPD(port, *tidbStatusPort+1, *pdClientPath)
	require.NoError(t, err)
	defer s.stopService("tidb", tidb, true)

	db, err := s.connectTiDB(port)
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	const sleepTime = 2

	// Test mysql client CTRL-C
	// mysql client "CTRL-C" truncate connection id to 32bits, and is ignored by TiDB.
	// see TiDB's logging for the truncation warning.
	s.testKillByCtrlC(t, port, sleepTime)

	// Test KILL statement
	elapsed := s.killByKillStatement(t, db, db, sleepTime)
	require.Less(t, elapsed, sleepTime*time.Second)
}

// [Test Scenario 3] Multiple TiDB nodes, killed {local,remote} by {Ctrl-C,KILL}.
func TestMultipleTiDB(t *testing.T) {
	doTestMultipleTiDB(t, false)
}

func TestMultipleTiDB32(t *testing.T) {
	doTestMultipleTiDB(t, true)
}

func doTestMultipleTiDB(t *testing.T, enable32Bits bool) {
	s := createGlobalKillSuite(t, enable32Bits)
	require.NoErrorf(t, s.pdErr, msgErrConnectPD, s.pdErr)

	// tidb1 & conn1a,conn1b
	port1 := *tidbStartPort + 1
	tidb1, err := s.startTiDBWithPD(port1, *tidbStatusPort+1, *pdClientPath)
	require.NoError(t, err)
	defer s.stopService("tidb1", tidb1, true)

	db1a, err := s.connectTiDB(port1)
	require.NoError(t, err)
	defer db1a.Close()

	db1b, err := s.connectTiDB(port1)
	require.NoError(t, err)
	defer db1b.Close()

	// tidb2 & conn2
	port2 := *tidbStartPort + 2
	tidb2, err := s.startTiDBWithPD(port2, *tidbStatusPort+2, *pdClientPath)
	require.NoError(t, err)
	defer s.stopService("tidb2", tidb2, true)

	db2, err := s.connectTiDB(port2)
	require.NoError(t, err)
	defer db2.Close()

	const sleepTime = 2
	var elapsed time.Duration

	// kill local by CTRL-C
	// mysql client "CTRL-C" truncate connection id to 32bits, and is ignored by TiDB.
	// see TiDB's logging for the truncation warning.
	s.testKillByCtrlC(t, port1, sleepTime)

	// kill local by KILL
	elapsed = s.killByKillStatement(t, db1a, db1b, sleepTime)
	require.Less(t, elapsed, sleepTime*time.Second)

	// kill remotely
	elapsed = s.killByKillStatement(t, db1a, db2, sleepTime)
	require.Less(t, elapsed, sleepTime*time.Second)
}

func TestLostConnection(t *testing.T) {
	doTestLostConnection(t, false)
}

func TestLostConnection32(t *testing.T) {
	doTestLostConnection(t, true)
}

func doTestLostConnection(t *testing.T, enable32Bits bool) {
	s := createGlobalKillSuite(t, enable32Bits)
	require.NoErrorf(t, s.pdErr, msgErrConnectPD, s.pdErr)

	// tidb1
	port1 := *tidbStartPort + 1
	tidb1, err := s.startTiDBWithPD(port1, *tidbStatusPort+1, *pdClientPath)
	require.NoError(t, err)
	defer s.stopService("tidb1", tidb1, true)

	db1, err := s.connectTiDB(port1)
	require.NoError(t, err)
	defer db1.Close()

	// tidb2
	port2 := *tidbStartPort + 2
	tidb2, err := s.startTiDBWithPD(port2, *tidbStatusPort+2, *pdClientPath)
	require.NoError(t, err)
	defer s.stopService("tidb2", tidb2, true)

	db2, err := s.connectTiDB(port2)
	require.NoError(t, err)
	defer db2.Close()

	// verify it's working.
	ctx := context.TODO()
	conn1, err := db1.Conn(ctx)
	require.NoError(t, err)
	defer conn1.Close()
	err = conn1.PingContext(ctx)
	require.NoError(t, err)

	// a running sql
	sqlTime := *lostConnectionToPDTimeout + 10
	ch := make(chan sleepResult)
	go sleepRoutine(ctx, sqlTime, conn1, 0, ch)
	time.Sleep(waitToStartup) // wait go-routine to start.

	// disconnect to PD by shutting down PD process.
	log.Info("shutdown PD to simulate lost connection to PD.")
	err = s.stopPD()
	log.Info(fmt.Sprintf("pd shutdown: %v", err))
	require.NoError(t, err)

	// wait for "lostConnectionToPDTimeout" elapsed.
	// delay additional 3 seconds for TiDB would have a small interval to detect lost connection more than "lostConnectionToPDTimeout".
	sleepTime := time.Duration(*lostConnectionToPDTimeout+3) * time.Second
	log.Info("sleep to wait for TiDB had detected lost connection", zap.Duration("sleepTime", sleepTime))
	time.Sleep(sleepTime)

	// check running sql
	// [Test Scenario 4] Existing connections are killed after PD lost connection for long time.
	r := <-ch
	log.Info("sleepRoutine err", zap.Error(r.err))
	require.NotNil(t, r.err)
	require.Equal(t, r.err.Error(), "invalid connection")

	// check new connection.
	// [Test Scenario 5] New connections are not accepted after PD lost connection for long time.
	log.Info("check connection after lost connection to PD.")
	_, err = s.connectTiDB(port1)
	log.Info("connectTiDB err", zap.Error(err))
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "driver: bad connection")

	err = s.stopTiKV()
	require.NoError(t, err)
	// restart cluster to restore connection.
	err = s.startCluster()
	require.NoError(t, err)

	// wait for "timeToCheckPDConnectionRestored" elapsed.
	// delay additional 3 seconds for TiDB would have a small interval to detect lost connection restored more than "timeToCheckPDConnectionRestored".
	sleepTime = time.Duration(*timeToCheckPDConnectionRestored+3) * time.Second
	log.Info("sleep to wait for TiDB had detected lost connection restored", zap.Duration("sleepTime", sleepTime))
	time.Sleep(sleepTime)

	// check restored
	{
		// [Test Scenario 6] New connections are accepted after PD lost connection for long time and then recovered.
		db1, err := s.connectTiDB(port1)
		require.NoError(t, err)
		defer func() {
			err := db1.Close()
			require.NoError(t, err)
		}()

		db2, err := s.connectTiDB(port2)
		require.NoError(t, err)
		defer func() {
			err := db2.Close()
			require.NoError(t, err)
		}()

		// [Test Scenario 7] Connections can be killed after PD lost connection for long time and then recovered.
		elapsed := s.killByKillStatement(t, db1, db1, 2)
		require.Less(t, elapsed, 2*time.Second)

		elapsed = s.killByKillStatement(t, db1, db2, 2)
		require.Less(t, elapsed, 2*time.Second)
	}
}

func TestServerIDUpgradeAndDowngrade(t *testing.T) {
	s := createGlobalKillSuite(t, true)
	require.NoErrorf(t, s.pdErr, msgErrConnectPD, s.pdErr)

	connect := func(idx int) Conn {
		return s.mustConnectTiDB(t, *tidbStartPort+idx)
	}

	// MaxTiDB32 is determined by `github.com/pingcap/tidb/pkg/util/globalconn.ldflagServerIDBits32`
	// See the ldflags in `Makefile`.
	// Also see `Domain.proposeServerID`.
	const MaxTiDB32 = 2 // (3^2 -1) x 0.9
	const MaxTiDB64 = 2

	// Startup MAX_TIDB_32 number of TiDBs.
	tidbs := make([]*exec.Cmd, MaxTiDB32*2)
	defer func() {
		for i := range tidbs {
			if tidbs[i] != nil {
				s.stopService(fmt.Sprintf("tidb%v", i), tidbs[i], true)
			}
		}
	}()
	{
		for i := 0; i < MaxTiDB32; i++ {
			tidbs[i] = s.mustStartTiDBWithPD(t, *tidbStartPort+i, *tidbStatusPort+i, *pdClientPath)
		}
		for i := 0; i < MaxTiDB32; i++ {
			conn := connect(i)
			conn.mustBe32(t)
			conn.Close()
		}
	}

	// Upgrade to 64 bits due to ServerID used up.
	{
		for i := MaxTiDB32; i < MaxTiDB32+MaxTiDB64; i++ {
			tidbs[i] = s.mustStartTiDBWithPD(t, *tidbStartPort+i, *tidbStatusPort+i, *pdClientPath)
		}
		for i := MaxTiDB32; i < MaxTiDB32+MaxTiDB64; i++ {
			conn := connect(i)
			conn.mustBe64(t)
			conn.Close()
		}
	}

	// Close TiDBs to downgrade to 32 bits.
	{
		for i := MaxTiDB32 / 2; i < MaxTiDB32+MaxTiDB64; i++ {
			s.stopService(fmt.Sprintf("tidb%v", i), tidbs[i], true)
			tidbs[i] = nil
		}

		dbIdx := MaxTiDB32 + MaxTiDB64
		tidb := s.mustStartTiDBWithPD(t, *tidbStartPort+dbIdx, *tidbStatusPort+dbIdx, *pdClientPath)
		defer s.stopService(fmt.Sprintf("tidb%v", dbIdx), tidb, true)
		conn := connect(dbIdx)
		conn.mustBe32(t)
		conn.Close()
	}
}

func TestConnIDUpgradeAndDowngrade(t *testing.T) {
	s := createGlobalKillSuite(t, true)
	require.NoErrorf(t, s.pdErr, msgErrConnectPD, s.pdErr)

	connect := func() Conn {
		return s.mustConnectTiDB(t, *tidbStartPort)
	}

	tidb := s.mustStartTiDBWithPD(t, *tidbStartPort, *tidbStatusPort, *pdClientPath)
	defer s.stopService("tidb0", tidb, true)

	// MaxConn32 is determined by `github.com/pingcap/tidb/pkg/util/globalconn.ldflagLocalConnIDBits32`
	// See the ldflags in `Makefile`.
	// Also see `LockFreeCircularPool.Cap`.
	const MaxConn32 = 1<<4 - 1

	conns32 := make(map[uint64]Conn)
	defer func() {
		for _, conn := range conns32 {
			conn.Close()
		}
	}()
	// 32 bits connection ID
	for i := 0; i < MaxConn32; i++ {
		conn := connect()
		require.Lessf(t, conn.connID, uint64(1<<32), "connID %x", conn.connID)
		conns32[conn.connID] = conn
	}
	// 32bits pool is full, should upgrade to 64 bits
	for i := MaxConn32; i < MaxConn32*2; i++ {
		conn := connect()
		conn.mustBe64(t)
		conn.Close()
	}

	// Release more than half of 32 bits connections, should downgrade to 32 bits
	count := MaxConn32/2 + 1
	for connID, conn := range conns32 {
		conn.Close()
		delete(conns32, connID)
		count--
		if count == 0 {
			break
		}
	}
	conn := connect()
	conn.mustBe32(t)
	conn.Close()
}
