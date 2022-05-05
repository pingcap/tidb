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
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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

	lostConnectionToPDTimeout       = flag.Int("conn_lost", 5, "lost connection to PD timeout, should be the same as TiDB ldflag <ldflagLostConnectionToPDTimeout>")
	timeToCheckPDConnectionRestored = flag.Int("conn_restored", 1, "time to check PD connection restored, should be the same as TiDB ldflag <ldflagServerIDTimeToCheckPDConnectionRestored>")
)

const (
	waitToStartup   = 500 * time.Millisecond
	msgErrConnectPD = "connect PD err: %v. Establish a cluster with PD & TiKV, and provide PD client path by `--pd=<ip:port>[,<ip:port>]"
)

// GlobakKillSuite is used for automated test of "Global Kill" feature.
// See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md.
type GlobalKillSuite struct {
	pdCli *clientv3.Client
	pdErr error

	clusterId string
	pdProc    *exec.Cmd
	tikvProc  *exec.Cmd
}

func createGloabalKillSuite(t *testing.T) (s *GlobalKillSuite, clean func()) {
	s = new(GlobalKillSuite)
	err := logutil.InitLogger(&logutil.LogConfig{Config: log.Config{Level: *logLevel}})
	require.NoError(t, err)

	s.clusterId = time.Now().Format(time.RFC3339Nano)
	err = s.startCluster()
	require.NoError(t, err)
	s.pdCli, s.pdErr = s.connectPD()
	clean = func() {
		if s.pdCli != nil {
			require.NoError(t, err)
		}
		require.NoError(t, s.cleanCluster())
	}

	return
}

func (s *GlobalKillSuite) connectPD() (cli *clientv3.Client, err error) {
	etcdLogCfg := zap.NewProductionConfig()
	etcdLogCfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	wait := 250 * time.Millisecond
	for i := 0; i < 5; i++ {
		log.Info(fmt.Sprintf("trying to connect pd, attempt %d", i))
		cli, err = clientv3.New(clientv3.Config{
			LogConfig:        &etcdLogCfg,
			Endpoints:        strings.Split(*pdClientPath, ","),
			AutoSyncInterval: 30 * time.Second,
			DialTimeout:      5 * time.Second,
			DialOptions: []grpc.DialOption{
				grpc.WithBackoffMaxDelay(time.Second * 3),
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
		fmt.Sprintf("--data-dir=tikv-%s", dataDir),
		"--addr=0.0.0.0:20160",
		"--log-file=tikv.log",
		"--advertise-addr=127.0.0.1:20160",
	)
	log.Info("starting tikv")
	err = s.tikvProc.Start()
	if err != nil {
		return errors.Trace(err)
	}
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (s *GlobalKillSuite) startPD(dataDir string) (err error) {
	s.pdProc = exec.Command(*pdBinaryPath,
		"--name=pd",
		"--log-file=pd.log",
		fmt.Sprintf("--client-urls=http://%s", *pdClientPath),
		fmt.Sprintf("--data-dir=pd-%s", dataDir))
	log.Info("starting pd")
	err = s.pdProc.Start()
	if err != nil {
		return errors.Trace(err)
	}
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (s *GlobalKillSuite) startCluster() (err error) {
	err = s.startPD(s.clusterId)
	if err != nil {
		return
	}

	err = s.startTiKV(s.clusterId)
	if err != nil {
		return
	}
	time.Sleep(10 * time.Second)
	return
}

func (s *GlobalKillSuite) stopPD() (err error) {
	if err = s.pdProc.Process.Kill(); err != nil {
		return
	}
	if err = s.pdProc.Wait(); err != nil && err.Error() != "signal: killed" {
		return err
	}
	return nil
}

func (s *GlobalKillSuite) stopTiKV() (err error) {
	if err = s.tikvProc.Process.Kill(); err != nil {
		return
	}
	if err = s.tikvProc.Wait(); err != nil && err.Error() != "signal: killed" {
		return err
	}
	return nil
}

func (s *GlobalKillSuite) cleanCluster() (err error) {
	if err = s.stopPD(); err != nil {
		return err
	}
	if err = s.stopTiKV(); err != nil {
		return err
	}
	log.Info("cluster cleaned")
	return nil
}

func (s *GlobalKillSuite) startTiDBWithoutPD(port int, statusPort int) (cmd *exec.Cmd, err error) {
	cmd = exec.Command(*tidbBinaryPath,
		"--store=mocktikv",
		fmt.Sprintf("-L=%s", *serverLogLevel),
		fmt.Sprintf("--path=%s/mocktikv", *tmpPath),
		fmt.Sprintf("-P=%d", port),
		fmt.Sprintf("--status=%d", statusPort),
		fmt.Sprintf("--log-file=%s/tidb%d.log", *tmpPath, port),
		fmt.Sprintf("--config=%s", "./config.toml"))
	log.Info("starting tidb", zap.Any("cmd", cmd))
	err = cmd.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	time.Sleep(500 * time.Millisecond)
	return cmd, nil
}

func (s *GlobalKillSuite) startTiDBWithPD(port int, statusPort int, pdPath string) (cmd *exec.Cmd, err error) {
	cmd = exec.Command(*tidbBinaryPath,
		"--store=tikv",
		fmt.Sprintf("-L=%s", *serverLogLevel),
		fmt.Sprintf("--path=%s", pdPath),
		fmt.Sprintf("-P=%d", port),
		fmt.Sprintf("--status=%d", statusPort),
		fmt.Sprintf("--log-file=%s/tidb%d.log", *tmpPath, port),
		fmt.Sprintf("--config=%s", "./config.toml"))
	log.Info("starting tidb", zap.Any("cmd", cmd))
	err = cmd.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	time.Sleep(500 * time.Millisecond)
	return cmd, nil
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
	startTime := time.Now()
	maxRetry := 10
	for i := 0; i < maxRetry; i++ {
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
		if i == maxRetry-1 {
			return
		}

		err = db.Close()
		if err != nil {
			return nil, errors.Trace(err)
		}
		time.Sleep(sleepTime)
		sleepTime += sleepTime
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

type sleepResult struct {
	elapsed time.Duration
	err     error
}

func (s *GlobalKillSuite) killByCtrlC(t *testing.T, port int, sleepTime int) time.Duration {
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
	return r.elapsed
}

func sleepRoutine(ctx context.Context, sleepTime int, conn *sql.Conn, connID uint64, ch chan<- sleepResult) {
	var err error
	startTS := time.Now()
	sql := fmt.Sprintf("SELECT SLEEP(%d);", sleepTime)
	if connID > 0 {
		log.Info("exec sql", zap.String("sql", sql), zap.String("connID", "0x"+strconv.FormatUint(connID, 16)))
	} else {
		log.Info("exec sql", zap.String("sql", sql))
	}
	rows, err := conn.QueryContext(ctx, sql)
	if err != nil {
		ch <- sleepResult{err: err}
		return
	}
	rows.Next()
	if rows.Err() != nil {
		ch <- sleepResult{err: rows.Err()}
		return
	}
	err = rows.Close()
	if err != nil {
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
	s, clean := createGloabalKillSuite(t)
	defer clean()
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
	elapsed := s.killByCtrlC(t, port, 2)
	require.GreaterOrEqual(t, elapsed, 2*time.Second)

	// Test KILL statement
	elapsed = s.killByKillStatement(t, db, db, 2)
	require.Less(t, elapsed, 2*time.Second)
}

// [Test Scenario 2] One TiDB with PD, killed by Ctrl+C, and killed by KILL.
func TestOneTiDB(t *testing.T) {
	s, clean := createGloabalKillSuite(t)
	defer clean()
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
	elapsed := s.killByCtrlC(t, port, sleepTime)
	require.GreaterOrEqual(t, elapsed, sleepTime*time.Second)

	// Test KILL statement
	elapsed = s.killByKillStatement(t, db, db, sleepTime)
	require.Less(t, elapsed, sleepTime*time.Second)
}

// [Test Scenario 3] Multiple TiDB nodes, killed {local,remote} by {Ctrl-C,KILL}.
func TestMultipleTiDB(t *testing.T) {
	s, clean := createGloabalKillSuite(t)
	defer clean()
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
	elapsed = s.killByCtrlC(t, port1, sleepTime)
	require.GreaterOrEqual(t, elapsed, sleepTime*time.Second)

	// kill local by KILL
	elapsed = s.killByKillStatement(t, db1a, db1b, sleepTime)
	require.Less(t, elapsed, sleepTime*time.Second)

	// kill remotely
	elapsed = s.killByKillStatement(t, db1a, db2, sleepTime)
	require.Less(t, elapsed, sleepTime*time.Second)
}

func TestLostConnection(t *testing.T) {
	s, clean := createGloabalKillSuite(t)
	defer clean()
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
	log.Info(fmt.Sprintf("pd shutdown: %s", err))
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
