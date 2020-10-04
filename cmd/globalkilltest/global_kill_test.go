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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/tidb/util/logutil"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func TestGlobalKill(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var (
	logLevel       = flag.String("L", "info", "log level")
	serverLogLevel = flag.String("server_log_level", "info", "server log level")

	tidbBinaryPath = flag.String("tidb_binary_path", "../../bin/tidb-server-global-kill-test", "tidb binary path")
	tidbStartPort  = flag.Int("tidb_start_port", 5000, "First tidb server listening port")
	tidbStatusPort = flag.Int("tidb_status_port", 8000, "First tidb server status port")

	pdBinaryPath = flag.String("pd_binary_path", "bin/pd-server", "pd binary path")
	pdClientPath = flag.String("pd_client_path", "127.0.0.1:2379", "pd client path")
	pdPeerPath   = flag.String("pd_peer_path", "127.0.0.1:3380", "pd peer path")
	pdProxyPort  = flag.String("pd_proxy_port", "3379", "pd proxy port")

	tikvBinaryPath = flag.String("tikv_binary_path", "bin/tikv-server", "tikv binary path")
	tikvPort       = flag.String("tikv_port", "22160", "tikv port")
	tikvStatusPort = flag.String("tikv_status_port", "22180", "tikv status port")

	varPath = flag.String("var_path", "var", "var path")
)

var _ = Suite(&TestGlobalKillSuite{})

// TestGlobakKillSuite is used for automated test of "Global Kill" feature.
// See https://github.com/pingcap/tidb/issues/8854.
type TestGlobalKillSuite struct {
	cmdPD            *exec.Cmd
	cmdTiDBWithoutPD *exec.Cmd
}

func (s *TestGlobalKillSuite) SetUpSuite(c *C) {
	logutil.InitLogger(&logutil.LogConfig{Config: zaplog.Config{Level: *logLevel}})

	// var err error

	// if s.cmdPD, err = s.startPD(); err != nil {
	// 	panic(fmt.Sprintf("startPD failed: %v", err))
	// }
}

func (s *TestGlobalKillSuite) TearDownSuite(c *C) {
	// var err error

	// if err = s.stopPD(); err != nil {
	// 	log.Warnf("stop pd server error: %v. Ignored.", err)
	// }
}

func (s *TestGlobalKillSuite) startPD() (cmd *exec.Cmd, err error) {
	cmd = exec.Command(*pdBinaryPath,
		"--name=pd_globalkilltest",
		"--force-new-cluster",
		fmt.Sprintf("--client-urls=http://%s", *pdClientPath),
		fmt.Sprintf("--peer-urls=http://%s", *pdPeerPath),
		fmt.Sprintf("--data-dir=%s/pd", *varPath),
		fmt.Sprintf("--log-file=%s/pd.log", *varPath))
	log.Infof("starting pd: %v", cmd)
	err = cmd.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	time.Sleep(500 * time.Millisecond)

	// check connection to PD.
	etcdLogCfg := zap.NewProductionConfig()
	etcdLogCfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	cli, err := clientv3.New(clientv3.Config{
		LogConfig:        &etcdLogCfg,
		Endpoints:        []string{*pdClientPath},
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBackoffMaxDelay(time.Second * 3),
		},
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer cli.Close()

	if err = cli.Sync(context.TODO()); err != nil { // use `Sync` to test connection.
		return nil, errors.Trace(err)
	}
	log.Infof("pd connected")

	return cmd, nil
}

func (s *TestGlobalKillSuite) startTiKV() (cmd *exec.Cmd, err error) {
	cmd = exec.Command(*tikvBinaryPath,
		fmt.Sprintf("--pd-endpoints=%s", *pdClientPath),
		fmt.Sprintf("--addr=127.0.0.1:%s", *tikvPort),
		fmt.Sprintf("--status-addr=127.0.0.1:%s", *tikvStatusPort),
		fmt.Sprintf("--data-dir=%s/tikv", *varPath),
		fmt.Sprintf("--log-file=%s/tikv.log", *varPath))
	log.Infof("starting tikv: %v", cmd)
	err = cmd.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	time.Sleep(500 * time.Millisecond)

	return cmd, nil
}

func (s *TestGlobalKillSuite) startTiDBWithoutPD(port int, statusPort int) (cmd *exec.Cmd, err error) {
	cmd = exec.Command(*tidbBinaryPath,
		"--store=mocktikv",
		fmt.Sprintf("-L=%s", *serverLogLevel),
		"--path=/tmp/globakkilltest_mocktikv", // TiDB requires absolute path here.
		fmt.Sprintf("-P=%d", port),
		fmt.Sprintf("--status=%d", statusPort),
		fmt.Sprintf("--log-file=%s/tidb%d.log", *varPath, port))
	log.Infof("starting tidb: %v", cmd)
	err = cmd.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	time.Sleep(500 * time.Millisecond)
	return cmd, nil
}

func (s *TestGlobalKillSuite) startTiDBWithPD(port int, statusPort int, pdPath string) (cmd *exec.Cmd, err error) {
	cmd = exec.Command(*tidbBinaryPath,
		"--store=tikv",
		fmt.Sprintf("-L=%s", *serverLogLevel),
		fmt.Sprintf("--path=%s", pdPath),
		fmt.Sprintf("-P=%d", port),
		fmt.Sprintf("--status=%d", statusPort),
		fmt.Sprintf("--log-file=%s/tidb%d.log", *varPath, port))
	log.Infof("starting tidb: %v", cmd)
	err = cmd.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	time.Sleep(500 * time.Millisecond)
	return cmd, nil
}

func (s *TestGlobalKillSuite) stopService(name string, cmd *exec.Cmd, graceful bool) (err error) {
	if graceful {
		if err = cmd.Process.Signal(os.Interrupt); err != nil {
			return errors.Trace(err)
		}
		if err = cmd.Wait(); err != nil {
			return errors.Trace(err)
		}
		log.Infof("service \"%s\" stopped gracefully", name)
		return nil
	}

	if err = cmd.Process.Kill(); err != nil {
		return errors.Trace(err)
	}
	time.Sleep(1 * time.Second)
	log.Infof("service \"%s\" killed", name)
	return nil
}

func (s *TestGlobalKillSuite) startPDProxy() (proxy *pdProxy, err error) {
	var p pdProxy
	from := fmt.Sprintf(":%s", *pdProxyPort)
	p.AddRoute(from, to(*pdClientPath))
	if err := p.Start(); err != nil {
		return nil, err
	}
	return &p, nil
}

// func (s *TestGlobalKillSuite) stopPDProxy(p *tcpproxy.Proxy) {
// 	p.Close()
// 	p.Wait()
// }

func (s *TestGlobalKillSuite) getTiDBConnection(port int) (db *sql.DB, err error) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	dsn := fmt.Sprintf("root@(%s)/test", addr)
	sleepTime := time.Millisecond * 250
	startTime := time.Now()
	for i := 0; i < 5; i++ {
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			log.Warnf("open addr %v failed, retry count %d err %v", addr, i, err)
			continue
		}
		err = db.Ping()
		if err == nil {
			break
		}
		log.Warnf("ping addr %v failed, retry count %d err %v", addr, i, err)

		db.Close()
		time.Sleep(sleepTime)
		sleepTime += sleepTime
	}
	if err != nil {
		log.Errorf("connect to server addr %v failed %v, take time %v", addr, err, time.Since(startTime))
		return nil, errors.Trace(err)
	}
	db.SetMaxOpenConns(10)

	log.Infof("connect to server %s ok", addr)
	return db, nil
}

const (
	waitToStartup = 500 * time.Millisecond

	// lostConnectionToPDTimeout is TiDB internal parameter.
	// Set by ldflag(github.com/pingcap/tidb/domain.ldflagLostConnectionToPDTimeout) for test purpose.
	// See TiDB Makefile.
	lostConnectionToPDTimeout = 3 * time.Second
)

func (s *TestGlobalKillSuite) killByCtrlC(c *C, port int, sleepTime int) time.Duration {
	cli := exec.Command("mysql",
		"-h127.0.0.1",
		fmt.Sprintf("-P%d", port),
		"-uroot",
		"-e", fmt.Sprintf("select sleep(%d);", sleepTime))
	log.Infof("run mysql cli: %v", cli)

	ch := make(chan time.Duration)
	go func() {
		startTS := time.Now()
		err := cli.Run()
		c.Assert(err, IsNil)

		elapsed := time.Now().Sub(startTS)
		log.Infof("mysql cli takes: %v", elapsed)
		ch <- elapsed
	}()

	time.Sleep(waitToStartup)               // wait before mysql cli running.
	err := cli.Process.Signal(os.Interrupt) // send "CTRL-C".
	c.Assert(err, IsNil)
	return <-ch
}

// NOTICE: db1 & db2 can be the same object, for getting conn1 & conn2 from the same TiDB instance.
func (s *TestGlobalKillSuite) killByKillStatement(c *C, db1 *sql.DB, db2 *sql.DB, sleepTime int) time.Duration {
	ctx := context.TODO()

	conn1, err := db1.Conn(ctx)
	c.Assert(err, IsNil)
	defer conn1.Close()

	var connID1 uint64
	err = conn1.QueryRowContext(ctx, "select connection_id();").Scan(&connID1)
	c.Assert(err, IsNil)
	log.Infof("connID1: 0x%x", connID1)

	ch := make(chan time.Duration)
	go func() {
		var err error
		startTS := time.Now()
		sql := fmt.Sprintf("select sleep(%d);", sleepTime)
		log.Infof("exec: %s [on 0x%x]", sql, connID1)
		rows, err := conn1.QueryContext(ctx, sql)
		c.Assert(err, IsNil)
		defer rows.Close()
		c.Assert(rows.Err(), IsNil)

		elapsed := time.Now().Sub(startTS)
		log.Infof("conn1 takes %v", elapsed)
		ch <- elapsed
	}()

	time.Sleep(waitToStartup) // wait go-routine to start.
	conn2, err := db2.Conn(ctx)
	c.Assert(err, IsNil)
	defer conn2.Close()

	var connID2 uint64
	err = conn2.QueryRowContext(ctx, "select connection_id();").Scan(&connID2)
	c.Assert(err, IsNil)
	log.Infof("connID2: 0x%x", connID2)

	log.Infof("exec: KILL QUERY %v(0x%x) [on 0x%x]", connID1, connID1, connID2)
	rows, err := conn2.QueryContext(ctx, fmt.Sprintf("KILL QUERY %v", connID1))
	c.Assert(err, IsNil)
	defer rows.Close()
	c.Assert(rows.Err(), IsNil)

	return <-ch
}

func (s *TestGlobalKillSuite) TestWithoutPD(c *C) {
	var err error

	port := *tidbStartPort
	tidb, err := s.startTiDBWithoutPD(port, *tidbStatusPort)
	c.Assert(err, IsNil)
	defer s.stopService("tidb", tidb, true)

	db, err := s.getTiDBConnection(port)
	c.Assert(err, IsNil)
	defer db.Close()

	const sleepTime = 2

	// Test mysql client CTRL-C
	// mysql client "CTRL-C" truncate connection id to 32bits, and is ignored by TiDB.
	elapsed := s.killByCtrlC(c, port, sleepTime)
	c.Assert(elapsed, GreaterEqual, sleepTime*time.Second)

	// Test KILL statement
	elapsed = s.killByKillStatement(c, db, db, sleepTime)
	c.Assert(elapsed, Less, sleepTime*time.Second)
}

func (s *TestGlobalKillSuite) TestOneTiDB(c *C) {
	port := *tidbStartPort + 1
	tidb, err := s.startTiDBWithPD(port, *tidbStatusPort+1, *pdClientPath)
	c.Assert(err, IsNil)
	defer s.stopService("tidb", tidb, true)

	db, err := s.getTiDBConnection(port)
	c.Assert(err, IsNil)
	defer db.Close()

	const sleepTime = 2

	// Test mysql client CTRL-C
	// mysql client "CTRL-C" truncate connection id to 32bits, and is ignored by TiDB.
	elapsed := s.killByCtrlC(c, port, sleepTime)
	c.Assert(elapsed, GreaterEqual, sleepTime*time.Second)

	// Test KILL statement
	elapsed = s.killByKillStatement(c, db, db, sleepTime)
	c.Assert(elapsed, Less, sleepTime*time.Second)
}

func (s *TestGlobalKillSuite) TestMultipleTiDB(c *C) {
	port1 := *tidbStartPort + 1
	tidb1, err := s.startTiDBWithPD(port1, *tidbStatusPort+1, *pdClientPath)
	c.Assert(err, IsNil)
	defer s.stopService("tidb1", tidb1, true)

	db1, err := s.getTiDBConnection(port1)
	c.Assert(err, IsNil)
	defer db1.Close()

	port2 := *tidbStartPort + 2
	tidb2, err := s.startTiDBWithPD(port2, *tidbStatusPort+2, *pdClientPath)
	c.Assert(err, IsNil)
	defer s.stopService("tidb2", tidb2, true)

	db2, err := s.getTiDBConnection(port2)
	c.Assert(err, IsNil)
	defer db2.Close()

	const sleepTime = 2

	elapsed := s.killByKillStatement(c, db1, db2, sleepTime)
	c.Assert(elapsed, Less, sleepTime*time.Second)
}

func (s *TestGlobalKillSuite) TestLostConnection(c *C) {
	pdProxy, err := s.startPDProxy()
	c.Assert(err, IsNil)
	pdPath := fmt.Sprintf("127.0.0.1:%s", *pdProxyPort)

	port1 := *tidbStartPort + 1
	tidb1, err := s.startTiDBWithPD(port1, *tidbStatusPort+1, pdPath)
	c.Assert(err, IsNil)
	defer s.stopService("tidb1", tidb1, false)

	db1, err := s.getTiDBConnection(port1)
	c.Assert(err, IsNil)
	defer db1.Close()

	port2 := *tidbStartPort + 2
	tidb2, err := s.startTiDBWithPD(port2, *tidbStatusPort+2, pdPath)
	c.Assert(err, IsNil)
	defer s.stopService("tidb2", tidb2, false)

	db2, err := s.getTiDBConnection(port2)
	c.Assert(err, IsNil)
	defer db2.Close()

	// verify it's working.
	ctx := context.TODO()
	conn1, err := db1.Conn(ctx)
	c.Assert(err, IsNil)
	defer conn1.Close()
	err = conn1.PingContext(ctx)
	c.Assert(err, IsNil)

	// disconnect to PD by close proxy.
	pdProxy.Close()
	pdProxy.closeAllConnections()

	// wait for "lostConnectionToPDTimeout" elapsed.
	time.Sleep(lostConnectionToPDTimeout + 3*time.Second)

	// check connection.
	log.Infof("check connection after lost connection to PD.")
	_, err = s.getTiDBConnection(port1)
	c.Assert(err, NotNil)
	// ctx1, cancel := context.WithTimeout(ctx, 1*time.Second)
	// _, err = db1.QueryContext(ctx1, "select 1;")
	// c.Assert(err, IsNil)
	// defer conn1.Close()
	// var i int
	// err = conn1.QueryRowContext(ctx1, "select 1;").Scan(&i)
	// cancel()
	// c.Assert(err, NotNil)
	log.Infof("err: %v", err)
}
