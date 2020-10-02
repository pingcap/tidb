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
	"sync"
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
	pdClientPath = flag.String("pd_client_path", "http://127.0.0.1:3379", "pd client path")
	pdPeerPath   = flag.String("pd_peer_path", "http://127.0.0.1:3380", "pd peer path")

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
		fmt.Sprintf("--client-urls=%s", *pdClientPath),
		fmt.Sprintf("--peer-urls=%s", *pdPeerPath),
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

func (s *TestGlobalKillSuite) stopPD() (err error) {
	if err = s.cmdPD.Process.Signal(os.Interrupt); err != nil {
		return errors.Trace(err)
	}
	if err = s.cmdPD.Wait(); err != nil {
		return errors.Trace(err)
	}
	s.cmdPD = nil
	return nil
}

func (s *TestGlobalKillSuite) startTiDBWithoutPD(port int) (cmd *exec.Cmd, err error) {
	cmd = exec.Command(*tidbBinaryPath,
		"--store=mocktikv",
		fmt.Sprintf("-L=%s", *serverLogLevel),
		"--path=/tmp/globakkilltest_mocktikv", // TiDB requires absolute path here.
		fmt.Sprintf("-P=%d", port),
		fmt.Sprintf("--status=%d", *tidbStatusPort),
		fmt.Sprintf("--log-file=%s/tidb%d.log", *varPath, port))
	log.Infof("starting tidb: %v", cmd)
	err = cmd.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	time.Sleep(500 * time.Millisecond)
	return cmd, nil
}

func (s *TestGlobalKillSuite) stopTiDB(cmd *exec.Cmd, graceful bool) (err error) {
	if graceful {
		if err = cmd.Process.Signal(os.Interrupt); err != nil {
			return errors.Trace(err)
		}
		if err = cmd.Wait(); err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	if err = cmd.Process.Kill(); err != nil {
		return errors.Trace(err)
	}
	time.Sleep(1 * time.Second)
	return nil
}

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

func (s *TestGlobalKillSuite) TestWithoutPD(c *C) {
	var err error

	port := *tidbStartPort
	tidb, err := s.startTiDBWithoutPD(port)
	c.Assert(err, IsNil)
	defer s.stopTiDB(tidb, true)

	db, err := s.getTiDBConnection(port)
	c.Assert(err, IsNil)
	defer db.Close()

	// Test mysql client CTRL-C
	cli := exec.Command("mysql",
		"-h127.0.0.1",
		fmt.Sprintf("-P%d", port),
		"-uroot",
		"-e", "select sleep(3);")
	log.Infof("running mysql cli: %v", cli)

	err = cli.Start()
	c.Assert(err, IsNil)
	startTS := time.Now()

	time.Sleep(500 * time.Millisecond)     // wait before mysql cli startup.
	err = cli.Process.Signal(os.Interrupt) // send "CTRL-C".
	c.Assert(err, IsNil)
	err = cli.Wait()
	c.Assert(err, IsNil)

	elapsed := time.Now().Sub(startTS)
	// mysql client "CTRL-C" truncate connection id to 32bits, and is ignored by TiDB.
	c.Assert(elapsed, GreaterEqual, 3*time.Second)

	// Test KILL statement
	ctx := context.TODO()
	conn1, err := db.Conn(ctx)
	c.Assert(err, IsNil)
	defer conn1.Close()

	var connID1 int64
	err = conn1.QueryRowContext(ctx, "select connection_id();").Scan(&connID1)
	c.Assert(err, IsNil)
	log.Infof("connID1: %v", connID1)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		rows, err := conn1.QueryContext(ctx, "select sleep(5);")
		c.Assert(err, IsNil)
		defer rows.Close()
		c.Assert(rows.Err(), IsNil)
	}()

	startTS = time.Now()
	time.Sleep(500 * time.Millisecond) // wait go-routine to start.
	conn2, err := db.Conn(ctx)
	c.Assert(err, IsNil)
	defer conn2.Close()
	rows, err := conn2.QueryContext(ctx, fmt.Sprintf("KILL %v", connID1))
	c.Assert(err, IsNil)
	defer rows.Close()
	c.Assert(rows.Err(), IsNil)

	wg.Wait()
	elapsed = time.Now().Sub(startTS)
	log.Infof("conn1 takes %v", elapsed)
	c.Assert(elapsed, Less, 5*time.Second)
}
