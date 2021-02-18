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

package graceshutdown

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
	log "github.com/sirupsen/logrus"
)

var (
	tidbBinaryPath = flag.String("s", "bin/tidb-server", "tidb server binary path")
	tmpPath        = flag.String("tmp", "/tmp/tidb_gracefulshutdown", "temporary files path")
	tidbStartPort  = flag.Int("tidb_start_port", 5500, "first tidb server listening port")
	tidbStatusPort = flag.Int("tidb_status_port", 8500, "first tidb server status port")
)

func TestGracefulShutdown(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&TestGracefulShutdownSuite{})

type TestGracefulShutdownSuite struct {
}

func (s *TestGracefulShutdownSuite) SetUpSuite(c *C) {
}
func (s *TestGracefulShutdownSuite) TearDownSuite(c *C) {
}

func (s *TestGracefulShutdownSuite) startTiDBWithoutPD(port int, statusPort int) (cmd *exec.Cmd, err error) {
	cmd = exec.Command(*tidbBinaryPath,
		"--store=mocktikv",
		fmt.Sprintf("--path=%s/mocktikv", *tmpPath),
		fmt.Sprintf("-P=%d", port),
		fmt.Sprintf("--status=%d", statusPort),
		fmt.Sprintf("--log-file=%s/tidb%d.log", *tmpPath, port))
	log.Infof("starting tidb: %v", cmd)
	err = cmd.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	time.Sleep(500 * time.Millisecond)
	return cmd, nil
}

func (s *TestGracefulShutdownSuite) stopService(name string, cmd *exec.Cmd) (err error) {
	if err = cmd.Process.Signal(os.Interrupt); err != nil {
		return errors.Trace(err)
	}
	log.Infof("service \"%s\" Interrupt", name)
	if err = cmd.Wait(); err != nil {
		return errors.Trace(err)
	}
	log.Infof("service \"%s\" stopped gracefully", name)
	return nil
}

func (s *TestGracefulShutdownSuite) connectTiDB(port int) (db *sql.DB, err error) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	dsn := fmt.Sprintf("root@(%s)/test", addr)
	sleepTime := 250 * time.Millisecond
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

func (s *TestGracefulShutdownSuite) TestGracefulShutdown(c *C) {
	port := *tidbStartPort + 1
	tidb, err := s.startTiDBWithoutPD(port, *tidbStatusPort)
	c.Assert(err, IsNil)

	db, err := s.connectTiDB(port)
	c.Assert(err, IsNil)
	defer db.Close()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	conn1, err := db.Conn(ctx)
	c.Assert(err, IsNil)
	defer conn1.Close()

	_, err = conn1.ExecContext(ctx, "drop table if exists t;")
	c.Assert(err, IsNil)
	_, err = conn1.ExecContext(ctx, "create table t(a int);")
	c.Assert(err, IsNil)
	_, err = conn1.ExecContext(ctx, "insert into t values(1);")
	c.Assert(err, IsNil)

	go func() {
		time.Sleep(1e9)
		err = s.stopService("tidb", tidb)
		c.Assert(err, IsNil)
	}()

	sql := `select 1 from t where not (select sleep(3)) ;`
	var a int64
	err = conn1.QueryRowContext(ctx, sql).Scan(&a)
	c.Assert(err, IsNil)
	c.Assert(a, Equals, int64(1))
}
