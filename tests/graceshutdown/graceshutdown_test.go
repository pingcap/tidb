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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	tidbBinaryPath = flag.String("s", "bin/tidb-server", "tidb server binary path")
	tmpPath        = flag.String("tmp", "/tmp/tidb_gracefulshutdown", "temporary files path")
	tidbStartPort  = flag.Int("tidb_start_port", 5500, "first tidb server listening port")
	tidbStatusPort = flag.Int("tidb_status_port", 8500, "first tidb server status port")
)

func startTiDBWithoutPD(port int, statusPort int) (cmd *exec.Cmd, err error) {
	cmd = exec.Command(*tidbBinaryPath,
		"--store=mocktikv",
		fmt.Sprintf("--path=%s/mocktikv", *tmpPath),
		fmt.Sprintf("-P=%d", port),
		fmt.Sprintf("--status=%d", statusPort),
		fmt.Sprintf("--log-file=%s/tidb%d.log", *tmpPath, port))
	log.Info("starting tidb", zap.Any("cmd", cmd))
	err = cmd.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	time.Sleep(500 * time.Millisecond)
	return cmd, nil
}

func stopService(name string, cmd *exec.Cmd) (err error) {
	if err = cmd.Process.Signal(os.Interrupt); err != nil {
		return errors.Trace(err)
	}
	log.Info("service Interrupt", zap.String("name", name))
	if err = cmd.Wait(); err != nil {
		return errors.Trace(err)
	}
	log.Info("service stopped gracefully", zap.String("name", name))
	return nil
}

func connectTiDB(port int) (db *sql.DB, err error) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	dsn := fmt.Sprintf("root@(%s)/test", addr)
	sleepTime := 250 * time.Millisecond
	startTime := time.Now()
	for i := 0; i < 5; i++ {
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

		err = db.Close()
		if err != nil {
			log.Warn("close db failed", zap.Int("retry count", i), zap.Error(err))
			break
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

func TestGracefulShutdown(t *testing.T) {
	port := *tidbStartPort + 1
	tidb, err := startTiDBWithoutPD(port, *tidbStatusPort)
	require.NoError(t, err)

	db, err := connectTiDB(port)
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	conn1, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn1.Close())
	}()

	_, err = conn1.ExecContext(ctx, "drop table if exists t;")
	require.NoError(t, err)
	_, err = conn1.ExecContext(ctx, "create table t(a int);")
	require.NoError(t, err)
	_, err = conn1.ExecContext(ctx, "insert into t values(1);")
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		time.Sleep(time.Second)
		err = stopService("tidb", tidb)
		require.NoError(t, err)
		close(done)
	}()

	sql := `select 1 from t where not (select sleep(3)) ;`
	var a int64
	err = conn1.QueryRowContext(ctx, sql).Scan(&a)
	require.NoError(t, err)
	require.Equal(t, a, int64(1))
	<-done
}
