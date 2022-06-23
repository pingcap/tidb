// Copyright 2015 PingCAP, Inc.
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

package ddltest

import (
	goctx "context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/store"
	tidbdriver "github.com/pingcap/tidb/store/driver"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	etcd              = flag.String("etcd", "127.0.0.1:2379", "etcd path")
	tidbIP            = flag.String("tidb_ip", "127.0.0.1", "tidb-server ip address")
	tikvPath          = flag.String("tikv_path", "", "tikv path")
	lease             = flag.Int("lease", 1, "DDL schema lease time, seconds")
	serverNum         = flag.Int("server_num", 3, "Maximum running tidb server")
	startPort         = flag.Int("start_port", 5000, "First tidb-server listening port")
	statusPort        = flag.Int("status_port", 8000, "First tidb-server status port")
	logLevel          = flag.String("L", "error", "log level")
	ddlServerLogLevel = flag.String("ddl_log_level", "fatal", "DDL server log level")
	dataNum           = flag.Int("n", 100, "minimal test dataset for a table")
	enableRestart     = flag.Bool("enable_restart", true, "whether random restart servers for tests")
)

type server struct {
	*exec.Cmd
	logFP *os.File
	db    *sql.DB
	addr  string
}

type ddlSuite struct {
	store kv.Storage
	dom   *domain.Domain
	s     session.Session
	ctx   sessionctx.Context

	m     sync.Mutex
	procs []*server

	wg   sync.WaitGroup
	quit chan struct{}

	retryCount int
}

func createDDLSuite(t *testing.T) (s *ddlSuite) {
	var err error
	s = new(ddlSuite)

	s.quit = make(chan struct{})

	s.store, err = store.New(fmt.Sprintf("tikv://%s%s", *etcd, *tikvPath))
	require.NoError(t, err)

	// Make sure the schema lease of this session is equal to other TiDB servers'.
	session.SetSchemaLease(time.Duration(*lease) * time.Second)

	s.dom, err = session.BootstrapSession(s.store)
	require.NoError(t, err)

	s.s, err = session.CreateSession(s.store)
	require.NoError(t, err)

	s.ctx = s.s.(sessionctx.Context)
	goCtx := goctx.Background()
	_, err = s.s.Execute(goCtx, "create database if not exists test_ddl")
	require.NoError(t, err)

	s.Bootstrap(t)

	// Stop current DDL worker, so that we can't be the owner now.
	err = domain.GetDomain(s.ctx).DDL().Stop()
	require.NoError(t, err)
	ddl.RunWorker = false
	session.ResetStoreForWithTiKVTest(s.store)
	s.dom.Close()
	require.NoError(t, s.store.Close())

	s.store, err = store.New(fmt.Sprintf("tikv://%s%s", *etcd, *tikvPath))
	require.NoError(t, err)
	s.s, err = session.CreateSession(s.store)
	require.NoError(t, err)
	s.dom, err = session.BootstrapSession(s.store)
	require.NoError(t, err)
	s.ctx = s.s.(sessionctx.Context)
	_, err = s.s.Execute(goCtx, "use test_ddl")
	require.NoError(t, err)

	addEnvPath("..")

	// Start multi tidb servers
	s.procs = make([]*server, *serverNum)

	// Set server restart retry count.
	s.retryCount = 20

	createLogFiles(t, *serverNum)
	err = s.startServers()
	require.NoError(t, err)

	s.wg.Add(1)
	go s.restartServerRegularly()

	return
}

// restartServerRegularly restarts a tidb server regularly.
func (s *ddlSuite) restartServerRegularly() {
	defer s.wg.Done()

	var err error
	after := *lease * (6 + randomIntn(6))
	for {
		select {
		case <-time.After(time.Duration(after) * time.Second):
			if *enableRestart {
				err = s.restartServerRand()
				if err != nil {
					log.Fatal("restartServerRand failed", zap.Error(err))
				}
			}
		case <-s.quit:
			return
		}
	}
}

func (s *ddlSuite) teardown(t *testing.T) {
	close(s.quit)
	s.wg.Wait()

	s.dom.Close()
	// TODO: Remove these logs after testing.
	quitCh := make(chan struct{})
	go func() {
		select {
		case <-time.After(100 * time.Second):
			log.Error("testing timeout", zap.Stack("stack"))
		case <-quitCh:
		}
	}()
	err := s.store.Close()
	require.NoError(t, err)
	close(quitCh)

	err = s.stopServers()
	require.NoError(t, err)
}

func (s *ddlSuite) startServers() (err error) {
	s.m.Lock()
	defer s.m.Unlock()

	for i := 0; i < len(s.procs); i++ {
		if s.procs[i] != nil {
			continue
		}

		// Open log file.
		logFP, err := os.OpenFile(fmt.Sprintf("%s%d", logFilePrefix, i), os.O_RDWR, 0766)
		if err != nil {
			return errors.Trace(err)
		}

		s.procs[i], err = s.startServer(i, logFP)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (s *ddlSuite) killServer(proc *os.Process) error {
	// Make sure this tidb is killed, and it makes the next tidb that has the same port as this one start quickly.
	err := proc.Kill()
	if err != nil {
		log.Error("kill server failed", zap.Error(err))
		return errors.Trace(err)
	}
	_, err = proc.Wait()
	if err != nil {
		log.Error("kill server, wait failed", zap.Error(err))
		return errors.Trace(err)
	}

	time.Sleep(1 * time.Second)
	return nil
}

func (s *ddlSuite) stopServers() error {
	s.m.Lock()
	defer s.m.Unlock()

	for i := 0; i < len(s.procs); i++ {
		if proc := s.procs[i]; proc != nil {
			if proc.db != nil {
				if err := proc.db.Close(); err != nil {
					return err
				}
			}

			err := s.killServer(proc.Process)
			if err != nil {
				return errors.Trace(err)
			}
			s.procs[i] = nil
		}
	}
	return nil
}

var logFilePrefix = "tidb_log_file_"

func createLogFiles(t *testing.T, length int) {
	for i := 0; i < length; i++ {
		fp, err := os.Create(fmt.Sprintf("%s%d", logFilePrefix, i))
		if err != nil {
			require.NoError(t, err)
		}
		require.NoError(t, fp.Close())
	}
}

func (s *ddlSuite) startServer(i int, fp *os.File) (*server, error) {
	cmd := exec.Command("ddltest_tidb-server",
		"--store=tikv",
		fmt.Sprintf("-L=%s", *ddlServerLogLevel),
		fmt.Sprintf("--path=%s%s", *etcd, *tikvPath),
		fmt.Sprintf("-P=%d", *startPort+i),
		fmt.Sprintf("--status=%d", *statusPort+i),
		fmt.Sprintf("--lease=%d", *lease))
	cmd.Stderr = fp
	cmd.Stdout = fp
	err := cmd.Start()
	if err != nil {
		return nil, errors.Trace(err)
	}
	time.Sleep(500 * time.Millisecond)

	// Make sure tidb server process is started.
	ps := fmt.Sprintf("ps -aux|grep ddltest_tidb|grep %d", *startPort+i)
	output, _ := exec.Command("sh", "-c", ps).Output()
	if !strings.Contains(string(output), "ddltest_tidb-server") {
		time.Sleep(1 * time.Second)
	}

	// Open database.
	var db *sql.DB
	addr := fmt.Sprintf("%s:%d", *tidbIP, *startPort+i)
	sleepTime := time.Millisecond * 250
	startTime := time.Now()
	for i := 0; i < s.retryCount; i++ {
		db, err = sql.Open("mysql", fmt.Sprintf("root@(%s)/test_ddl", addr))
		if err != nil {
			log.Warn("open addr failed", zap.String("addr", addr), zap.Int("retry count", i), zap.Error(err))
			continue
		}
		err = db.Ping()
		if err == nil {
			break
		}
		log.Warn("ping addr failed", zap.String("addr", addr), zap.Int("retry count", i), zap.Error(err))

		err = db.Close()
		if err != nil {
			log.Warn("close db failed", zap.Int("retry count", i), zap.Error(err))
			break
		}
		time.Sleep(sleepTime)
		sleepTime += sleepTime
	}
	if err != nil {
		log.Error("restart server addr failed",
			zap.String("addr", addr),
			zap.Duration("take time", time.Since(startTime)),
			zap.Error(err),
		)
		return nil, errors.Trace(err)
	}
	db.SetMaxOpenConns(10)

	_, err = db.Exec("use test_ddl")
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Info("start server ok", zap.String("addr", addr), zap.Error(err))

	return &server{
		Cmd:   cmd,
		db:    db,
		addr:  addr,
		logFP: fp,
	}, nil
}

func (s *ddlSuite) restartServerRand() error {
	i := rand.Intn(*serverNum)

	s.m.Lock()
	defer s.m.Unlock()

	if s.procs[i] == nil {
		return nil
	}

	server := s.procs[i]
	s.procs[i] = nil
	log.Warn("begin to restart", zap.String("addr", server.addr))
	err := s.killServer(server.Process)
	if err != nil {
		return errors.Trace(err)
	}

	s.procs[i], err = s.startServer(i, server.logFP)
	return errors.Trace(err)
}

func isRetryError(err error) bool {
	if err == nil {
		return false
	}

	if terror.ErrorEqual(err, driver.ErrBadConn) ||
		strings.Contains(err.Error(), "connection refused") ||
		strings.Contains(err.Error(), "getsockopt: connection reset by peer") ||
		strings.Contains(err.Error(), "KV error safe to retry") ||
		strings.Contains(err.Error(), "try again later") ||
		strings.Contains(err.Error(), "invalid connection") {
		return true
	}

	// TODO: Check the specific columns number.
	if strings.Contains(err.Error(), "Column count doesn't match value count at row") {
		log.Warn("err", zap.Error(err))
		return false
	}

	log.Error("can not retry", zap.Error(err))

	return false
}

func (s *ddlSuite) exec(query string, args ...interface{}) (sql.Result, error) {
	for {
		server := s.getServer()
		r, err := server.db.Exec(query, args...)
		if isRetryError(err) {
			log.Error("exec in server, retry",
				zap.String("query", query),
				zap.String("addr", server.addr),
				zap.Error(err),
			)
			continue
		}

		return r, err
	}
}

func (s *ddlSuite) mustExec(query string, args ...interface{}) sql.Result {
	r, err := s.exec(query, args...)
	if err != nil {
		log.Fatal("[mustExec fail]query",
			zap.String("query", query),
			zap.Any("args", args),
			zap.Error(err),
		)
	}

	return r
}

func (s *ddlSuite) execInsert(query string, args ...interface{}) sql.Result {
	for {
		r, err := s.exec(query, args...)
		if err == nil {
			return r
		}

		if *enableRestart {
			// If you use enable random restart servers, we should ignore key exists error.
			if strings.Contains(err.Error(), "Duplicate entry") &&
				strings.Contains(err.Error(), "for key") {
				return r
			}
		}

		log.Fatal("[execInsert fail]query",
			zap.String("query", query),
			zap.Any("args", args),
			zap.Error(err),
		)
	}
}

func (s *ddlSuite) query(query string, args ...interface{}) (*sql.Rows, error) {
	for {
		server := s.getServer()
		r, err := server.db.Query(query, args...)
		if isRetryError(err) {
			log.Error("query in server, retry",
				zap.String("query", query),
				zap.String("addr", server.addr),
				zap.Error(err),
			)
			continue
		}

		return r, err
	}
}

func (s *ddlSuite) getServer() *server {
	s.m.Lock()
	defer s.m.Unlock()

	for i := 0; i < 20; i++ {
		i := rand.Intn(*serverNum)

		if s.procs[i] != nil {
			return s.procs[i]
		}
	}

	log.Fatal("try to get server too many times")
	return nil
}

// runDDL executes the DDL query, returns a channel so that you can use it to wait DDL finished.
func (s *ddlSuite) runDDL(sql string) chan error {
	done := make(chan error, 1)
	go func() {
		_, err := s.s.Execute(goctx.Background(), sql)
		// We must wait 2 * lease time to guarantee all servers update the schema.
		if err == nil {
			time.Sleep(time.Duration(*lease) * time.Second * 2)
		}

		done <- err
	}()

	return done
}

func (s *ddlSuite) getTable(t *testing.T, name string) table.Table {
	tbl, err := domain.GetDomain(s.ctx).InfoSchema().TableByName(model.NewCIStr("test_ddl"), model.NewCIStr(name))
	require.NoError(t, err)
	return tbl
}

func dumpRows(t *testing.T, rows *sql.Rows) [][]interface{} {
	cols, err := rows.Columns()
	require.NoError(t, err)
	var ay [][]interface{}
	for rows.Next() {
		v := make([]interface{}, len(cols))
		for i := range v {
			v[i] = new(interface{})
		}
		err = rows.Scan(v...)
		require.NoError(t, err)

		for i := range v {
			v[i] = *(v[i].(*interface{}))
		}
		ay = append(ay, v)
	}

	require.NoError(t, rows.Close())
	require.NoErrorf(t, rows.Err(), "%v", ay)
	return ay
}

func matchRows(t *testing.T, rows *sql.Rows, expected [][]interface{}) {
	ay := dumpRows(t, rows)
	require.Equalf(t, len(expected), len(ay), "%v", expected)
	for i := range ay {
		match(t, ay[i], expected[i]...)
	}
}

func match(t *testing.T, row []interface{}, expected ...interface{}) {
	require.Equal(t, len(expected), len(row))
	for i := range row {
		if row[i] == nil {
			require.Nil(t, expected[i])
			continue
		}

		got, err := types.ToString(row[i])
		require.NoError(t, err)

		need, err := types.ToString(expected[i])
		require.NoError(t, err)
		require.Equal(t, need, got)
	}
}

func (s *ddlSuite) Bootstrap(t *testing.T) {
	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test_ddl")
	tk.MustExec("drop table if exists test_index, test_column, test_insert, test_conflict_insert, " +
		"test_update, test_conflict_update, test_delete, test_conflict_delete, test_mixed, test_inc")

	tk.MustExec("create table test_index (c int, c1 bigint, c2 double, c3 varchar(256), primary key(c))")
	tk.MustExec("create table test_column (c1 int, c2 int, primary key(c1))")
	tk.MustExec("create table test_insert (c1 int, c2 int, primary key(c1))")
	tk.MustExec("create table test_conflict_insert (c1 int, c2 int, primary key(c1))")
	tk.MustExec("create table test_update (c1 int, c2 int, primary key(c1))")
	tk.MustExec("create table test_conflict_update (c1 int, c2 int, primary key(c1))")
	tk.MustExec("create table test_delete (c1 int, c2 int, primary key(c1))")
	tk.MustExec("create table test_conflict_delete (c1 int, c2 int, primary key(c1))")
	tk.MustExec("create table test_mixed (c1 int, c2 int, primary key(c1))")
	tk.MustExec("create table test_inc (c1 int, c2 int, primary key(c1))")

	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists test_insert_common, test_conflict_insert_common, " +
		"test_update_common, test_conflict_update_common, test_delete_common, test_conflict_delete_common, " +
		"test_mixed_common, test_inc_common")
	tk.MustExec("create table test_insert_common (c1 int, c2 int, primary key(c1, c2))")
	tk.MustExec("create table test_conflict_insert_common (c1 int, c2 int, primary key(c1, c2))")
	tk.MustExec("create table test_update_common (c1 int, c2 int, primary key(c1, c2))")
	tk.MustExec("create table test_conflict_update_common (c1 int, c2 int, primary key(c1, c2))")
	tk.MustExec("create table test_delete_common (c1 int, c2 int, primary key(c1, c2))")
	tk.MustExec("create table test_conflict_delete_common (c1 int, c2 int, primary key(c1, c2))")
	tk.MustExec("create table test_mixed_common (c1 int, c2 int, primary key(c1, c2))")
	tk.MustExec("create table test_inc_common (c1 int, c2 int, primary key(c1, c2))")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
}

func TestSimple(t *testing.T) {
	s := createDDLSuite(t)
	defer s.teardown(t)

	t.Run("Basic", func(t *testing.T) {
		done := s.runDDL("create table if not exists test_simple (c1 int, c2 int, c3 int)")
		err := <-done
		require.NoError(t, err)

		_, err = s.exec("insert into test_simple values (1, 1, 1)")
		require.NoError(t, err)

		rows, err := s.query("select c1 from test_simple limit 1")
		require.NoError(t, err)
		matchRows(t, rows, [][]interface{}{{1}})

		done = s.runDDL("drop table if exists test_simple")
		err = <-done
		require.NoError(t, err)
	})
	t.Run("Mixed", func(t *testing.T) {
		tests := []struct {
			name string
		}{
			{"test_mixed"},
			{"test_mixed_common"},
		}

		for _, test := range tests {
			tblName := test.name
			t.Run(test.name, func(t *testing.T) {
				workerNum := 10
				rowCount := 10000
				batch := rowCount / workerNum

				start := time.Now()

				var wg sync.WaitGroup
				wg.Add(workerNum)
				for i := 0; i < workerNum; i++ {
					go func(i int) {
						defer wg.Done()

						for j := 0; j < batch; j++ {
							k := batch*i + j
							s.execInsert(fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
						}
					}(i)
				}
				wg.Wait()

				end := time.Now()
				fmt.Printf("[TestSimpleMixed][Insert][Time Cost]%v\n", end.Sub(start))

				start = time.Now()

				rowID := int64(rowCount)
				defaultValue := int64(-1)

				wg.Add(workerNum)
				for i := 0; i < workerNum; i++ {
					go func() {
						defer wg.Done()

						for j := 0; j < batch; j++ {
							key := atomic.AddInt64(&rowID, 1)
							s.execInsert(fmt.Sprintf("insert into %s values (%d, %d)", tblName, key, key))
							key = int64(randomNum(rowCount))
							s.mustExec(fmt.Sprintf("update %s set c2 = %d where c1 = %d", tblName, defaultValue, key))
							key = int64(randomNum(rowCount))
							s.mustExec(fmt.Sprintf("delete from %s where c1 = %d", tblName, key))
						}
					}()
				}
				wg.Wait()

				end = time.Now()
				fmt.Printf("[TestSimpleMixed][Mixed][Time Cost]%v\n", end.Sub(start))

				ctx := s.ctx
				err := sessiontxn.NewTxn(goctx.Background(), ctx)
				require.NoError(t, err)

				tbl := s.getTable(t, tblName)
				updateCount := int64(0)
				insertCount := int64(0)
				err = tables.IterRecords(tbl, ctx, tbl.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
					if reflect.DeepEqual(data[1].GetValue(), data[0].GetValue()) {
						insertCount++
					} else if reflect.DeepEqual(data[1].GetValue(), defaultValue) && data[0].GetInt64() < int64(rowCount) {
						updateCount++
					} else {
						log.Fatal("[TestSimpleMixed fail]invalid row", zap.Any("row", data))
					}

					return true, nil
				})
				require.NoError(t, err)

				deleteCount := atomic.LoadInt64(&rowID) - insertCount - updateCount
				require.Greater(t, insertCount, int64(0))
				require.Greater(t, updateCount, int64(0))
				require.Greater(t, deleteCount, int64(0))
			})
		}
	})
	t.Run("Inc", func(t *testing.T) {
		tests := []struct {
			name string
		}{
			{"test_inc"},
			{"test_inc_common"},
		}

		for _, test := range tests {
			tblName := test.name
			t.Run(test.name, func(t *testing.T) {
				workerNum := 10
				rowCount := 1000
				batch := rowCount / workerNum

				start := time.Now()

				var wg sync.WaitGroup
				wg.Add(workerNum)
				for i := 0; i < workerNum; i++ {
					go func(i int) {
						defer wg.Done()

						for j := 0; j < batch; j++ {
							k := batch*i + j
							s.execInsert(fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
						}
					}(i)
				}
				wg.Wait()

				end := time.Now()
				fmt.Printf("[TestSimpleInc][Insert][Time Cost]%v\n", end.Sub(start))

				start = time.Now()

				wg.Add(workerNum)
				for i := 0; i < workerNum; i++ {
					go func() {
						defer wg.Done()

						for j := 0; j < batch; j++ {
							s.mustExec(fmt.Sprintf("update %s set c2 = c2 + 1 where c1 = 0", tblName))
						}
					}()
				}
				wg.Wait()

				end = time.Now()
				fmt.Printf("[TestSimpleInc][Update][Time Cost]%v\n", end.Sub(start))

				ctx := s.ctx
				err := sessiontxn.NewTxn(goctx.Background(), ctx)
				require.NoError(t, err)

				tbl := s.getTable(t, "test_inc")
				err = tables.IterRecords(tbl, ctx, tbl.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
					if reflect.DeepEqual(data[0].GetValue(), int64(0)) {
						if *enableRestart {
							require.GreaterOrEqual(t, data[1].GetValue(), int64(rowCount))
						} else {
							require.Equal(t, int64(rowCount), data[1].GetValue())
						}
					} else {
						require.Equal(t, data[1].GetValue(), data[0].GetValue())
					}

					return true, nil
				})
				require.NoError(t, err)
			})
		}
	})
}

func TestSimpleInsert(t *testing.T) {
	s := createDDLSuite(t)
	defer s.teardown(t)

	t.Run("Basic", func(t *testing.T) {
		tests := []struct {
			name string
		}{
			{"test_insert"},
			{"test_insert_common"},
		}

		for _, test := range tests {
			tblName := test.name
			t.Run(test.name, func(t *testing.T) {
				workerNum := 10
				rowCount := 10000
				batch := rowCount / workerNum

				start := time.Now()

				var wg sync.WaitGroup
				wg.Add(workerNum)
				for i := 0; i < workerNum; i++ {
					go func(i int) {
						defer wg.Done()

						for j := 0; j < batch; j++ {
							k := batch*i + j
							s.execInsert(fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
						}
					}(i)
				}
				wg.Wait()

				end := time.Now()
				fmt.Printf("[TestSimpleInsert][Time Cost]%v\n", end.Sub(start))

				ctx := s.ctx
				err := sessiontxn.NewTxn(goctx.Background(), ctx)
				require.NoError(t, err)

				tbl := s.getTable(t, "test_insert")
				handles := kv.NewHandleMap()
				err = tables.IterRecords(tbl, ctx, tbl.Cols(), func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
					handles.Set(h, struct{}{})
					require.Equal(t, data[1].GetValue(), data[0].GetValue())
					return true, nil
				})
				require.NoError(t, err)
				require.Equal(t, rowCount, handles.Len())
			})
		}
	})
	t.Run("Conflict", func(t *testing.T) {
		tests := []struct {
			name string
		}{
			{"test_conflict_insert"},
			{"test_conflict_insert_common"},
		}

		for _, test := range tests {
			tblName := test.name
			t.Run(test.name, func(t *testing.T) {
				var mu sync.Mutex
				keysMap := make(map[int64]int64)

				workerNum := 10
				rowCount := 10000
				batch := rowCount / workerNum

				start := time.Now()

				var wg sync.WaitGroup
				wg.Add(workerNum)
				for i := 0; i < workerNum; i++ {
					go func() {
						defer wg.Done()

						for j := 0; j < batch; j++ {
							k := randomNum(rowCount)
							_, _ = s.exec(fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
							mu.Lock()
							keysMap[int64(k)] = int64(k)
							mu.Unlock()
						}
					}()
				}
				wg.Wait()

				end := time.Now()
				fmt.Printf("[TestSimpleConflictInsert][Time Cost]%v\n", end.Sub(start))

				ctx := s.ctx
				err := sessiontxn.NewTxn(goctx.Background(), ctx)
				require.NoError(t, err)

				tbl := s.getTable(t, tblName)
				handles := kv.NewHandleMap()
				err = tables.IterRecords(tbl, ctx, tbl.Cols(), func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
					handles.Set(h, struct{}{})
					require.Contains(t, keysMap, data[0].GetValue())
					require.Equal(t, data[1].GetValue(), data[0].GetValue())
					return true, nil
				})
				require.NoError(t, err)
				require.Len(t, keysMap, handles.Len())
			})
		}
	})
}

func TestSimpleUpdate(t *testing.T) {
	s := createDDLSuite(t)
	defer s.teardown(t)

	t.Run("Basic", func(t *testing.T) {
		tests := []struct {
			name string
		}{
			{"test_update"},
			{"test_update_common"},
		}

		for _, test := range tests {
			tblName := test.name
			t.Run(test.name, func(t *testing.T) {
				var mu sync.Mutex
				keysMap := make(map[int64]int64)

				workerNum := 10
				rowCount := 10000
				batch := rowCount / workerNum

				start := time.Now()

				var wg sync.WaitGroup
				wg.Add(workerNum)
				for i := 0; i < workerNum; i++ {
					go func(i int) {
						defer wg.Done()

						for j := 0; j < batch; j++ {
							k := batch*i + j
							s.execInsert(fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
							v := randomNum(rowCount)
							s.mustExec(fmt.Sprintf("update %s set c2 = %d where c1 = %d", tblName, v, k))
							mu.Lock()
							keysMap[int64(k)] = int64(v)
							mu.Unlock()
						}
					}(i)
				}
				wg.Wait()

				end := time.Now()
				fmt.Printf("[TestSimpleUpdate][Time Cost]%v\n", end.Sub(start))

				ctx := s.ctx
				err := sessiontxn.NewTxn(goctx.Background(), ctx)
				require.NoError(t, err)

				tbl := s.getTable(t, tblName)
				handles := kv.NewHandleMap()
				err = tables.IterRecords(tbl, ctx, tbl.Cols(), func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
					handles.Set(h, struct{}{})
					key := data[0].GetInt64()
					require.Equal(t, keysMap[key], data[1].GetValue())
					return true, nil
				})
				require.NoError(t, err)
				require.Equal(t, rowCount, handles.Len())
			})
		}
	})
	t.Run("Conflict", func(t *testing.T) {
		tests := []struct {
			name string
		}{
			{"test_conflict_update"},
			{"test_conflict_update_common"},
		}

		for _, test := range tests {
			tblName := test.name
			t.Run(test.name, func(t *testing.T) {
				var mu sync.Mutex
				keysMap := make(map[int64]int64)

				workerNum := 10
				rowCount := 10000
				batch := rowCount / workerNum

				start := time.Now()

				var wg sync.WaitGroup
				wg.Add(workerNum)
				for i := 0; i < workerNum; i++ {
					go func(i int) {
						defer wg.Done()

						for j := 0; j < batch; j++ {
							k := batch*i + j
							s.execInsert(fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
							mu.Lock()
							keysMap[int64(k)] = int64(k)
							mu.Unlock()
						}
					}(i)
				}
				wg.Wait()

				end := time.Now()
				fmt.Printf("[TestSimpleConflictUpdate][Insert][Time Cost]%v\n", end.Sub(start))

				start = time.Now()

				defaultValue := int64(-1)
				wg.Add(workerNum)
				for i := 0; i < workerNum; i++ {
					go func() {
						defer wg.Done()

						for j := 0; j < batch; j++ {
							k := randomNum(rowCount)
							s.mustExec(fmt.Sprintf("update %s set c2 = %d where c1 = %d", tblName, defaultValue, k))
							mu.Lock()
							keysMap[int64(k)] = defaultValue
							mu.Unlock()
						}
					}()
				}
				wg.Wait()

				end = time.Now()
				fmt.Printf("[TestSimpleConflictUpdate][Update][Time Cost]%v\n", end.Sub(start))

				ctx := s.ctx
				err := sessiontxn.NewTxn(goctx.Background(), ctx)
				require.NoError(t, err)

				tbl := s.getTable(t, tblName)
				handles := kv.NewHandleMap()
				err = tables.IterRecords(tbl, ctx, tbl.Cols(), func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
					handles.Set(h, struct{}{})
					require.Contains(t, keysMap, data[0].GetValue())

					if !reflect.DeepEqual(data[1].GetValue(), data[0].GetValue()) && !reflect.DeepEqual(data[1].GetValue(), defaultValue) {
						log.Fatal("[TestSimpleConflictUpdate fail]Bad row", zap.Any("row", data))
					}

					return true, nil
				})
				require.NoError(t, err)
				require.Equal(t, rowCount, handles.Len())
			})
		}
	})
}

func TestSimpleDelete(t *testing.T) {
	s := createDDLSuite(t)
	defer s.teardown(t)

	t.Run("Basic", func(t *testing.T) {
		tests := []struct {
			name string
		}{
			{"test_delete"},
			{"test_delete_common"},
		}

		for _, test := range tests {
			tblName := test.name
			t.Run(test.name, func(t *testing.T) {

				workerNum := 10
				rowCount := 1000
				batch := rowCount / workerNum

				start := time.Now()

				var wg sync.WaitGroup
				wg.Add(workerNum)
				for i := 0; i < workerNum; i++ {
					go func(i int) {
						defer wg.Done()

						for j := 0; j < batch; j++ {
							k := batch*i + j
							s.execInsert(fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
							s.mustExec(fmt.Sprintf("delete from %s where c1 = %d", tblName, k))
						}
					}(i)
				}
				wg.Wait()

				end := time.Now()
				fmt.Printf("[TestSimpleDelete][Time Cost]%v\n", end.Sub(start))

				ctx := s.ctx
				err := sessiontxn.NewTxn(goctx.Background(), ctx)
				require.NoError(t, err)

				tbl := s.getTable(t, tblName)
				handles := kv.NewHandleMap()
				err = tables.IterRecords(tbl, ctx, tbl.Cols(), func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
					handles.Set(h, struct{}{})
					return true, nil
				})
				require.NoError(t, err)
				require.Equal(t, 0, handles.Len())
			})
		}
	})
	t.Run("Conflict", func(t *testing.T) {
		tests := []struct {
			name string
		}{
			{"test_conflict_delete"},
			{"test_conflict_delete_common"},
		}

		for _, test := range tests {
			tblName := test.name
			t.Run(test.name, func(t *testing.T) {

				var mu sync.Mutex
				keysMap := make(map[int64]int64)

				workerNum := 10
				rowCount := 1000
				batch := rowCount / workerNum

				start := time.Now()

				var wg sync.WaitGroup
				wg.Add(workerNum)
				for i := 0; i < workerNum; i++ {
					go func(i int) {
						defer wg.Done()

						for j := 0; j < batch; j++ {
							k := batch*i + j
							s.execInsert(fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
							mu.Lock()
							keysMap[int64(k)] = int64(k)
							mu.Unlock()
						}
					}(i)
				}
				wg.Wait()

				end := time.Now()
				fmt.Printf("[TestSimpleConflictDelete][Insert][Time Cost]%v\n", end.Sub(start))

				start = time.Now()

				wg.Add(workerNum)
				for i := 0; i < workerNum; i++ {
					go func(i int) {
						defer wg.Done()

						for j := 0; j < batch; j++ {
							k := randomNum(rowCount)
							s.mustExec(fmt.Sprintf("delete from %s where c1 = %d", tblName, k))
							mu.Lock()
							delete(keysMap, int64(k))
							mu.Unlock()
						}
					}(i)
				}
				wg.Wait()

				end = time.Now()
				fmt.Printf("[TestSimpleConflictDelete][Delete][Time Cost]%v\n", end.Sub(start))

				ctx := s.ctx
				err := sessiontxn.NewTxn(goctx.Background(), ctx)
				require.NoError(t, err)

				tbl := s.getTable(t, tblName)
				handles := kv.NewHandleMap()
				err = tables.IterRecords(tbl, ctx, tbl.Cols(), func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
					handles.Set(h, struct{}{})
					require.Contains(t, keysMap, data[0].GetValue())
					return true, nil
				})
				require.NoError(t, err)
				require.Len(t, keysMap, handles.Len())
			})
		}
	})
}

// addEnvPath appends newPath to $PATH.
func addEnvPath(newPath string) {
	_ = os.Setenv("PATH", fmt.Sprintf("%s%c%s", os.Getenv("PATH"), os.PathListSeparator, newPath))
}

func init() {
	rand.Seed(time.Now().UnixNano())
	_ = store.Register("tikv", tidbdriver.TiKVDriver{})
}
