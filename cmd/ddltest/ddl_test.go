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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddltest

import (
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

func TestDDL(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

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

var _ = Suite(&TestDDLSuite{})

type server struct {
	*exec.Cmd
	logFP *os.File
	db    *sql.DB
	addr  string
}

type TestDDLSuite struct {
	store kv.Storage
	dom   *domain.Domain
	s     session.Session
	ctx   sessionctx.Context

	m     sync.Mutex
	procs []*server

	wg   sync.WaitGroup
	quit chan struct{}

	retryCount int

	testutil.CommonHandleSuite
}

func (s *TestDDLSuite) SetUpSuite(c *C) {
	logutil.InitLogger(&logutil.LogConfig{Config: zaplog.Config{Level: *logLevel}})

	s.quit = make(chan struct{})

	var err error
	s.store, err = store.New(fmt.Sprintf("tikv://%s%s", *etcd, *tikvPath))
	c.Assert(err, IsNil)

	// Make sure the schema lease of this session is equal to other TiDB servers'.
	session.SetSchemaLease(time.Duration(*lease) * time.Second)

	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)

	s.s, err = session.CreateSession(s.store)
	c.Assert(err, IsNil)

	s.ctx = s.s.(sessionctx.Context)
	goCtx := goctx.Background()
	_, err = s.s.Execute(goCtx, "create database if not exists test_ddl")
	c.Assert(err, IsNil)

	s.Bootstrap(c)

	// Stop current DDL worker, so that we can't be the owner now.
	err = domain.GetDomain(s.ctx).DDL().Stop()
	c.Assert(err, IsNil)
	ddl.RunWorker = false
	session.ResetStoreForWithTiKVTest(s.store)
	s.s, err = session.CreateSession(s.store)
	c.Assert(err, IsNil)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.ctx = s.s.(sessionctx.Context)
	_, err = s.s.Execute(goCtx, "use test_ddl")
	c.Assert(err, IsNil)

	addEnvPath("..")

	// Start multi tidb servers
	s.procs = make([]*server, *serverNum)

	// Set server restart retry count.
	s.retryCount = 5

	createLogFiles(c, *serverNum)
	err = s.startServers()
	c.Assert(err, IsNil)

	s.wg.Add(1)
	go s.restartServerRegularly()
}

// restartServerRegularly restarts a tidb server regularly.
func (s *TestDDLSuite) restartServerRegularly() {
	defer s.wg.Done()

	var err error
	after := *lease * (6 + randomIntn(6))
	for {
		select {
		case <-time.After(time.Duration(after) * time.Second):
			if *enableRestart {
				err = s.restartServerRand()
				if err != nil {
					log.Fatalf("restartServerRand failed, err %v", errors.ErrorStack(err))
				}
			}
		case <-s.quit:
			return
		}
	}
}

func (s *TestDDLSuite) TearDownSuite(c *C) {
	close(s.quit)
	s.wg.Wait()

	s.dom.Close()
	// TODO: Remove these logs after testing.
	quitCh := make(chan struct{})
	go func() {
		select {
		case <-time.After(100 * time.Second):
			buf := make([]byte, 2<<20)
			size := runtime.Stack(buf, true)
			log.Errorf("%s", buf[:size])
		case <-quitCh:
		}
	}()
	err := s.store.Close()
	c.Assert(err, IsNil)
	close(quitCh)

	err = s.stopServers()
	c.Assert(err, IsNil)
}

func (s *TestDDLSuite) startServers() (err error) {
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

func (s *TestDDLSuite) killServer(proc *os.Process) error {
	// Make sure this tidb is killed, and it makes the next tidb that has the same port as this one start quickly.
	err := proc.Kill()
	if err != nil {
		log.Errorf("kill server failed err %v", err)
		return errors.Trace(err)
	}
	_, err = proc.Wait()
	if err != nil {
		log.Errorf("kill server, wait failed err %v", err)
		return errors.Trace(err)
	}

	time.Sleep(1 * time.Second)
	return nil
}

func (s *TestDDLSuite) stopServers() error {
	s.m.Lock()
	defer s.m.Unlock()

	for i := 0; i < len(s.procs); i++ {
		if s.procs[i] != nil {
			err := s.killServer(s.procs[i].Process)
			if err != nil {
				return errors.Trace(err)
			}
			s.procs[i] = nil
		}
	}
	return nil
}

var logFilePrefix = "tidb_log_file_"

func createLogFiles(c *C, length int) {
	for i := 0; i < length; i++ {
		fp, err := os.Create(fmt.Sprintf("%s%d", logFilePrefix, i))
		if err != nil {
			c.Assert(err, IsNil)
		}
		fp.Close()
	}
}

func (s *TestDDLSuite) startServer(i int, fp *os.File) (*server, error) {
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
		log.Errorf("restart server addr %v failed %v, take time %v", addr, err, time.Since(startTime))
		return nil, errors.Trace(err)
	}
	db.SetMaxOpenConns(10)

	_, err = db.Exec("use test_ddl")
	if err != nil {
		return nil, errors.Trace(err)
	}

	log.Infof("start server %s ok %v", addr, err)

	return &server{
		Cmd:   cmd,
		db:    db,
		addr:  addr,
		logFP: fp,
	}, nil
}

func (s *TestDDLSuite) restartServerRand() error {
	i := rand.Intn(*serverNum)

	s.m.Lock()
	defer s.m.Unlock()

	if s.procs[i] == nil {
		return nil
	}

	server := s.procs[i]
	s.procs[i] = nil
	log.Warnf("begin to restart %s", server.addr)
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
		log.Warnf("err is %v", err)
		return false
	}

	log.Errorf("err is %v, can not retry", err)

	return false
}

func (s *TestDDLSuite) exec(query string, args ...interface{}) (sql.Result, error) {
	for {
		server := s.getServer()
		r, err := server.db.Exec(query, args...)
		if isRetryError(err) {
			log.Errorf("exec %s in server %s err %v, retry", query, err, server.addr)
			continue
		}

		return r, err
	}
}

func (s *TestDDLSuite) mustExec(c *C, query string, args ...interface{}) sql.Result {
	r, err := s.exec(query, args...)
	if err != nil {
		log.Fatalf("[mustExec fail]query - %v %v, error - %v", query, args, err)
	}

	return r
}

func (s *TestDDLSuite) execInsert(c *C, query string, args ...interface{}) sql.Result {
	for {
		r, err := s.exec(query, args...)
		if err == nil {
			return r
		}

		if *enableRestart {
			// If use enable random restart servers, we should ignore key exists error.
			if strings.Contains(err.Error(), "Duplicate entry") &&
				strings.Contains(err.Error(), "for key") {
				return r
			}
		}

		log.Fatalf("[execInsert fail]query - %v %v, error - %v", query, args, err)
	}
}

func (s *TestDDLSuite) query(query string, args ...interface{}) (*sql.Rows, error) {
	for {
		server := s.getServer()
		r, err := server.db.Query(query, args...)
		if isRetryError(err) {
			log.Errorf("query %s in server %s err %v, retry", query, err, server.addr)
			continue
		}

		return r, err
	}
}

func (s *TestDDLSuite) getServer() *server {
	s.m.Lock()
	defer s.m.Unlock()

	for i := 0; i < 20; i++ {
		i := rand.Intn(*serverNum)

		if s.procs[i] != nil {
			return s.procs[i]
		}
	}

	log.Fatalf("try to get server too many times")
	return nil
}

// runDDL executes the DDL query, returns a channel so that you can use it to wait DDL finished.
func (s *TestDDLSuite) runDDL(sql string) chan error {
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

func (s *TestDDLSuite) getTable(c *C, name string) table.Table {
	tbl, err := domain.GetDomain(s.ctx).InfoSchema().TableByName(model.NewCIStr("test_ddl"), model.NewCIStr(name))
	c.Assert(err, IsNil)
	return tbl
}

func dumpRows(c *C, rows *sql.Rows) [][]interface{} {
	cols, err := rows.Columns()
	c.Assert(err, IsNil)
	var ay [][]interface{}
	for rows.Next() {
		v := make([]interface{}, len(cols))
		for i := range v {
			v[i] = new(interface{})
		}
		err = rows.Scan(v...)
		c.Assert(err, IsNil)

		for i := range v {
			v[i] = *(v[i].(*interface{}))
		}
		ay = append(ay, v)
	}

	rows.Close()
	c.Assert(rows.Err(), IsNil, Commentf("%v", ay))
	return ay
}

func matchRows(c *C, rows *sql.Rows, expected [][]interface{}) {
	ay := dumpRows(c, rows)
	c.Assert(len(ay), Equals, len(expected), Commentf("%v", expected))
	for i := range ay {
		match(c, ay[i], expected[i]...)
	}
}

func match(c *C, row []interface{}, expected ...interface{}) {
	c.Assert(len(row), Equals, len(expected))
	for i := range row {
		if row[i] == nil {
			c.Assert(expected[i], IsNil)
			continue
		}

		got, err := types.ToString(row[i])
		c.Assert(err, IsNil)

		need, err := types.ToString(expected[i])
		c.Assert(err, IsNil)
		c.Assert(got, Equals, need)
	}
}

func (s *TestDDLSuite) Bootstrap(c *C) {
	tk := testkit.NewTestKit(c, s.store)
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

	tk.MustExec("set @@tidb_enable_clustered_index = 1")
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
	tk.MustExec("set @@tidb_enable_clustered_index = 0")
}

func (s *TestDDLSuite) TestSimple(c *C) {
	done := s.runDDL("create table if not exists test_simple (c1 int, c2 int, c3 int)")
	err := <-done
	c.Assert(err, IsNil)

	_, err = s.exec("insert into test_simple values (1, 1, 1)")
	c.Assert(err, IsNil)

	rows, err := s.query("select c1 from test_simple limit 1")
	c.Assert(err, IsNil)
	matchRows(c, rows, [][]interface{}{{1}})

	done = s.runDDL("drop table if exists test_simple")
	err = <-done
	c.Assert(err, IsNil)
}

func (s *TestDDLSuite) TestSimpleInsert(c *C) {
	tblName := "test_insert"
	if s.IsCommonHandle {
		tblName = "test_insert_common"
	}

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
				s.execInsert(c, fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
			}
		}(i)
	}
	wg.Wait()

	end := time.Now()
	fmt.Printf("[TestSimpleInsert][Time Cost]%v\n", end.Sub(start))

	ctx := s.ctx
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)

	tbl := s.getTable(c, "test_insert")
	handles := kv.NewHandleMap()
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.Cols(), func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		handles.Set(h, struct{}{})
		c.Assert(data[0].GetValue(), Equals, data[1].GetValue())
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(handles.Len(), Equals, rowCount, Commentf("%d %d", handles.Len(), rowCount))
	s.RerunWithCommonHandleEnabled(c, s.TestSimpleInsert)
}

func (s *TestDDLSuite) TestSimpleConflictInsert(c *C) {
	tblName := "test_conflict_insert"
	if s.IsCommonHandle {
		tblName = "test_conflict_insert_common"
	}

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
				s.exec(fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
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
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)

	tbl := s.getTable(c, tblName)
	handles := kv.NewHandleMap()
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.Cols(), func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		handles.Set(h, struct{}{})
		c.Assert(keysMap, HasKey, data[0].GetValue())
		c.Assert(data[0].GetValue(), Equals, data[1].GetValue())
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(handles.Len(), Equals, len(keysMap))
	s.RerunWithCommonHandleEnabled(c, s.TestSimpleConflictInsert)
}

func (s *TestDDLSuite) TestSimpleUpdate(c *C) {
	tblName := "test_update"
	if s.IsCommonHandle {
		tblName = "test_update_common"
	}
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
				s.execInsert(c, fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
				v := randomNum(rowCount)
				s.mustExec(c, fmt.Sprintf("update %s set c2 = %d where c1 = %d", tblName, v, k))
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
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)

	tbl := s.getTable(c, tblName)
	handles := kv.NewHandleMap()
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.Cols(), func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		handles.Set(h, struct{}{})
		key := data[0].GetInt64()
		c.Assert(data[1].GetValue(), Equals, keysMap[key])
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(handles.Len(), Equals, rowCount)
	s.RerunWithCommonHandleEnabled(c, s.TestSimpleUpdate)
}

func (s *TestDDLSuite) TestSimpleConflictUpdate(c *C) {
	tblName := "test_conflict_update"
	if s.IsCommonHandle {
		tblName = "test_conflict_update_common"
	}
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
				s.execInsert(c, fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
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
				s.mustExec(c, fmt.Sprintf("update %s set c2 = %d where c1 = %d", tblName, defaultValue, k))
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
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)

	tbl := s.getTable(c, tblName)
	handles := kv.NewHandleMap()
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.Cols(), func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		handles.Set(h, struct{}{})
		c.Assert(keysMap, HasKey, data[0].GetValue())

		if !reflect.DeepEqual(data[1].GetValue(), data[0].GetValue()) && !reflect.DeepEqual(data[1].GetValue(), defaultValue) {
			log.Fatalf("[TestSimpleConflictUpdate fail]Bad row: %v", data)
		}

		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(handles.Len(), Equals, rowCount)
	s.RerunWithCommonHandleEnabled(c, s.TestSimpleConflictUpdate)
}

func (s *TestDDLSuite) TestSimpleDelete(c *C) {
	tblName := "test_delete"
	if s.IsCommonHandle {
		tblName = "test_delete_common"
	}
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
				s.execInsert(c, fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
				s.mustExec(c, fmt.Sprintf("delete from %s where c1 = %d", tblName, k))
			}
		}(i)
	}
	wg.Wait()

	end := time.Now()
	fmt.Printf("[TestSimpleDelete][Time Cost]%v\n", end.Sub(start))

	ctx := s.ctx
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)

	tbl := s.getTable(c, tblName)
	handles := kv.NewHandleMap()
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.Cols(), func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		handles.Set(h, struct{}{})
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(handles.Len(), Equals, 0)
	s.RerunWithCommonHandleEnabled(c, s.TestSimpleDelete)
}

func (s *TestDDLSuite) TestSimpleConflictDelete(c *C) {
	tblName := "test_conflict_delete"
	if s.IsCommonHandle {
		tblName = "test_conflict_delete_common"
	}
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
				s.execInsert(c, fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
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
				s.mustExec(c, fmt.Sprintf("delete from %s where c1 = %d", tblName, k))
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
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)

	tbl := s.getTable(c, tblName)
	handles := kv.NewHandleMap()
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.Cols(), func(h kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		handles.Set(h, struct{}{})
		c.Assert(keysMap, HasKey, data[0].GetValue())
		return true, nil
	})
	c.Assert(err, IsNil)
	c.Assert(handles.Len(), Equals, len(keysMap))
	s.RerunWithCommonHandleEnabled(c, s.TestSimpleConflictDelete)
}

func (s *TestDDLSuite) TestSimpleMixed(c *C) {
	tblName := "test_mixed"
	if s.IsCommonHandle {
		tblName = "test_mixed_common"
	}
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
				s.execInsert(c, fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
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
				s.execInsert(c, fmt.Sprintf("insert into %s values (%d, %d)", tblName, key, key))
				key = int64(randomNum(rowCount))
				s.mustExec(c, fmt.Sprintf("update %s set c2 = %d where c1 = %d", tblName, defaultValue, key))
				key = int64(randomNum(rowCount))
				s.mustExec(c, fmt.Sprintf("delete from %s where c1 = %d", tblName, key))
			}
		}()
	}
	wg.Wait()

	end = time.Now()
	fmt.Printf("[TestSimpleMixed][Mixed][Time Cost]%v\n", end.Sub(start))

	ctx := s.ctx
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)

	tbl := s.getTable(c, tblName)
	updateCount := int64(0)
	insertCount := int64(0)
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		if reflect.DeepEqual(data[1].GetValue(), data[0].GetValue()) {
			insertCount++
		} else if reflect.DeepEqual(data[1].GetValue(), defaultValue) && data[0].GetInt64() < int64(rowCount) {
			updateCount++
		} else {
			log.Fatalf("[TestSimpleMixed fail]invalid row: %v", data)
		}

		return true, nil
	})
	c.Assert(err, IsNil)

	deleteCount := atomic.LoadInt64(&rowID) - insertCount - updateCount
	c.Assert(insertCount, Greater, int64(0))
	c.Assert(updateCount, Greater, int64(0))
	c.Assert(deleteCount, Greater, int64(0))
	s.RerunWithCommonHandleEnabled(c, s.TestSimpleMixed)
}

func (s *TestDDLSuite) TestSimpleInc(c *C) {
	tblName := "test_inc"
	if s.IsCommonHandle {
		tblName = "test_inc_common"
	}
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
				s.execInsert(c, fmt.Sprintf("insert into %s values (%d, %d)", tblName, k, k))
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
				s.mustExec(c, fmt.Sprintf("update %s set c2 = c2 + 1 where c1 = 0", tblName))
			}
		}()
	}
	wg.Wait()

	end = time.Now()
	fmt.Printf("[TestSimpleInc][Update][Time Cost]%v\n", end.Sub(start))

	ctx := s.ctx
	err := ctx.NewTxn(goctx.Background())
	c.Assert(err, IsNil)

	tbl := s.getTable(c, "test_inc")
	err = tbl.IterRecords(ctx, tbl.FirstKey(), tbl.Cols(), func(_ kv.Handle, data []types.Datum, cols []*table.Column) (bool, error) {
		if reflect.DeepEqual(data[0].GetValue(), int64(0)) {
			if *enableRestart {
				c.Assert(data[1].GetValue(), GreaterEqual, int64(rowCount))
			} else {
				c.Assert(data[1].GetValue(), Equals, int64(rowCount))
			}
		} else {
			c.Assert(data[0].GetValue(), Equals, data[1].GetValue())
		}

		return true, nil
	})
	c.Assert(err, IsNil)
	s.RerunWithCommonHandleEnabled(c, s.TestSimpleInc)
}

// addEnvPath appends newPath to $PATH.
func addEnvPath(newPath string) {
	os.Setenv("PATH", fmt.Sprintf("%s%c%s", os.Getenv("PATH"), os.PathListSeparator, newPath))
}

func init() {
	rand.Seed(time.Now().UnixNano())
	store.Register("tikv", tikv.Driver{})
}
