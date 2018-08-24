// Copyright 2018 PingCAP, Inc.
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

package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strings"

	"flag"
	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	log "github.com/sirupsen/logrus"
	"time"
)

const dbName = "test"

var (
	logLevel string
	record   bool
	create   bool
)

func init() {
	flag.StringVar(&logLevel, "log-level", "error", "set log level: info, warn, error, debug [default: error]")
	flag.BoolVar(&record, "record", false, "record the test output in the result file")
	flag.BoolVar(&create, "create", false, "create and import data into table, and save json file of stats")
}

var mdb *sql.DB

type query struct {
	Query string
	Line  int
}

type tester struct {
	name string

	tx *sql.Tx

	buf bytes.Buffer

	// enable query log will output origin statement into result file too
	// use --disable_query_log or --enable_query_log to control it
	enableQueryLog bool

	singleQuery bool

	// check expected error, use --error before the statement
	// see http://dev.mysql.com/doc/mysqltest/2.0/en/writing-tests-expecting-errors.html
	expectedErrs []string

	// only for test, not record, every time we execute a statement, we should read the result
	// data to check correction.
	resultFD *os.File
	// ctx is used for Compile sql statement
	ctx sessionctx.Context
}

func newTester(name string) *tester {
	t := new(tester)

	t.name = name
	t.enableQueryLog = true
	t.ctx = mock.NewContext()

	return t
}

func (t *tester) Run() error {
	queries, err := t.loadQueries()
	if err != nil {
		return errors.Trace(err)
	}

	if err = t.openResult(); err != nil {
		return errors.Trace(err)
	}

	var s string
	defer func() {
		if t.tx != nil {
			log.Errorf("transaction is not committed correctly, rollback")
			err = t.rollback()
			if err != nil {
				log.Errorf("transaction is rollback err %v", err)
			}
		}

		if t.resultFD != nil {
			err = t.resultFD.Close()
			if err != nil {
				log.Errorf("result fd close error %v", err)
			}
		}
	}()

LOOP:
	for _, q := range queries {
		s = q.Query
		if strings.HasPrefix(s, "--") {
			// clear expected errors
			t.expectedErrs = nil

			switch s {
			case "--enable_query_log":
				t.enableQueryLog = true
			case "--disable_query_log":
				t.enableQueryLog = false
			case "--single_query":
				t.singleQuery = true
			case "--halt":
				// if we meet halt, we will ignore following tests
				break LOOP
			default:
				if strings.HasPrefix(s, "--error") {
					t.expectedErrs = strings.Split(strings.TrimSpace(strings.TrimLeft(s, "--error")), ",")
				} else if strings.HasPrefix(s, "-- error") {
					t.expectedErrs = strings.Split(strings.TrimSpace(strings.TrimLeft(s, "-- error")), ",")
				} else if strings.HasPrefix(s, "--echo") {
					echo := strings.TrimSpace(strings.TrimLeft(s, "--echo"))
					t.buf.WriteString(echo)
					t.buf.WriteString("\n")
				}
			}
		} else {
			if err = t.execute(q); err != nil {
				return errors.Annotate(err, fmt.Sprintf("sql:%v", q.Query))
			}
		}
	}

	return t.flushResult()
}

func (t *tester) loadQueries() ([]query, error) {
	data, err := ioutil.ReadFile(t.testFileName())
	if err != nil {
		return nil, err
	}

	seps := bytes.Split(data, []byte("\n"))
	queries := make([]query, 0, len(seps))
	newStmt := true
	for i, v := range seps {
		s := string(bytes.TrimSpace(v))
		// we will skip # comment here
		if strings.HasPrefix(s, "#") {
			newStmt = true
			continue
		} else if strings.HasPrefix(s, "--") {
			queries = append(queries, query{Query: s, Line: i + 1})
			newStmt = true
			continue
		} else if len(s) == 0 {
			continue
		}

		if newStmt {
			queries = append(queries, query{Query: s, Line: i + 1})
		} else {
			lastQuery := queries[len(queries)-1]
			lastQuery.Query = fmt.Sprintf("%s\n%s", lastQuery.Query, s)
			queries[len(queries)-1] = lastQuery
		}

		// if the line has a ; in the end, we will treat new line as the new statement.
		newStmt = strings.HasSuffix(s, ";")
	}
	return queries, nil
}

// parserErrorHandle handle mysql_test syntax `--error ER_PARSE_ERROR`, to allow following query
// return parser error.
func (t *tester) parserErrorHandle(query query, err error) error {
	offset := t.buf.Len()
	for _, expectedErr := range t.expectedErrs {
		if expectedErr == "ER_PARSE_ERROR" {
			if t.enableQueryLog {
				t.buf.WriteString(query.Query)
				t.buf.WriteString("\n")
			}

			t.buf.WriteString(fmt.Sprintf("%s\n", err))
			err = nil
			break
		}
	}

	if err != nil {
		return errors.Trace(err)
	}

	// clear expected errors after we execute the first query
	t.expectedErrs = nil
	t.singleQuery = false

	if !record && !create {
		// check test result now
		gotBuf := t.buf.Bytes()[offset:]
		buf := make([]byte, t.buf.Len()-offset)
		if _, err = t.resultFD.ReadAt(buf, int64(offset)); err != nil {
			return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we got \n%s\nbut read result err %s", query.Query, query.Line, gotBuf, err))
		}

		if !bytes.Equal(gotBuf, buf) {
			return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we need(%v):\n%s\nbut got(%v):\n%s\n", query.Query, query.Line, len(buf), buf, len(gotBuf), gotBuf))
		}
	}

	return errors.Trace(err)
}

func (t *tester) executeDefault(qText string) (err error) {
	if t.tx != nil {
		return filterWarning(t.executeStmt(qText))
	}

	// if begin or following commit fails, we don't think
	// this error is the expected one.
	if t.tx, err = mdb.Begin(); err != nil {
		err2 := t.rollback()
		if err2 != nil {
			log.Errorf("transaction is rollback err %v", err)
		}
		return err
	}

	if err = filterWarning(t.executeStmt(qText)); err != nil {
		err2 := t.rollback()
		if err2 != nil {
			log.Errorf("transaction is rollback err %v", err)
		}
		return err
	}

	if err = t.commit(); err != nil {
		err2 := t.rollback()
		if err2 != nil {
			log.Errorf("transaction is rollback err %v", err)
		}
		return err
	}
	return nil
}

func (t *tester) execute(query query) error {
	if len(query.Query) == 0 {
		return nil
	}

	list, err := session.Parse(t.ctx, query.Query)
	if err != nil {
		return t.parserErrorHandle(query, err)
	}
	for _, st := range list {
		var qText string
		if t.singleQuery {
			qText = query.Query
		} else {
			qText = st.Text()
		}
		offset := t.buf.Len()
		if t.enableQueryLog {
			t.buf.WriteString(qText)
			t.buf.WriteString("\n")
		}
		switch st.(type) {
		case *ast.BeginStmt:
			t.tx, err = mdb.Begin()
			if err != nil {
				err2 := t.rollback()
				if err2 != nil {
					log.Errorf("transaction is rollback err %v", err)
				}
				break
			}
		case *ast.CommitStmt:
			err = t.commit()
			if err != nil {
				err2 := t.rollback()
				if err2 != nil {
					log.Errorf("transaction is rollback err %v", err)
				}
				break
			}
		case *ast.RollbackStmt:
			err = t.rollback()
			if err != nil {
				break
			}
		default:
			if create {
				createStmt, isCreate := st.(*ast.CreateTableStmt)
				if isCreate {
					if err = t.create(createStmt.Table.Name.String(), qText); err != nil {
						break
					}
				} else {
					_, isDrop := st.(*ast.DropTableStmt)
					_, isAnalyze := st.(*ast.AnalyzeTableStmt)
					if isDrop || isAnalyze {
						if err = t.executeDefault(qText); err != nil {
							break
						}
					}
				}
			} else if err = t.executeDefault(qText); err != nil {
				break
			}
		}

		if err != nil && len(t.expectedErrs) > 0 {
			// TODO: check whether this err is expected.
			// but now we think it is.

			// output expected err
			t.buf.WriteString(fmt.Sprintf("%s\n", err))
			err = nil
		}
		// clear expected errors after we execute the first query
		t.expectedErrs = nil
		t.singleQuery = false

		if err != nil {
			return errors.Trace(errors.Errorf("run \"%v\" at line %d err %v", st.Text(), query.Line, err))
		}

		if !record && !create {
			// check test result now
			gotBuf := t.buf.Bytes()[offset:]

			buf := make([]byte, t.buf.Len()-offset)
			if _, err = t.resultFD.ReadAt(buf, int64(offset)); err != nil {
				return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we got \n%s\nbut read result err %s", st.Text(), query.Line, gotBuf, err))
			}

			if !bytes.Equal(gotBuf, buf) {
				return errors.Trace(errors.Errorf("run \"%v\" at line %d err, we need:\n%s\nbut got:\n%s\n", query.Query, query.Line, buf, gotBuf))
			}
		}
	}
	return errors.Trace(err)
}

func filterWarning(err error) error {
	causeErr := errors.Cause(err)
	if _, ok := causeErr.(mysql.MySQLWarnings); ok {
		return nil
	}
	return err
}

func (t *tester) create(tableName string, qText string) error {
	fmt.Printf("import data for table %s of test %s:\n", tableName, t.name)

	path := "./importer -t \"" + qText + "\" -P 4001 -n 2000 -c 100"
	cmd := exec.Command("sh", "-c", path)
	stdoutIn, err := cmd.StdoutPipe()
	if err != nil {
		log.Errorf("open stdout pipe failure %v", err)
	}
	stderrIn, err := cmd.StderrPipe()
	if err != nil {
		log.Errorf("open stderr pipe failure %v", err)
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	var errStdout, errStderr error
	stdout := io.MultiWriter(os.Stdout, &stdoutBuf)
	stderr := io.MultiWriter(os.Stderr, &stderrBuf)

	if err = cmd.Start(); err != nil {
		return errors.Trace(err)
	}

	go func() {
		_, errStdout = io.Copy(stdout, stdoutIn)
	}()
	go func() {
		_, errStderr = io.Copy(stderr, stderrIn)
	}()

	if err = cmd.Wait(); err != nil {
		log.Fatalf("importer failed with: %s", err)
		return err
	}

	if errStdout != nil {
		return errors.Trace(errStdout)
	}

	if errStderr != nil {
		return errors.Trace(errStderr)
	}

	if err = t.analyze(tableName); err != nil {
		return err
	}

	resp, err := http.Get("http://127.0.0.1:10081/stats/dump/" + dbName + "/" + tableName)
	if err != nil {
		return err
	}

	js, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(t.statsFileName(tableName), js, 0644)
}

func (t *tester) commit() error {
	err := t.tx.Commit()
	if err != nil {
		return err
	}
	t.tx = nil
	return nil
}

func (t *tester) rollback() error {
	if t.tx == nil {
		return nil
	}
	err := t.tx.Rollback()
	t.tx = nil
	return err
}

func (t *tester) analyze(tableName string) error {
	return t.execute(query{Query: "analyze table " + tableName + ";", Line: 0})
}

func (t *tester) executeStmt(query string) error {
	if session.IsQuery(query) {
		rows, err := t.tx.Query(query)
		if err != nil {
			return errors.Trace(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			return errors.Trace(err)
		}

		for i, c := range cols {
			t.buf.WriteString(c)
			if i != len(cols)-1 {
				t.buf.WriteString("\t")
			}
		}
		t.buf.WriteString("\n")

		values := make([][]byte, len(cols))
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				return errors.Trace(err)
			}

			var value string
			for i, col := range values {
				// Here we can check if the value is nil (NULL value)
				if col == nil {
					value = "NULL"
				} else {
					value = string(col)
				}
				t.buf.WriteString(value)
				if i < len(values)-1 {
					t.buf.WriteString("\t")
				}
			}
			t.buf.WriteString("\n")
		}
		err = rows.Err()
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		// TODO: rows affected and last insert id
		_, err := t.tx.Exec(query)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *tester) openResult() error {
	if record || create {
		return nil
	}

	var err error
	t.resultFD, err = os.Open(t.resultFileName())
	return err
}

func (t *tester) flushResult() error {
	if !record {
		return nil
	}
	return ioutil.WriteFile(t.resultFileName(), t.buf.Bytes(), 0644)
}

func (t *tester) statsFileName(tableName string) string {
	return fmt.Sprintf("./s/%s_%s.json", t.name, tableName)
}

func (t *tester) testFileName() string {
	// test and result must be in current ./t the same as MySQL
	return fmt.Sprintf("./t/%s.test", t.name)
}

func (t *tester) resultFileName() string {
	// test and result must be in current ./r, the same as MySQL
	return fmt.Sprintf("./r/%s.result", t.name)
}

func loadAllTests() ([]string, error) {
	// tests must be in t folder
	files, err := ioutil.ReadDir("./t")
	if err != nil {
		return nil, err
	}

	tests := make([]string, 0, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		// the test file must have a suffix .test
		name := f.Name()
		if strings.HasSuffix(name, ".test") {
			name = strings.TrimSuffix(name, ".test")

			// if we use record and the result file exists, skip generating
			if record && resultExists(name) {
				continue
			}

			if create && !strings.HasSuffix(name, "_stats") {
				continue
			}

			tests = append(tests, name)
		}
	}

	return tests, nil
}

func resultExists(name string) bool {
	resultFile := fmt.Sprintf("./r/%s.result", name)

	if _, err := os.Stat(resultFile); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// openDBWithRetry opens a database specified by its database driver name and a
// driver-specific data source name. And it will do some retries if the connection fails.
func openDBWithRetry(driverName, dataSourceName string) (mdb *sql.DB, err error) {
	startTime := time.Now()
	sleepTime := time.Millisecond * 500
	retryCnt := 60
	// The max retry interval is 30 s.
	for i := 0; i < retryCnt; i++ {
		mdb, err = sql.Open(driverName, dataSourceName)
		if err != nil {
			log.Warnf("open db failed, retry count %d err %v", i, err)
			time.Sleep(sleepTime)
			continue
		}
		err = mdb.Ping()
		if err == nil {
			break
		}
		log.Warnf("ping db failed, retry count %d err %v", i, err)
		err = mdb.Close()
		if err != nil {
			log.Errorf("close db err %v", err)
		}
		time.Sleep(sleepTime)
	}
	if err != nil {
		log.Errorf("open db failed %v, take time %v", err, time.Since(startTime))
		return nil, errors.Trace(err)
	}

	return
}

func main() {
	flag.Parse()

	err := logutil.InitLogger(&logutil.LogConfig{
		Level: logLevel,
	})
	if err != nil {
		panic("init logger fail, " + err.Error())
	}

	mdb, err = openDBWithRetry(
		"mysql",
		"root@tcp(localhost:4001)/"+dbName+"?strict=true&allowAllFiles=true",
	)
	if err != nil {
		log.Fatalf("Open db err %v", err)
	}

	defer func() {
		log.Warn("close db")
		err = mdb.Close()
		if err != nil {
			log.Errorf("close db err %v", err)
		}
	}()

	log.Warn("Create new db", mdb)

	if _, err = mdb.Exec("DROP DATABASE IF EXISTS test"); err != nil {
		log.Fatalf("Executing drop db test err[%v]", err)
	}
	if _, err = mdb.Exec("CREATE DATABASE test"); err != nil {
		log.Fatalf("Executing create db test err[%v]", err)
	}
	if _, err = mdb.Exec("USE test"); err != nil {
		log.Fatalf("Executing Use test err[%v]", err)
	}
	if _, err = mdb.Exec("set @@tidb_hash_join_concurrency=1"); err != nil {
		log.Fatalf("set @@tidb_hash_join_concurrency=1 err[%v]", err)
	}

	tests := flag.Args()

	// we will run all tests if no tests assigned
	if len(tests) == 0 {
		if tests, err = loadAllTests(); err != nil {
			log.Fatalf("load all tests err %v", err)
		}
	}

	if record {
		log.Printf("recording tests: %v", tests)
	} else if create {
		log.Printf("creating data for tests: %v", tests)
	} else {
		log.Printf("running tests: %v", tests)
	}

	for _, t := range tests {
		if strings.Contains(t, "--log-level") {
			continue
		}
		tr := newTester(t)
		if err = tr.Run(); err != nil {
			log.Fatalf("run test [%s] err: %v", t, err)
		}
		log.Infof("run test [%s] ok", t)
	}

	println("\nGreat, All tests passed")
}
