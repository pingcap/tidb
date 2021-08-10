package readonlytest

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
)

var (
	logLevel       = flag.String("L", "info", "test log level")
	serverLogLevel = flag.String("server_log_level", "info", "server log level")
	tmpPath        = flag.String("tmp", "/tmp/tidb_globalkilltest", "temporary files path")

	tidbBinaryPath = flag.String("s", "bin/globalkilltest_tidb-server", "tidb server binary path")
	pdBinaryPath   = flag.String("p", "bin/pd-server", "pd server binary path")
	tikvBinaryPath = flag.String("k", "bin/tikv-server", "tikv server binary path")

	tidbRootPassword = flag.String("passwd", "", "tidb root password")
	tidbStartPort    = flag.Int("tidb_start_port", 4000, "first tidb server listening port")
	tidbStatusPort   = flag.Int("tidb_status_port", 8000, "first tidb server status port")

	ReadOnlyErrMsg = "Error 1836: Running in read-only mode"
)

func TestReadOnly(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&TestReadOnlySuit{})

type TestReadOnlySuit struct {
	db  *sql.DB
	udb *sql.DB
	rdb *sql.DB
}

func checkVariable(c *C, db *sql.DB, on bool) {
	var name, status string
	rs, err := db.Query("show variables like 'tidb_restricted_read_only'")
	c.Assert(err, IsNil)
	c.Assert(rs.Next(), IsTrue)

	err = rs.Scan(&name, &status)
	c.Assert(err, IsNil)
	c.Assert(name, Equals, "tidb_restricted_read_only")
	if on {
		c.Assert(status, Equals, "ON")
	} else {
		c.Assert(status, Equals, "OFF")
	}
}

func setReadOnly(c *C, db *sql.DB, status int) {
	_, err := db.Exec(fmt.Sprintf("set global tidb_restricted_read_only=%d", status))
	c.Assert(err, IsNil)
}

func (s *TestReadOnlySuit) SetUpSuite(c *C) {
	var err error
	s.db, err = sql.Open("mysql", fmt.Sprintf("root:%s@(%s:%d)/test", *tidbRootPassword, "127.0.0.1", *tidbStartPort+1))
	c.Assert(err, IsNil)

	setReadOnly(c, s.db, 0)

	_, err = s.db.Exec("drop user if exists 'u1'@'%'")
	c.Assert(err, IsNil)
	_, err = s.db.Exec("create user 'u1'@'%' identified by 'password'")
	c.Assert(err, IsNil)
	_, err = s.db.Exec("grant all privileges on test.* to 'u1'@'%'")
	c.Assert(err, IsNil)
	s.udb, err = sql.Open("mysql", fmt.Sprintf("u1:password@(%s:%d)/test", "127.0.0.1", *tidbStartPort+2))
	c.Assert(err, IsNil)
	_, err = s.db.Exec("drop user if exists 'r1'@'%'")
	c.Assert(err, IsNil)
	_, err = s.db.Exec("create user 'r1'@'%' identified by 'password'")
	c.Assert(err, IsNil)
	_, err = s.db.Exec("grant all privileges on test.* to 'r1'@'%'")
	c.Assert(err, IsNil)
	_, err = s.db.Exec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'r1'@'%'")
	c.Assert(err, IsNil)
	s.rdb, err = sql.Open("mysql", fmt.Sprintf("r1:password@(%s:%d)/test", "127.0.0.1", *tidbStartPort+2))
}

func (s *TestReadOnlySuit) TestRestriction(c *C) {
	_, err := s.db.Exec("set global tidb_restricted_read_only=1")
	c.Assert(err, IsNil)
	time.Sleep(1)
	checkVariable(c, s.udb, true)

	_, err = s.udb.Exec("create table t(a int)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ReadOnlyErrMsg)
}

func (s *TestReadOnlySuit) TestRestrictionWithConnectionPool(c *C) {
	_, err := s.db.Exec("set global tidb_restricted_read_only=0")
	c.Assert(err, IsNil)
	_, err = s.db.Exec("drop table if exists t")
	c.Assert(err, IsNil)
	_, err = s.db.Exec("create table t (a int)")
	c.Assert(err, IsNil)
	time.Sleep(1)
	checkVariable(c, s.udb, false)

	conn, err := s.udb.Conn(context.Background())
	c.Assert(err, IsNil)
	done := make(chan bool)
	go func(conn *sql.Conn, done chan bool) {
		ticker := time.NewTicker(50 * time.Millisecond)
		for {
			t := <-ticker.C
			_, err := conn.ExecContext(context.Background(), fmt.Sprintf("insert into t values (%d)", t.Nanosecond()))
			if err == nil {
				continue
			}
			if err.Error() == ReadOnlyErrMsg {
				done <- true
			} else {
				done <- false
			}
		}
	}(conn, done)
	time.Sleep(1 * time.Second)

	timer := time.NewTimer(10 * time.Second)
	_, err = s.db.Exec("set global tidb_restricted_read_only=1")
	c.Assert(err, IsNil)
	select {
	case <-timer.C:
		c.Fail()
	case success := <-done:
		c.Assert(success, IsTrue)
	}
}

func (s *TestReadOnlySuit) TestReplicationWriter(c *C) {
	_, err := s.db.Exec("set global tidb_restricted_read_only=0")
	c.Assert(err, IsNil)
	_, err = s.db.Exec("drop table if exists t")
	c.Assert(err, IsNil)
	_, err = s.db.Exec("create table t (a int)")
	c.Assert(err, IsNil)
	time.Sleep(1)
	checkVariable(c, s.udb, false)

	conn, err := s.rdb.Conn(context.Background())
	c.Assert(err, IsNil)
	go func(conn *sql.Conn) {
		ticker := time.NewTicker(50 * time.Millisecond)
		for {
			t := <-ticker.C
			_, err := conn.ExecContext(context.Background(), fmt.Sprintf("insert into t values (%d)", t.Nanosecond()))
			c.Assert(err, IsNil)
		}
	}(conn)
	time.Sleep(1 * time.Second)
	timer := time.NewTimer(3 * time.Second)
	_, err = s.db.Exec("set global tidb_restricted_read_only=1")
	c.Assert(err, IsNil)
	// SUPER user can't write
	_, err = s.db.Exec("insert into t values (1)")
	c.Assert(err.Error(), Equals, ReadOnlyErrMsg)
	<-timer.C
}
