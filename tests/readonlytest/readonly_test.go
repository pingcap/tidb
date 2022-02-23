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
	tidbRootPassword       = flag.String("passwd", "", "tidb root password")
	tidbAPort              = flag.Int("tidb_a_port", 4001, "first tidb server listening port")
	tidbBPort              = flag.Int("tidb_b_port", 4002, "second tidb server listening port")
	ReadOnlyErrMsg         = "Error 1836: Running in read-only mode"
	ConflictErrMsg         = "Error 1105: can't turn off tidb_super_read_only when tidb_restricted_read_only is on"
	PriviledgedErrMsg      = "Error 1227: Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation"
	TiDBRestrictedReadOnly = "tidb_restricted_read_only"
	TiDBSuperReadOnly      = "tidb_super_read_only"
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

func TestRestriction(t *testing.T) {
	s, clean := createReadOnlySuite(t)
	defer clean()

	var err error
	_, err = s.db.Exec("drop table if exists t")
	require.NoError(t, err)
	_, err = s.udb.Exec("create table t (a int primary key, b int)")
	require.NoError(t, err)
	_, err = s.udb.Exec("insert into t values (1, 1)")
	require.NoError(t, err)
	_, err = s.udb.Exec("update t set b = 2 where a = 1")
	require.NoError(t, err)

	setVariable(t, s.db, TiDBRestrictedReadOnly, 1)
	time.Sleep(1)
	checkVariable(c, s.udb, true)

	checkVariable(t, s.udb, TiDBRestrictedReadOnly, true)
	checkVariable(t, s.udb, TiDBSuperReadOnly, true)

	checkVariable(t, s.rdb, TiDBRestrictedReadOnly, true)
	checkVariable(t, s.rdb, TiDBSuperReadOnly, true)

	// can't create table
	_, err = s.udb.Exec("create table t(a int)")
	require.Error(t, err)
	require.Equal(t, err.Error(), ReadOnlyErrMsg)

	// can't do point update when tidb_restricted_read_only is on
	_, err = s.udb.Exec("update t set b = 2 where a = 1")
	require.Error(t, err)
	require.Equal(t, err.Error(), ReadOnlyErrMsg)

	// can't insert
	_, err = s.udb.Exec("insert into t values (2, 3)")
	require.Error(t, err)
	require.Equal(t, err.Error(), ReadOnlyErrMsg)

	// can't turn off tidb_super_read_only if tidb_restricted_read_only is on
	err = setVariable(t, s.db, TiDBSuperReadOnly, 0)
	require.Error(t, err)
	require.Equal(t, err.Error(), ConflictErrMsg)

	// can't change global variable
	err = setVariable(t, s.udb, TiDBSuperReadOnly, 0)
	require.Error(t, err)
	require.Equal(t, err.Error(), PriviledgedErrMsg)

	err = setVariable(t, s.rdb, TiDBSuperReadOnly, 0)
	require.Error(t, err)
	require.Equal(t, err.Error(), PriviledgedErrMsg)

	// turn off tidb_restricted_read_only does not affect tidb_super_read_only
	setVariableNoError(t, s.db, TiDBRestrictedReadOnly, 0)

	checkVariable(t, s.udb, TiDBRestrictedReadOnly, false)
	checkVariable(t, s.rdb, TiDBRestrictedReadOnly, false)

	checkVariable(t, s.udb, TiDBSuperReadOnly, true)
	checkVariable(t, s.rdb, TiDBSuperReadOnly, true)

	// it is now allowed to turn off tidb_super_read_only
	setVariableNoError(t, s.db, TiDBSuperReadOnly, 0)

	checkVariable(t, s.udb, TiDBRestrictedReadOnly, false)
	checkVariable(t, s.rdb, TiDBRestrictedReadOnly, false)

	checkVariable(t, s.udb, TiDBSuperReadOnly, false)
	checkVariable(t, s.rdb, TiDBSuperReadOnly, false)
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
