// Copyright 2021 PingCAP, Inc.
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

package readonlytest

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

var (
	tidbRootPassword       = flag.String("passwd", "", "tidb root password")
	tidbAPort              = flag.Int("tidb_a_port", 4000, "first tidb server listening port")
	tidbBPort              = flag.Int("tidb_b_port", 4001, "second tidb server listening port")
	ReadOnlyErrMsg         = "Error 1836: Running in read-only mode"
	TiDBRestrictedReadOnly = "tidb_restricted_read_only"
	TiDBSuperReadOnly      = "tidb_super_read_only"
)

type ReadOnlySuite struct {
	db  *sql.DB
	udb *sql.DB
	rdb *sql.DB
}

func checkVariable(t *testing.T, db *sql.DB, variable string, on bool) {
	var name, status string
	rs, err := db.Query(fmt.Sprintf("show variables like '%s'", variable))
	require.NoError(t, err)
	require.True(t, rs.Next())

	require.NoError(t, rs.Scan(&name, &status))
	require.Equal(t, name, variable)
	if on {
		require.Equal(t, status, "ON")
	} else {
		require.Equal(t, status, "OFF")
	}
	require.NoError(t, rs.Close())
}

func setVariableNoError(t *testing.T, db *sql.DB, variable string, status int) {
	_, err := db.Exec(fmt.Sprintf("set global %s=%d", variable, status))
	require.NoError(t, err)
}

func setVariable(t *testing.T, db *sql.DB, variable string, status int) error {
	_, err := db.Exec(fmt.Sprintf("set global %s=%d", variable, status))
	return err
}

func createReadOnlySuite(t *testing.T) (s *ReadOnlySuite, clean func()) {
	s = new(ReadOnlySuite)
	var err error
	s.db, err = sql.Open("mysql", fmt.Sprintf("root:%s@(%s:%d)/test", *tidbRootPassword, "127.0.0.1", *tidbAPort))
	require.NoError(t, err)

	setVariableNoError(t, s.db, TiDBRestrictedReadOnly, 0)
	setVariableNoError(t, s.db, TiDBSuperReadOnly, 0)

	_, err = s.db.Exec("drop user if exists 'u1'@'%'")
	require.NoError(t, err)
	_, err = s.db.Exec("create user 'u1'@'%' identified by 'password'")
	require.NoError(t, err)
	_, err = s.db.Exec("grant all privileges on test.* to 'u1'@'%'")
	require.NoError(t, err)
	s.udb, err = sql.Open("mysql", fmt.Sprintf("u1:password@(%s:%d)/test", "127.0.0.1", *tidbBPort))
	require.NoError(t, err)
	_, err = s.db.Exec("drop user if exists 'r1'@'%'")
	require.NoError(t, err)
	_, err = s.db.Exec("create user 'r1'@'%' identified by 'password'")
	require.NoError(t, err)
	_, err = s.db.Exec("grant all privileges on test.* to 'r1'@'%'")
	require.NoError(t, err)
	_, err = s.db.Exec("grant RESTRICTED_REPLICA_WRITER_ADMIN on *.* to 'r1'@'%'")
	require.NoError(t, err)
	s.rdb, err = sql.Open("mysql", fmt.Sprintf("r1:password@(%s:%d)/test", "127.0.0.1", *tidbBPort))
	require.NoError(t, err)
	clean = func() {
		require.NoError(t, s.db.Close())
		require.NoError(t, s.rdb.Close())
		require.NoError(t, s.udb.Close())
	}
	return
}

func TestRestriction(t *testing.T) {
	s, clean := createReadOnlySuite(t)
	defer clean()
	setVariable(t, s.db, TiDBRestrictedReadOnly, 1)

	time.Sleep(1)

	checkVariable(t, s.udb, TiDBRestrictedReadOnly, true)
	checkVariable(t, s.udb, TiDBSuperReadOnly, true)

	checkVariable(t, s.rdb, TiDBRestrictedReadOnly, true)
	checkVariable(t, s.rdb, TiDBSuperReadOnly, true)

	_, err := s.udb.Exec("create table t(a int)")
	require.Error(t, err)
	require.Equal(t, err.Error(), ReadOnlyErrMsg)
}

func TestRestrictionWithConnectionPool(t *testing.T) {
	s, clean := createReadOnlySuite(t)
	defer clean()
	var err error
	_, err = s.db.Exec("drop table if exists t")
	require.NoError(t, err)
	_, err = s.db.Exec("create table t (a int)")
	require.NoError(t, err)

	conn, err := s.udb.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()
	done := make(chan bool)
	go func(conn *sql.Conn, done chan bool) {
		ticker := time.NewTicker(50 * time.Millisecond)
		for {
			t := <-ticker.C
			_, err := conn.ExecContext(context.Background(), fmt.Sprintf("insert into t values (%d)", t.Nanosecond()))
			if err != nil {
				if err.Error() == ReadOnlyErrMsg {
					done <- true
				} else {
					done <- false
				}
				return
			}
		}
	}(conn, done)
	time.Sleep(1 * time.Second)

	timer := time.NewTimer(10 * time.Second)
	setVariableNoError(t, s.db, TiDBRestrictedReadOnly, 1)
	select {
	case <-timer.C:
		require.Fail(t, "failed")
	case success := <-done:
		require.True(t, success)
	}
}

func TestReplicationWriter(t *testing.T) {
	s, clean := createReadOnlySuite(t)
	defer clean()
	_, err := s.db.Exec("set global tidb_restricted_read_only=0")
	require.NoError(t, err)
	_, err = s.db.Exec("drop table if exists t")
	require.NoError(t, err)
	_, err = s.db.Exec("create table t (a int)")
	require.NoError(t, err)

	conn, err := s.rdb.Conn(context.Background())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()
	done := make(chan struct{})
	go func(conn *sql.Conn) {
		ticker := time.NewTicker(50 * time.Millisecond)
		for {
			select {
			case t1 := <-ticker.C:
				_, err := conn.ExecContext(context.Background(), fmt.Sprintf("insert into t values (%d)", t1.Nanosecond()))
				require.NoError(t, err)
			case <-done:
				return
			}
		}
	}(conn)
	time.Sleep(1 * time.Second)
	timer := time.NewTimer(3 * time.Second)
	_, err = s.db.Exec("set global tidb_restricted_read_only=1")
	require.NoError(t, err)
	// SUPER user can't write
	_, err = s.db.Exec("insert into t values (1)")
	require.Equal(t, err.Error(), ReadOnlyErrMsg)
	<-timer.C
	done <- struct{}{}
}
