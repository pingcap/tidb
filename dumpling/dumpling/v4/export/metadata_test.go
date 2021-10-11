// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	tcontext "github.com/pingcap/dumpling/v4/context"
	"github.com/stretchr/testify/require"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/br/pkg/storage"
)

const (
	logFile = "ON.000001"
	pos     = "7502"
	gtidSet = "6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29"
)

func TestMysqlMetaData(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos, "", "", gtidSet)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT @@default_master_connection").WillReturnError(fmt.Errorf("mock error"))
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"exec_master_log_pos", "relay_master_log_file", "master_host", "Executed_Gtid_Set", "Seconds_Behind_Master"}))

	m := newGlobalMetadata(tcontext.Background(), createStorage(t), "")
	require.NoError(t, m.recordGlobalMetaData(conn, ServerTypeMySQL, false))

	expected := "SHOW MASTER STATUS:\n" +
		"\tLog: ON.000001\n" +
		"\tPos: 7502\n" +
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n"
	require.Equal(t, expected, m.buffer.String())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMetaDataAfterConn(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos, "", "", gtidSet)
	pos2 := "7510"
	rows2 := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos2, "", "", gtidSet)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT @@default_master_connection").WillReturnError(fmt.Errorf("mock error"))
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"exec_master_log_pos", "relay_master_log_file", "master_host", "Executed_Gtid_Set", "Seconds_Behind_Master"}))
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows2)

	m := newGlobalMetadata(tcontext.Background(), createStorage(t), "")
	require.NoError(t, m.recordGlobalMetaData(conn, ServerTypeMySQL, false))
	require.NoError(t, m.recordGlobalMetaData(conn, ServerTypeMySQL, true))

	m.buffer.Write(m.afterConnBuffer.Bytes())

	expected := "SHOW MASTER STATUS:\n" +
		"\tLog: ON.000001\n" +
		"\tPos: 7502\n" +
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n" +
		"SHOW MASTER STATUS: /* AFTER CONNECTION POOL ESTABLISHED */\n" +
		"\tLog: ON.000001\n" +
		"\tPos: 7510\n" +
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n"
	require.Equal(t, expected, m.buffer.String())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMysqlWithFollowersMetaData(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos, "", "", gtidSet)
	followerRows := sqlmock.NewRows([]string{"exec_master_log_pos", "relay_master_log_file", "master_host", "Executed_Gtid_Set", "Seconds_Behind_Master"}).
		AddRow("256529431", "mysql-bin.001821", "192.168.1.100", gtidSet, 0)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT @@default_master_connection").WillReturnError(fmt.Errorf("mock error"))
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(followerRows)

	m := newGlobalMetadata(tcontext.Background(), createStorage(t), "")
	require.NoError(t, m.recordGlobalMetaData(conn, ServerTypeMySQL, false))

	expected := "SHOW MASTER STATUS:\n" +
		"\tLog: ON.000001\n" +
		"\tPos: 7502\n" +
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n" +
		"SHOW SLAVE STATUS:\n" +
		"\tHost: 192.168.1.100\n" +
		"\tLog: mysql-bin.001821\n" +
		"\tPos: 256529431\n" +
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n"
	require.Equal(t, expected, m.buffer.String())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMysqlWithNullFollowersMetaData(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos, "", "", gtidSet)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT @@default_master_connection").WillReturnError(fmt.Errorf("mock error"))
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(sqlmock.NewRows([]string{"SQL_Remaining_Delay"}).AddRow(nil))

	m := newGlobalMetadata(tcontext.Background(), createStorage(t), "")
	require.NoError(t, m.recordGlobalMetaData(conn, ServerTypeMySQL, false))

	expected := "SHOW MASTER STATUS:\n" +
		"\tLog: ON.000001\n" +
		"\tPos: 7502\n" +
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n"
	require.Equal(t, expected, m.buffer.String())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMariaDBMetaData(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	logFile := "mariadb-bin.000016"
	pos := "475"
	gtidSet := "0-1-2"
	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
		AddRow(logFile, pos, "", "")
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	rows = sqlmock.NewRows([]string{"@@global.gtid_binlog_pos"}).
		AddRow(gtidSet)
	mock.ExpectQuery("SELECT @@global.gtid_binlog_pos").WillReturnRows(rows)
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(rows)
	m := newGlobalMetadata(tcontext.Background(), createStorage(t), "")
	require.NoError(t, m.recordGlobalMetaData(conn, ServerTypeMariaDB, false))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMariaDBWithFollowersMetaData(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
		AddRow(logFile, pos, "", "", gtidSet)
	followerRows := sqlmock.NewRows([]string{"exec_master_log_pos", "relay_master_log_file", "master_host", "Executed_Gtid_Set", "connection_name", "Seconds_Behind_Master"}).
		AddRow("256529431", "mysql-bin.001821", "192.168.1.100", gtidSet, "connection_1", 0).
		AddRow("256529451", "mysql-bin.001820", "192.168.1.102", gtidSet, "connection_2", 200)
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT @@default_master_connection").
		WillReturnRows(sqlmock.NewRows([]string{"@@default_master_connection"}).
			AddRow("connection_1"))
	mock.ExpectQuery("SHOW ALL SLAVES STATUS").WillReturnRows(followerRows)

	m := newGlobalMetadata(tcontext.Background(), createStorage(t), "")
	require.NoError(t, m.recordGlobalMetaData(conn, ServerTypeMySQL, false))

	expected := "SHOW MASTER STATUS:\n" +
		"\tLog: ON.000001\n" +
		"\tPos: 7502\n" +
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n" +
		"SHOW SLAVE STATUS:\n" +
		"\tConnection name: connection_1\n" +
		"\tHost: 192.168.1.100\n" +
		"\tLog: mysql-bin.001821\n" +
		"\tPos: 256529431\n" +
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n" +
		"SHOW SLAVE STATUS:\n" +
		"\tConnection name: connection_2\n" +
		"\tHost: 192.168.1.102\n" +
		"\tLog: mysql-bin.001820\n" +
		"\tPos: 256529451\n" +
		"\tGTID:6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29\n\n"
	require.Equal(t, expected, m.buffer.String())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestEarlierMysqlMetaData(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	logFile := "mysql-bin.000001"
	pos := "4879"
	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
		AddRow(logFile, pos, "", "")
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	mock.ExpectQuery("SELECT @@default_master_connection").WillReturnError(fmt.Errorf("mock error"))
	mock.ExpectQuery("SHOW SLAVE STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"exec_master_log_pos", "relay_master_log_file", "master_host", "Executed_Gtid_Set", "Seconds_Behind_Master"}))

	m := newGlobalMetadata(tcontext.Background(), createStorage(t), "")
	require.NoError(t, m.recordGlobalMetaData(conn, ServerTypeMySQL, false))

	expected := "SHOW MASTER STATUS:\n" +
		"\tLog: mysql-bin.000001\n" +
		"\tPos: 4879\n" +
		"\tGTID:\n\n"
	require.Equal(t, expected, m.buffer.String())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTiDBSnapshotMetaData(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	logFile := "tidb-binlog"
	pos := "420633329401856001"
	rows := sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
		AddRow(logFile, pos, "", "")
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)

	m := newGlobalMetadata(tcontext.Background(), createStorage(t), "")
	require.NoError(t, m.recordGlobalMetaData(conn, ServerTypeTiDB, false))

	expected := "SHOW MASTER STATUS:\n" +
		"\tLog: tidb-binlog\n" +
		"\tPos: 420633329401856001\n" +
		"\tGTID:\n\n"
	require.Equal(t, expected, m.buffer.String())

	snapshot := "420633273211289601"
	rows = sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
		AddRow(logFile, pos, "", "")
	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(rows)
	m = newGlobalMetadata(tcontext.Background(), createStorage(t), snapshot)
	require.NoError(t, m.recordGlobalMetaData(conn, ServerTypeTiDB, false))

	expected = "SHOW MASTER STATUS:\n" +
		"\tLog: tidb-binlog\n" +
		"\tPos: 420633273211289601\n" +
		"\tGTID:\n\n"
	require.Equal(t, expected, m.buffer.String())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestNoPrivilege(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	mock.ExpectQuery("SHOW MASTER STATUS").WillReturnError(errors.New("lack SUPER or REPLICATION CLIENT privilege"))

	m := newGlobalMetadata(tcontext.Background(), createStorage(t), "")
	// some consistencyType will ignore this error, this test make sure no extra message is written
	require.Error(t, m.recordGlobalMetaData(conn, ServerTypeTiDB, false))
	require.Equal(t, "", m.buffer.String())
}

func createStorage(t *testing.T) storage.ExternalStorage {
	backend, err := storage.ParseBackend("file:///"+os.TempDir(), nil)
	require.NoError(t, err)
	testLoc, _ := storage.Create(context.Background(), backend, true)
	return testLoc
}
