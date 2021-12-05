// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
)

type globalMetadata struct {
	tctx            *tcontext.Context
	buffer          bytes.Buffer
	afterConnBuffer bytes.Buffer
	snapshot        string

	storage storage.ExternalStorage
}

const (
	metadataPath       = "metadata"
	metadataTimeLayout = "2006-01-02 15:04:05"

	fileFieldIndex    = 0
	posFieldIndex     = 1
	gtidSetFieldIndex = 4
)

func newGlobalMetadata(tctx *tcontext.Context, s storage.ExternalStorage, snapshot string) *globalMetadata {
	return &globalMetadata{
		tctx:     tctx,
		storage:  s,
		buffer:   bytes.Buffer{},
		snapshot: snapshot,
	}
}

func (m globalMetadata) String() string {
	return m.buffer.String()
}

func (m *globalMetadata) recordStartTime(t time.Time) {
	m.buffer.WriteString("Started dump at: " + t.Format(metadataTimeLayout) + "\n")
}

func (m *globalMetadata) recordFinishTime(t time.Time) {
	m.buffer.Write(m.afterConnBuffer.Bytes())
	m.buffer.WriteString("Finished dump at: " + t.Format(metadataTimeLayout) + "\n")
}

func (m *globalMetadata) recordGlobalMetaData(db *sql.Conn, serverType version.ServerType, afterConn bool) error { // revive:disable-line:flag-parameter
	if afterConn {
		m.afterConnBuffer.Reset()
		return recordGlobalMetaData(m.tctx, db, &m.afterConnBuffer, serverType, afterConn, m.snapshot)
	}
	return recordGlobalMetaData(m.tctx, db, &m.buffer, serverType, afterConn, m.snapshot)
}

func recordGlobalMetaData(tctx *tcontext.Context, db *sql.Conn, buffer *bytes.Buffer, serverType version.ServerType, afterConn bool, snapshot string) error { // revive:disable-line:flag-parameter
	writeMasterStatusHeader := func() {
		buffer.WriteString("SHOW MASTER STATUS:")
		if afterConn {
			buffer.WriteString(" /* AFTER CONNECTION POOL ESTABLISHED */")
		}
		buffer.WriteString("\n")
	}

	switch serverType {
	// For MySQL:
	// mysql 5.6+
	// mysql> SHOW MASTER STATUS;
	// +-----------+----------+--------------+------------------+-------------------------------------------+
	// | File      | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                         |
	// +-----------+----------+--------------+------------------+-------------------------------------------+
	// | ON.000001 |     7502 |              |                  | 6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29 |
	// +-----------+----------+--------------+------------------+-------------------------------------------+
	// 1 row in set (0.00 sec)
	// mysql 5.5- doesn't have column Executed_Gtid_Set
	//
	// For TiDB:
	// mysql> SHOW MASTER STATUS;
	// +-------------+--------------------+--------------+------------------+-------------------+
	// | File        | Position           | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set |
	// +-------------+--------------------+--------------+------------------+-------------------+
	// | tidb-binlog | 415195906970746880 |              |                  |                   |
	// +-------------+--------------------+--------------+------------------+-------------------+
	// 1 row in set (0.00 sec)
	case version.ServerTypeMySQL, version.ServerTypeTiDB:
		str, err := ShowMasterStatus(db)
		if err != nil {
			return err
		}
		logFile := getValidStr(str, fileFieldIndex)
		var pos string
		if serverType == version.ServerTypeTiDB && snapshot != "" {
			pos = snapshot
		} else {
			pos = getValidStr(str, posFieldIndex)
		}
		gtidSet := getValidStr(str, gtidSetFieldIndex)

		if logFile != "" {
			writeMasterStatusHeader()
			fmt.Fprintf(buffer, "\tLog: %s\n\tPos: %s\n\tGTID:%s\n", logFile, pos, gtidSet)
		}
	// For MariaDB:
	// SHOW MASTER STATUS;
	// +--------------------+----------+--------------+------------------+
	// | File               | Position | Binlog_Do_DB | Binlog_Ignore_DB |
	// +--------------------+----------+--------------+------------------+
	// | mariadb-bin.000016 |      475 |              |                  |
	// +--------------------+----------+--------------+------------------+
	// SELECT @@global.gtid_binlog_pos;
	// +--------------------------+
	// | @@global.gtid_binlog_pos |
	// +--------------------------+
	// | 0-1-2                    |
	// +--------------------------+
	// 1 row in set (0.00 sec)
	case version.ServerTypeMariaDB:
		str, err := ShowMasterStatus(db)
		if err != nil {
			return err
		}
		logFile := getValidStr(str, fileFieldIndex)
		pos := getValidStr(str, posFieldIndex)
		var gtidSet string
		err = db.QueryRowContext(context.Background(), "SELECT @@global.gtid_binlog_pos").Scan(&gtidSet)
		if err != nil {
			tctx.L().Warn("fail to get gtid for mariaDB", zap.Error(err))
		}

		if logFile != "" {
			writeMasterStatusHeader()
			fmt.Fprintf(buffer, "\tLog: %s\n\tPos: %s\n\tGTID:%s\n", logFile, pos, gtidSet)
		}
	default:
		return errors.Errorf("unsupported serverType %s for recordGlobalMetaData", serverType.String())
	}
	buffer.WriteString("\n")
	if serverType == version.ServerTypeTiDB {
		return nil
	}

	// omit follower status if called after connection pool established
	if afterConn {
		return nil
	}
	// get follower status info
	var (
		isms  bool
		query string
	)
	if err := simpleQuery(db, "SELECT @@default_master_connection", func(rows *sql.Rows) error {
		isms = true
		return nil
	}); err != nil {
		isms = false
	}
	if isms {
		query = "SHOW ALL SLAVES STATUS"
	} else {
		query = "SHOW SLAVE STATUS"
	}
	return simpleQuery(db, query, func(rows *sql.Rows) error {
		cols, err := rows.Columns()
		if err != nil {
			return errors.Trace(err)
		}
		data := make([]sql.NullString, len(cols))
		args := make([]interface{}, 0, len(cols))
		for i := range data {
			args = append(args, &data[i])
		}
		if err := rows.Scan(args...); err != nil {
			return errors.Trace(err)
		}
		var connName, pos, logFile, host, gtidSet string
		for i, col := range cols {
			if data[i].Valid {
				col = strings.ToLower(col)
				switch col {
				case "connection_name":
					connName = data[i].String
				case "exec_master_log_pos":
					pos = data[i].String
				case "relay_master_log_file":
					logFile = data[i].String
				case "master_host":
					host = data[i].String
				case "executed_gtid_set":
					gtidSet = data[i].String
				}
			}
		}
		if len(host) > 0 {
			buffer.WriteString("SHOW SLAVE STATUS:\n")
			if isms {
				buffer.WriteString("\tConnection name: " + connName + "\n")
			}
			fmt.Fprintf(buffer, "\tHost: %s\n\tLog: %s\n\tPos: %s\n\tGTID:%s\n\n", host, logFile, pos, gtidSet)
		}
		return nil
	})
}

func (m *globalMetadata) writeGlobalMetaData() error {
	// keep consistent with mydumper. Never compress metadata
	fileWriter, tearDown, err := buildFileWriter(m.tctx, m.storage, metadataPath, storage.NoCompression)
	if err != nil {
		return err
	}
	defer tearDown(m.tctx)

	return write(m.tctx, fileWriter, m.String())
}

func getValidStr(str []string, idx int) string {
	if idx < len(str) {
		return str[idx]
	}
	return ""
}
