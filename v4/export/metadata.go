package export

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/dumpling/v4/log"
	"go.uber.org/zap"
)

type globalMetadata struct {
	buffer bytes.Buffer

	storage storage.ExternalStorage
}

const (
	metadataPath       = "metadata"
	metadataTimeLayout = "2006-01-02 15:04:05"

	fileFieldIndex    = 0
	posFieldIndex     = 1
	gtidSetFieldIndex = 4

	mariadbShowMasterStatusFieldNum = 4
)

func newGlobalMetadata(s storage.ExternalStorage) *globalMetadata {
	return &globalMetadata{
		storage: s,
		buffer:  bytes.Buffer{},
	}
}

func (m globalMetadata) String() string {
	return m.buffer.String()
}

func (m *globalMetadata) recordStartTime(t time.Time) {
	m.buffer.WriteString("Started dump at: " + t.Format(metadataTimeLayout) + "\n")
}

func (m *globalMetadata) recordFinishTime(t time.Time) {
	m.buffer.WriteString("Finished dump at: " + t.Format(metadataTimeLayout) + "\n")
}

func (m *globalMetadata) recordGlobalMetaData(db *sql.Conn, serverType ServerType, afterConn bool) error {
	// get master status info
	m.buffer.WriteString("SHOW MASTER STATUS:")
	if afterConn {
		m.buffer.WriteString(" /* AFTER CONNECTION POOL ESTABLISHED */")
	}
	m.buffer.WriteString("\n")
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
	case ServerTypeMySQL, ServerTypeTiDB:
		str, err := ShowMasterStatus(db)
		if err != nil {
			return err
		}
		logFile := getValidStr(str, fileFieldIndex)
		pos := getValidStr(str, posFieldIndex)
		gtidSet := getValidStr(str, gtidSetFieldIndex)

		if logFile != "" {
			fmt.Fprintf(&m.buffer, "\tLog: %s\n\tPos: %s\n\tGTID:%s\n", logFile, pos, gtidSet)
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
	case ServerTypeMariaDB:
		str, err := ShowMasterStatus(db)
		if err != nil {
			return err
		}
		logFile := getValidStr(str, fileFieldIndex)
		pos := getValidStr(str, posFieldIndex)
		var gtidSet string
		err = db.QueryRowContext(context.Background(), "SELECT @@global.gtid_binlog_pos").Scan(&gtidSet)
		if err != nil {
			log.Error("fail to get gtid for mariaDB", zap.Error(err))
		}

		if logFile != "" {
			fmt.Fprintf(&m.buffer, "\tLog: %s\n\tPos: %s\n\tGTID:%s\n", logFile, pos, gtidSet)
		}
	default:
		return errors.New("unsupported serverType" + serverType.String() + "for recordGlobalMetaData")
	}
	m.buffer.WriteString("\n")
	if serverType == ServerTypeTiDB {
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
			return err
		}
		data := make([]sql.NullString, len(cols))
		args := make([]interface{}, 0, len(cols))
		for i := range data {
			args = append(args, &data[i])
		}
		if err := rows.Scan(args...); err != nil {
			return err
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
			m.buffer.WriteString("SHOW SLAVE STATUS:\n")
			if isms {
				m.buffer.WriteString("\tConnection name: " + connName + "\n")
			}
			fmt.Fprintf(&m.buffer, "\tHost: %s\n\tLog: %s\n\tPos: %s\n\tGTID:%s\n\n", host, logFile, pos, gtidSet)
		}
		return nil
	})
}

func (m *globalMetadata) writeGlobalMetaData(ctx context.Context) error {
	fileWriter, tearDown, err := buildFileWriter(ctx, m.storage, metadataPath)
	if err != nil {
		return err
	}
	defer tearDown(ctx)

	return write(ctx, fileWriter, m.String())
}

func getValidStr(str []string, idx int) string {
	if idx < len(str) {
		return str[idx]
	}
	return ""
}
