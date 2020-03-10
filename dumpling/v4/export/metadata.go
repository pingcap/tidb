package export

import (
	"database/sql"
	"errors"
	"path"
	"time"
)

type globalMetadata struct {
	logFile string
	pos string
	gtidSet string

	filePath string
	startTime time.Time
	finishTime time.Time
}

const (
	metadataPath       = "metadata"
	metadataTimeLayout = "2006-01-02 15:04:05"

	fileFieldIndex = 0
	posFieldIndex = 1
	gtidSetFieldIndex = 4

	mariadbShowMasterStatusFieldNum = 4
)

func newGlobalMetadata(outputDir string) *globalMetadata {
	return &globalMetadata{
		filePath: path.Join(outputDir, metadataPath),
	}
}

func (m globalMetadata) String() string {
	str := ""
	if m.startTime.IsZero() {
		return str
	}
	str += "Started dump at: " + m.startTime.Format(metadataTimeLayout) + "\n"

	str += "SHOW MASTER STATUS:"
	if m.logFile != "" {
		str += "\t\tLog: " + m.logFile + "\n"
	}
	if m.pos != "" {
		str += "\t\tPos: " + m.pos + "\n"
	}
	if m.gtidSet != "" {
		str += "\t\tGTID:" + m.gtidSet + "\n"
	}

	if m.finishTime.IsZero() {
		return str
	}
	str += "Finished dump at: " + m.finishTime.Format(metadataTimeLayout) + "\n"
	return str
}

func (m *globalMetadata) recordStartTime(t time.Time) {
	m.startTime = t
}

func (m *globalMetadata) recordFinishTime(t time.Time)  {
	m.finishTime = t
}

func (m *globalMetadata) getGlobalMetaData(db *sql.DB, serverType ServerType) error {
	switch serverType {
	// For MySQL:
	// mysql> SHOW MASTER STATUS;
	// +-----------+----------+--------------+------------------+-------------------------------------------+
	// | File      | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                         |
	// +-----------+----------+--------------+------------------+-------------------------------------------+
	// | ON.000001 |     7502 |              |                  | 6ce40be3-e359-11e9-87e0-36933cb0ca5a:1-29 |
	// +-----------+----------+--------------+------------------+-------------------------------------------+
	// 1 row in set (0.00 sec)
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
		str, err := ShowMasterStatus(db, showMasterStatusFieldNum)
		if err != nil {
			return err
		}
		m.logFile = str[fileFieldIndex]
		m.pos = str[posFieldIndex]
		m.gtidSet = str[gtidSetFieldIndex]
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
		str, err := ShowMasterStatus(db, mariadbShowMasterStatusFieldNum)
		if err != nil {
			return err
		}
		m.logFile = str[fileFieldIndex]
		m.pos = str[posFieldIndex]
		err = db.QueryRow("SELECT @@global.gtid_binlog_pos").Scan(&m.gtidSet)
		if err != nil {
			return err
		}
	default:
		return errors.New("unsupported serverType" +serverType.String() + "for getGlobalMetaData")
	}
	return nil
}

func (m globalMetadata) writeGlobalMetaData() error {
	fileWriter, tearDown, err := buildFileWriter(m.filePath)
	if err != nil {
		return err
	}
	defer tearDown()

	return write(fileWriter, m.String())
}