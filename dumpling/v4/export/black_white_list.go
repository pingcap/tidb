package export

import (
	"github.com/pingcap/dumpling/v4/log"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"go.uber.org/zap"
)

type BWList interface {
	Apply(schema string, table string) bool
}

type BWListMode byte

const (
	NopMode BWListMode = 0x0
	// We have used this black and white list in pingcap/dm and pingcap/tidb-lightning project
	MySQLReplicationMode BWListMode = 0x1
)

type MySQLReplicationConf struct {
	Rules         *filter.Rules
	CaseSensitive bool
}

type BWListConf struct {
	Mode BWListMode

	Rules *MySQLReplicationConf
}

type MySQLReplicationBWList struct {
	*filter.Filter
}

func (bw *MySQLReplicationBWList) Apply(schema, table string) bool {
	return bw.Match(&filter.Table{schema, table})
}

type NopeBWList struct{}

func (bw *NopeBWList) Apply(schema, table string) bool {
	return true
}

func NewBWList(conf BWListConf) (BWList, error) {
	switch conf.Mode {
	case MySQLReplicationMode:
		c := conf.Rules
		f, err := filter.New(c.CaseSensitive, c.Rules)
		if err != nil {
			return nil, withStack(err)
		}

		return &MySQLReplicationBWList{
			Filter: f,
		}, nil
	}

	return &NopeBWList{}, nil
}

func filterDirtySchemaTables(conf *Config) {
	switch conf.ServerInfo.ServerType {
	case ServerTypeTiDB:
		for dbName := range conf.Tables {
			if filter.IsSystemSchema(dbName) {
				log.Warn("unsupported dump schema in TiDB now", zap.String("schema", dbName))
				delete(conf.Tables, dbName)
			}
		}
	}
}

func filterTables(conf *Config) error {
	log.Debug("filter tables")
	// filter dirty schema tables because of non-impedance implementation reasons
	filterDirtySchemaTables(conf)
	dbTables := DatabaseTables{}
	ignoredDBTable := DatabaseTables{}
	bwList, err := NewBWList(conf.BlackWhiteList)
	if err != nil {
		return withStack(err)
	}

	for dbName, tables := range conf.Tables {
		for _, table := range tables {
			if bwList.Apply(dbName, table.Name) {
				dbTables.AppendTable(dbName, table)
			} else {
				ignoredDBTable.AppendTable(dbName, table)
			}
		}
	}

	if len(ignoredDBTable) > 0 {
		log.Debug("ignore table", zap.String("", ignoredDBTable.Literal()))
	}

	conf.Tables = dbTables
	return nil
}
