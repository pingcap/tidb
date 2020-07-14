package export

import (
	"database/sql"
	"strings"
	"text/template"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/dumpling/v4/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/utils"
)

func adjustConfig(conf *Config) error {
	// Init logger
	if conf.Logger != nil {
		log.SetAppLogger(conf.Logger)
	} else {
		err := log.InitAppLogger(&log.Config{
			Level:  conf.LogLevel,
			File:   conf.LogFile,
			Format: conf.LogFormat,
		})
		if err != nil {
			return err
		}
	}

	// Register TLS config
	if len(conf.Security.CAPath) > 0 {
		tlsConfig, err := utils.ToTLSConfig(conf.Security.CAPath, conf.Security.CertPath, conf.Security.KeyPath)
		if err != nil {
			return err
		}
		err = mysql.RegisterTLSConfig("dumpling-tls-target", tlsConfig)
		if err != nil {
			return err
		}
	}

	if conf.Rows != UnspecifiedSize {
		// Disable filesize if rows was set
		conf.FileSize = UnspecifiedSize
	}
	if conf.SessionParams == nil {
		conf.SessionParams = make(map[string]interface{})
	}
	if conf.OutputFileTemplate == nil {
		var err error
		conf.OutputFileTemplate, err = template.New("filename").Parse("{{.DB}}.{{.Table}}.{{.Index}}")
		if err != nil {
			return err
		}
	}
	resolveAutoConsistency(conf)

	return nil
}

func detectServerInfo(db *sql.DB) (ServerInfo, error) {
	versionStr, err := SelectVersion(db)
	if err != nil {
		return ServerInfoUnknown, err
	}
	return ParseServerInfo(versionStr), nil
}

func prepareDumpingDatabases(conf *Config, db *sql.DB) ([]string, error) {
	databases, err := ShowDatabases(db)
	if len(conf.Databases) == 0 {
		return databases, err
	} else {
		dbMap := make(map[string]interface{}, len(databases))
		for _, database := range databases {
			dbMap[database] = struct{}{}
		}
		var notExistsDatabases []string
		for _, database := range conf.Databases {
			if _, ok := dbMap[database]; !ok {
				notExistsDatabases = append(notExistsDatabases, database)
			}
		}
		if len(notExistsDatabases) > 0 {
			return nil, errors.Errorf("Unknown databases [%s]", strings.Join(notExistsDatabases, ","))
		}
		return conf.Databases, nil
	}
}

func listAllTables(db *sql.DB, databaseNames []string) (DatabaseTables, error) {
	log.Debug("list all the tables")
	return ListAllDatabasesTables(db, databaseNames, TableTypeBase)
}

func listAllViews(db *sql.DB, databaseNames []string) (DatabaseTables, error) {
	log.Debug("list all the views")
	return ListAllDatabasesTables(db, databaseNames, TableTypeView)
}

type databaseName = string

type TableType int8

const (
	TableTypeBase TableType = iota
	TableTypeView
)

type TableInfo struct {
	Name string
	Type TableType
}

func (t *TableInfo) Equals(other *TableInfo) bool {
	return t.Name == other.Name && t.Type == other.Type
}

type DatabaseTables map[databaseName][]*TableInfo

func NewDatabaseTables() DatabaseTables {
	return DatabaseTables{}
}

func (d DatabaseTables) AppendTable(dbName string, table *TableInfo) DatabaseTables {
	d[dbName] = append(d[dbName], table)
	return d
}

func (d DatabaseTables) AppendTables(dbName string, tableNames ...string) DatabaseTables {
	for _, t := range tableNames {
		d[dbName] = append(d[dbName], &TableInfo{t, TableTypeBase})
	}
	return d
}

func (d DatabaseTables) AppendViews(dbName string, viewNames ...string) DatabaseTables {
	for _, v := range viewNames {
		d[dbName] = append(d[dbName], &TableInfo{v, TableTypeView})
	}
	return d
}

func (d DatabaseTables) Merge(other DatabaseTables) {
	for name, infos := range other {
		d[name] = append(d[name], infos...)
	}
}

func (d DatabaseTables) Literal() string {
	var b strings.Builder
	b.WriteString("tables list\n")
	b.WriteString("\n")

	for dbName, tables := range d {
		b.WriteString("schema ")
		b.WriteString(dbName)
		b.WriteString(" :[")
		for _, tbl := range tables {
			b.WriteString(tbl.Name)
			b.WriteString(", ")
		}
		b.WriteString("]")
	}

	return b.String()
}
