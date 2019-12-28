package export

import (
	"context"
	"database/sql"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

func Dump(conf *Config) error {
	databases, serverInfo, err := prepareMeta(conf)
	if err != nil {
		return err
	}

	conf.ServerInfo = serverInfo
	for _, database := range databases {
		fsWriter, err := NewSimpleWriter(conf)
		if err != nil {
			return err
		}
		if err := dumpDatabase(context.Background(), conf, database, fsWriter); err != nil {
			return err
		}
	}
	return nil
}

func prepareMeta(conf *Config) (databases []string, info ServerInfo, err error) {
	pool, err := sql.Open("mysql", conf.getDSN(""))
	if err != nil {
		return nil, ServerInfoUnknown, withStack(err)
	}
	defer pool.Close()

	if conf.Database == "" {
		databases, err = ShowDatabases(pool)
		if err != nil {
			return nil, ServerInfoUnknown, withStack(err)
		}
	} else {
		databases = strings.Split(conf.Database, ",")
	}

	versionStr, err := SelectVersion(pool)
	if err != nil {
		return databases, ServerInfoUnknown, withStack(err)
	}
	info = ParseServerInfo(versionStr)
	return
}

func dumpDatabase(ctx context.Context, conf *Config, dbName string, writer Writer) error {
	dsn := conf.getDSN(dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return withStack(err)
	}
	defer db.Close()

	createDatabaseSQL, err := ShowCreateDatabase(db, dbName)
	if err != nil {
		return err
	}
	if err := writer.WriteDatabaseMeta(ctx, dbName, createDatabaseSQL); err != nil {
		return err
	}

	tables, err := ShowTables(db)
	if err != nil {
		return err
	}

	rateLimit := newRateLimit(conf.Threads)
	var wg sync.WaitGroup
	wg.Add(len(tables))
	res := make([]error, len(tables))
	for i, table := range tables {
		go func(ith int, table string, wg *sync.WaitGroup, res []error) {
			defer wg.Done()
			createTableSQL, err := ShowCreateTable(db, dbName, table)
			if err != nil {
				res[ith] = err
				return
			}
			if err := writer.WriteTableMeta(ctx, dbName, table, createTableSQL); err != nil {
				res[ith] = err
				return
			}

			rateLimit.getToken()
			tableIR, err := SelectAllFromTable(conf, db, dbName, table)
			defer rateLimit.putToken()
			if err != nil {
				res[ith] = err
				return
			}

			if err := writer.WriteTableData(ctx, tableIR); err != nil {
				res[ith] = err
				return
			}
		}(i, table, &wg, res)
	}
	wg.Wait()
	for _, err := range res {
		if err != nil {
			return err
		}
	}
	return nil
}
