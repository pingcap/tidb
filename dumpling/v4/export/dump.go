package export

import (
	"context"
	"database/sql"
	"golang.org/x/sync/errgroup"

	_ "github.com/go-sql-driver/mysql"
)

func Dump(conf *Config) (err error) {
	pool, err := sql.Open("mysql", conf.getDSN(""))
	if err != nil {
		return withStack(err)
	}
	defer pool.Close()

	conf.ServerInfo, err = detectServerInfo(pool)
	if err != nil {
		return err
	}

	databases, err := prepareDumpingDatabases(conf, pool)
	if err != nil {
		return err
	}

	conf.Tables, err = listAllTables(pool, databases)
	if err != nil {
		return err
	}

	var conCtrl ConsistencyController = &ConsistencyNone{}
	if err = conCtrl.Setup(); err != nil {
		return err
	}

	fsWriter, err := NewSimpleWriter(conf)
	if err != nil {
		return err
	}
	if err = dumpDatabases(context.Background(), conf, pool, fsWriter); err != nil {
		return err
	}

	return conCtrl.TearDown()
}

func dumpDatabases(ctx context.Context, conf *Config, db *sql.DB, writer Writer) error {
	allTables := conf.Tables
	for dbName, tables := range allTables {
		createDatabaseSQL, err := ShowCreateDatabase(db, dbName)
		if err != nil {
			return err
		}
		if err := writer.WriteDatabaseMeta(ctx, dbName, createDatabaseSQL); err != nil {
			return err
		}

		rateLimit := newRateLimit(conf.Threads)
		var g errgroup.Group
		for _, table := range tables {
			table := table
			g.Go(func() error {
				rateLimit.getToken()
				defer rateLimit.putToken()
				return dumpTable(ctx, conf, db, dbName, table, writer)
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func dumpTable(ctx context.Context, conf *Config, db *sql.DB, dbName, table string, writer Writer) error {
	createTableSQL, err := ShowCreateTable(db, dbName, table)
	if err != nil {
		return err
	}
	if err := writer.WriteTableMeta(ctx, dbName, table, createTableSQL); err != nil {
		return err
	}

	tableIR, err := SelectAllFromTable(conf, db, dbName, table)
	if err != nil {
		return err
	}

	if err := writer.WriteTableData(ctx, tableIR); err != nil {
		return err
	}
	return nil
}
