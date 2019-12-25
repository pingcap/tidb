package export

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

func Dump(conf *Config) error {
	pool, err := sql.Open("mysql", conf.getDSN(""))
	if err != nil {
		return withStack(err)
	}

	var databases []string
	if conf.Database == "" {
		var err error
		databases, err = showDatabases(pool)
		if err != nil {
			pool.Close()
			return err
		}
	} else {
		databases = strings.Split(conf.Database, ",")
	}
	pool.Close()

	for _, database := range databases {
		fsWriter, err := NewSimpleWriter(extractOutputConfig(conf))
		if err != nil {
			return err
		}
		if err := dumpDatabase(context.Background(), conf, database, fsWriter); err != nil {
			return err
		}
	}
	return nil
}

func simpleQuery(db *sql.DB, sql string, handleOneRow func(*sql.Rows) error) error {
	rows, err := db.Query(sql)
	if err != nil {
		return withStack(err)
	}

	for rows.Next() {
		if err := handleOneRow(rows); err != nil {
			rows.Close()
			return withStack(err)
		}
	}
	return nil
}

type oneStrColumnTable struct {
	data []string
}

func (o *oneStrColumnTable) handleOneRow(rows *sql.Rows) error {
	var str string
	if err := rows.Scan(&str); err != nil {
		return withStack(err)
	}
	o.data = append(o.data, str)
	return nil
}

func showDatabases(db *sql.DB) ([]string, error) {
	var res oneStrColumnTable
	if err := simpleQuery(db, "SHOW DATABASES", res.handleOneRow); err != nil {
		return nil, err
	}
	return res.data, nil
}

// showTables shows the tables of a database, the caller should use the correct database.
func showTables(db *sql.DB) ([]string, error) {
	var res oneStrColumnTable
	if err := simpleQuery(db, "SHOW TABLES", res.handleOneRow); err != nil {
		return nil, err
	}
	return res.data, nil
}

func showCreateDatabase(db *sql.DB, database string) (string, error) {
	var oneRow [2]string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&oneRow[0], &oneRow[1])
	}
	query := fmt.Sprintf("SHOW CREATE DATABASE %s", database)
	err := simpleQuery(db, query, handleOneRow)
	if err != nil {
		return "", err
	}
	return oneRow[1], nil
}

func showCreateTable(db *sql.DB, database, table string) (string, error) {
	var oneRow [2]string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&oneRow[0], &oneRow[1])
	}
	query := fmt.Sprintf("SHOW CREATE TABLE %s.%s", database, table)
	err := simpleQuery(db, query, handleOneRow)
	if err != nil {
		return "", err
	}
	return oneRow[1], nil
}

func dumpDatabase(ctx context.Context, conf *Config, dbName string, writer Writer) error {
	dsn := conf.getDSN(dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return withStack(err)
	}
	defer db.Close()

	createDatabaseSQL, err := showCreateDatabase(db, dbName)
	if err != nil {
		return err
	}
	if err := writer.WriteDatabaseMeta(ctx, dbName, createDatabaseSQL); err != nil {
		return err
	}

	tables, err := showTables(db)
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
			createTableSQL, err := showCreateTable(db, dbName, table)
			if err != nil {
				res[ith] = err
				return
			}
			if err := writer.WriteTableMeta(ctx, dbName, table, createTableSQL); err != nil {
				res[ith] = err
				return
			}

			rateLimit.getToken()
			tableIR, err := dumpTable(ctx, db, dbName, table)
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

func getColumnTypes(db *sql.DB, table string) ([]*sql.ColumnType, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", table))
	if err != nil {
		return nil, withStack(err)
	}
	defer rows.Close()
	return rows.ColumnTypes()
}

type tableDumper interface {
	handleOneRow(ctx context.Context, rows *sql.Rows) error
	prepareColumns(ctx context.Context, colTypes []*sql.ColumnType)
	finishTable(ctx context.Context)
}

func dumpTable(ctx context.Context, db *sql.DB, database, table string) (TableDataIR, error) {
	colTypes, err := getColumnTypes(db, table)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(fmt.Sprintf("SELECT * from %s", table))
	if err != nil {
		return nil, withStack(err)
	}

	return &tableData{
		database: database,
		table:    table,
		rows:     rows,
		colTypes: colTypes,
	}, nil
}
