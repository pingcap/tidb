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
	databases, err = getDumpingDatabaseNames(pool, conf)
	if err != nil {
		return nil, ServerInfoUnknown, withStack(err)
	}
	info, err = detectServerInfo(pool)
	if err != nil {
		return databases, ServerInfoUnknown, withStack(err)
	}
	return
}

func getDumpingDatabaseNames(pool *sql.DB, conf *Config) ([]string, error) {
	if conf.Database == "" {
		return showDatabases(pool)
	}
	return strings.Split(conf.Database, ","), nil
}

func detectServerInfo(db *sql.DB) (ServerInfo, error) {
	var versionInfo string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&versionInfo)
	}
	err := simpleQuery(db, "SELECT version()", handleOneRow)
	if err != nil {
		return ServerInfoUnknown, withStack(err)
	}
	return ParseServerInfo(versionInfo), nil
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
			tableIR, err := dumpTable(conf, db, dbName, table)
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

func dumpTable(conf *Config, db *sql.DB, database, table string) (TableDataIR, error) {
	colTypes, err := getColumnTypes(db, table)
	if err != nil {
		return nil, err
	}

	query, err := buildSelectAllQuery(conf, db, database, table)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(query)
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

func buildSelectAllQuery(conf *Config, db *sql.DB, database, table string) (string, error) {
	var query strings.Builder
	query.WriteString("SELECT * FROM ")
	query.WriteString(database)
	query.WriteString(".")
	query.WriteString(table)
	if conf.SortByPk {
		orderByClause, err := buildOrderByClause(conf, db, database, table)
		if err != nil {
			return "", err
		}
		if orderByClause != "" {
			query.WriteString(" ")
			query.WriteString(orderByClause)
		}
	}
	return query.String(), nil
}

const errBadFieldCode = 1054

func buildOrderByClause(conf *Config, db *sql.DB, database, table string) (string, error) {
	if conf.ServerInfo.ServerType == ServerTypeTiDB {
		tiDBRowIDQuery := fmt.Sprintf("SELECT _tidb_rowid from %s.%s LIMIT 0", database, table)
		isBadField, otherErr := queryReturnExpectedErrorCode(db, tiDBRowIDQuery, errBadFieldCode)
		if otherErr != nil {
			return "", otherErr
		}
		if isBadField {
			return "", nil
		}
		return "ORDER BY _tidb_rowid", nil
	}
	pkName, err := getPrimaryKeyName(db, database, table)
	if err != nil {
		return "", err
	}
	tableContainsPriKey := pkName != ""
	if tableContainsPriKey {
		return fmt.Sprintf("ORDER BY %s", pkName), nil
	}
	return "", nil
}

func queryReturnExpectedErrorCode(db *sql.DB, sql string, errCode int) (bool, error) {
	_, err := db.Exec(sql)
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, fmt.Sprintf("%d", errCode)) {
			return true, nil
		}
		return false, withStack(err)
	}
	return false, nil
}

func getPrimaryKeyName(db *sql.DB, database, table string) (string, error) {
	priKeyQuery := `SELECT column_name FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? AND column_key = 'PRI';`
	stmt, err := db.Prepare(priKeyQuery)
	if err != nil {
		return "", err
	}
	defer stmt.Close()
	rows, err := stmt.Query(database, table)
	if err != nil {
		return "", withStack(err)
	}

	var colName string
	for rows.Next() {
		if err := rows.Scan(&colName); err != nil {
			rows.Close()
			return "", withStack(err)
		}
	}
	return colName, nil
}
