package export

import (
	"database/sql"
	"fmt"
	"strings"
)

func ShowDatabases(db *sql.DB) ([]string, error) {
	var res oneStrColumnTable
	if err := simpleQuery(db, "SHOW DATABASES", res.handleOneRow); err != nil {
		return nil, err
	}
	return res.data, nil
}

// ShowTables shows the tables of a database, the caller should use the correct database.
func ShowTables(db *sql.DB) ([]string, error) {
	var res oneStrColumnTable
	if err := simpleQuery(db, "SHOW TABLES", res.handleOneRow); err != nil {
		return nil, err
	}
	return res.data, nil
}

func ShowCreateDatabase(db *sql.DB, database string) (string, error) {
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

func ShowCreateTable(db *sql.DB, database, table string) (string, error) {
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

func SelectVersion(db *sql.DB) (string, error) {
	var versionInfo string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&versionInfo)
	}
	err := simpleQuery(db, "SELECT version()", handleOneRow)
	if err != nil {
		return "", withStack(err)
	}
	return versionInfo, nil
}

func SelectAllFromTable(conf *Config, db *sql.DB, database, table string) (TableDataIR, error) {
	colTypes, err := GetColumnTypes(db, table)
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

func buildOrderByClause(conf *Config, db *sql.DB, database, table string) (string, error) {
	if conf.ServerInfo.ServerType == ServerTypeTiDB {
		ok, err := SelectTiDBRowID(db, database, table)
		if err != nil {
			return "", err
		}
		if ok {
			return "ORDER BY _tidb_rowid", nil
		} else {
			return "", nil
		}
	}
	pkName, err := GetPrimaryKeyName(db, database, table)
	if err != nil {
		return "", err
	}
	tableContainsPriKey := pkName != ""
	if tableContainsPriKey {
		return fmt.Sprintf("ORDER BY %s", pkName), nil
	}
	return "", nil
}

func SelectTiDBRowID(db *sql.DB, database, table string) (bool, error) {
	const errBadFieldCode = 1054
	tiDBRowIDQuery := fmt.Sprintf("SELECT _tidb_rowid from %s.%s LIMIT 0", database, table)
	_, err := db.Exec(tiDBRowIDQuery)
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, fmt.Sprintf("%d", errBadFieldCode)) {
			return false, nil
		}
		return false, withStack(err)
	}
	return true, nil
}

func GetColumnTypes(db *sql.DB, table string) ([]*sql.ColumnType, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 1", table))
	if err != nil {
		return nil, withStack(err)
	}
	defer rows.Close()
	return rows.ColumnTypes()
}

func GetPrimaryKeyName(db *sql.DB, database, table string) (string, error) {
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
