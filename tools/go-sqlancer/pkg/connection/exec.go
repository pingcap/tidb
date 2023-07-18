// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connection

import (
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
)

// Select run select statement and return query result
func (c *Connection) Select(stmt string, args ...interface{}) ([]QueryItems, error) {
	start := time.Now()
	rows, err := c.db.Query(stmt, args...)
	if err != nil {
		c.logSQL(stmt, time.Since(start), err)
		return []QueryItems{}, err
	}
	if rows.Err() != nil {
		c.logSQL(stmt, time.Since(start), rows.Err())
		return []QueryItems{}, rows.Err()
	}

	columnTypes, _ := rows.ColumnTypes()
	var result []QueryItems

	for rows.Next() {
		var (
			rowResultSets []interface{}
			resultRow     QueryItems
		)
		for range columnTypes {
			rowResultSets = append(rowResultSets, new(interface{}))
		}
		if err := rows.Scan(rowResultSets...); err != nil {
			log.Error(err.Error())
		}
		for index, resultItem := range rowResultSets {
			r := *resultItem.(*interface{})
			item := QueryItem{
				ValType: columnTypes[index],
			}
			if r != nil {
				bytes := r.([]byte)
				item.ValString = string(bytes)
			} else {
				item.Null = true
			}
			resultRow = append(resultRow, &item)
		}
		result = append(result, resultRow)
	}

	// TODO: make args and stmt together
	c.logSQL(stmt, time.Since(start), nil)
	return result, nil
}

// Update run update statement and return error
func (c *Connection) Update(stmt string) (int64, error) {
	var affectedRows int64
	start := time.Now()
	result, err := c.db.Exec(stmt)
	if err == nil {
		affectedRows, _ = result.RowsAffected()
	}
	c.logSQL(stmt, time.Since(start), err, affectedRows)
	return affectedRows, err
}

// Insert run insert statement and return error
func (c *Connection) Insert(stmt string) (int64, error) {
	var affectedRows int64
	start := time.Now()
	result, err := c.db.Exec(stmt)
	if err == nil {
		affectedRows, _ = result.RowsAffected()
	}
	c.logSQL(stmt, time.Since(start), err, affectedRows)
	return affectedRows, err
}

// Delete run delete statement and return error
func (c *Connection) Delete(stmt string) (int64, error) {
	var affectedRows int64
	start := time.Now()
	result, err := c.db.Exec(stmt)
	if err == nil {
		affectedRows, _ = result.RowsAffected()
	}
	c.logSQL(stmt, time.Since(start), err, affectedRows)
	return affectedRows, err
}

// ExecDDL do DDL actions
func (c *Connection) ExecDDL(query string, args ...interface{}) error {
	start := time.Now()
	_, err := c.db.Exec(query, args...)
	c.logSQL(query, time.Since(start), err)
	return err
}

// Exec do any exec actions
func (c *Connection) Exec(stmt string) error {
	_, err := c.db.Exec(stmt)
	return err
}

// Begin a txn
func (c *Connection) Begin() error {
	start := time.Now()
	err := c.db.Begin()
	c.logSQL("BEGIN", time.Since(start), err)
	return err
}

// Commit a txn
func (c *Connection) Commit() error {
	start := time.Now()
	err := c.db.Commit()
	c.logSQL("COMMIT", time.Since(start), err)
	return err
}

// Rollback a txn
func (c *Connection) Rollback() error {
	start := time.Now()
	err := c.db.Rollback()
	c.logSQL("ROLLBACK", time.Since(start), err)
	return err
}

// IfTxn show if in a transaction
func (c *Connection) IfTxn() bool {
	return c.db.IfTxn()
}

// GetBeginTime get the begin time of a transaction
// if not in transaction, return 0
func (c *Connection) GetBeginTime() time.Time {
	return c.db.GetBeginTime()
}

// GeneralLog turn on or turn off general_log in TiDB
func (c *Connection) GeneralLog(v int) error {
	_, err := c.db.Exec(fmt.Sprintf("set @@tidb_general_log=\"%d\"", v))
	return err
}

// ShowDatabases list databases
func (c *Connection) ShowDatabases() ([]string, error) {
	res, err := c.Select("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	var dbs []string
	for _, db := range res {
		if len(db) == 1 {
			dbs = append(dbs, db[0].ValString)
		}
	}
	return dbs, nil
}

// CreateViewBySelect create veiw from select statement
func (c *Connection) CreateViewBySelect(view, selectStmt string, rows int, columns []types.Column) error {
	order := make([]string, 0, len(columns))
	for _, column := range columns {
		order = append(order, column.GetAliasName().String())
	}
	viewStmt := fmt.Sprintf("CREATE VIEW `%s` AS %s ORDER BY %s LIMIT %d, %d", view, selectStmt, strings.Join(order, ", "), util.Rd(rows), util.RdRange(5, 15))
	_, err := c.db.Exec(viewStmt)
	return err
}
