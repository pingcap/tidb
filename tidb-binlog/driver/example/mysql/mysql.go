// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/tidb-binlog/driver/reader"
	pb "github.com/pingcap/tidb/tidb-binlog/proto/go-binlog"
	_ "github.com/pingcap/tidb/types/parser_driver" // for parser driver
	"github.com/pingcap/tidb/util/dbutil"
	"go.uber.org/zap"
)

// a simple example to sync data to mysql

var (
	port     = flag.Int("P", 3306, "port")
	user     = flag.String("u", "root", "user")
	password = flag.String("p", "", "password")
	host     = flag.String("h", "localhost", "host")

	kafkaAddr = flag.String("kafkaAddr", "127.0.0.1:9092", "kafkaAddr like 127.0.0.1:9092,127.0.0.1:9093")
	clusterID = flag.String("clusterID", "", "clusterID")
	topic     = flag.String("topic", "", "topic name to consume binlog, one of topic or clusterID must be set")
	offset    = flag.Int64("offset", sarama.OffsetNewest, "offset")
	commitTS  = flag.Int64("commitTS", 0, "commitTS")
)

func getDB() (db *sql.DB, err error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", *user, *password, *host, *port, "test")
	log.Debug(dsn)

	db, err = sql.Open("mysql", dsn)

	return
}

func main() {
	flag.Parse()

	cfg := &reader.Config{
		KafkaAddr: strings.Split(*kafkaAddr, ","),
		Offset:    *offset,
		CommitTS:  *commitTS,
		Topic:     *topic,
		ClusterID: *clusterID,
	}

	breader, err := reader.NewReader(cfg)
	if err != nil {
		panic(err)
	}

	db, err := getDB()
	if err != nil {
		panic(err)
	}

	for msg := range breader.Messages() {
		log.Debug("recv", zap.Stringer("message", msg.Binlog))
		binlog := msg.Binlog
		sqls, args := toSQL(binlog)

		tx, err := db.Begin()
		if err != nil {
			log.Fatal("begin transcation failed", zap.Error(err))
		}

		for i := 0; i < len(sqls); i++ {
			log.Debug("exec sql", zap.String("sql", sqls[i]), zap.Reflect("args", args[i]))
			_, err = tx.Exec(sqls[i], args[i]...)
			if err != nil {
				// TODO
				_ = tx.Rollback()
				log.Fatal("exec sql failed", zap.Error(err))
			}
		}
		err = tx.Commit()
		if err != nil {
			log.Fatal("commit transcation failed", zap.Error(err))
		}
	}
}

func columnToArg(c *pb.Column) (arg interface{}) {
	if c.GetIsNull() {
		return nil
	}

	if c.Int64Value != nil {
		return c.GetInt64Value()
	}

	if c.Uint64Value != nil {
		return c.GetUint64Value()
	}

	if c.DoubleValue != nil {
		return c.GetDoubleValue()
	}

	if c.BytesValue != nil {
		return c.GetBytesValue()
	}

	return c.GetStringValue()
}

func tableToSQL(table *pb.Table) (sqls []string, sqlArgs [][]interface{}) {
	replace := func(row *pb.Row) {
		sql := fmt.Sprintf("replace into %s", dbutil.TableName(table.GetSchemaName(), table.GetTableName()))

		var names []string
		var placeHolders []string
		for _, c := range table.GetColumnInfo() {
			names = append(names, c.GetName())
			placeHolders = append(placeHolders, "?")
		}
		sql += "(" + strings.Join(names, ",") + ")"
		sql += "values(" + strings.Join(placeHolders, ",") + ")"

		var args []interface{}
		for _, col := range row.GetColumns() {
			args = append(args, columnToArg(col))
		}

		sqls = append(sqls, sql)
		sqlArgs = append(sqlArgs, args)
	}

	constructWhere := func() (sql string, usePK bool) {
		var whereColumns []string
		for _, col := range table.GetColumnInfo() {
			if col.GetIsPrimaryKey() {
				whereColumns = append(whereColumns, col.GetName())
				usePK = true
			}
		}
		// no primary key
		if len(whereColumns) == 0 {
			for _, col := range table.GetColumnInfo() {
				whereColumns = append(whereColumns, col.GetName())
			}
		}

		sql = " where "
		for i, col := range whereColumns {
			if i != 0 {
				sql += " and "
			}

			sql += fmt.Sprintf("%s = ? ", col)
		}

		sql += " limit 1"

		return
	}

	where, usePK := constructWhere()

	for _, mutation := range table.Mutations {
		switch mutation.GetType() {
		case pb.MutationType_Insert:
			replace(mutation.Row)
		case pb.MutationType_Update:
			columnInfo := table.GetColumnInfo()
			sql := fmt.Sprintf("update %s set ", dbutil.TableName(table.GetSchemaName(), table.GetTableName()))
			// construct c1 = ?, c2 = ?...
			for i, col := range columnInfo {
				if i != 0 {
					sql += ","
				}
				sql += fmt.Sprintf("%s = ? ", col.Name)
			}

			sql += where

			row := mutation.Row
			changedRow := mutation.ChangeRow

			var args []interface{}
			// for set
			for _, col := range row.GetColumns() {
				args = append(args, columnToArg(col))
			}

			// for where
			for i, col := range changedRow.GetColumns() {
				if !usePK || columnInfo[i].GetIsPrimaryKey() {
					args = append(args, columnToArg(col))
				}
			}

			sqls = append(sqls, sql)
			sqlArgs = append(sqlArgs, args)

		case pb.MutationType_Delete:
			columnInfo := table.GetColumnInfo()
			where, usePK := constructWhere()

			sql := fmt.Sprintf("delete from %s %s", dbutil.TableName(table.GetSchemaName(), table.GetTableName()), where)

			row := mutation.Row
			var args []interface{}
			for i, col := range row.GetColumns() {
				if !usePK || columnInfo[i].GetIsPrimaryKey() {
					args = append(args, columnToArg(col))
				}
			}

			sqls = append(sqls, sql)
			sqlArgs = append(sqlArgs, args)
		}
	}

	return
}

func isCreateDatabase(sql string) (isCreateDatabase bool, err error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return false, err
	}

	_, isCreateDatabase = stmt.(*ast.CreateDatabaseStmt)

	return
}

func toSQL(binlog *pb.Binlog) ([]string, [][]interface{}) {
	var allSQL []string
	var allArgs [][]interface{}

	switch binlog.GetType() {
	case pb.BinlogType_DDL:
		ddl := binlog.DdlData
		isCreateDatabase, err := isCreateDatabase(string(ddl.DdlQuery))
		if err != nil {
			log.Fatal("parse ddl failed", zap.Error(err))
		}
		if !isCreateDatabase {
			sql := fmt.Sprintf("use %s", ddl.GetSchemaName())
			allSQL = append(allSQL, sql)
			allArgs = append(allArgs, nil)
		}
		allSQL = append(allSQL, string(ddl.DdlQuery))
		allArgs = append(allArgs, nil)

	case pb.BinlogType_DML:
		dml := binlog.DmlData
		for _, table := range dml.GetTables() {
			sqls, sqlArgs := tableToSQL(table)
			allSQL = append(allSQL, sqls...)
			allArgs = append(allArgs, sqlArgs...)
		}

	default:
		log.Fatal("unknown type", zap.Stringer("type", binlog.GetType()))
	}

	return allSQL, allArgs
}
