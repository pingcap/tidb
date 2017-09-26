// Copyright 2017 PingCAP, Inc.
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

package cache

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/codec"
)

var (
	EnablePlanCache   bool  = true
	PlanCacheCapacity int64 = 1000
)

type sqlCacheKey struct {
	schemaVersion  int64
	sqlMode        mysql.SQLMode
	timeZoneOffset int
	snapshot       uint64
	database       string
	sql            string
	hash           []byte
}

// Hash implements Key interface
func (sck *sqlCacheKey) Hash() []byte {
	if sck.hash == nil {
		dbBytes := []byte(sck.database)
		sqlBytes := []byte(sck.sql)

		bufferSize := 8*4 + len(dbBytes) + len(sqlBytes)
		sck.hash = make([]byte, 0, bufferSize)

		sck.hash = codec.EncodeInt(sck.hash, sck.schemaVersion)
		sck.hash = codec.EncodeInt(sck.hash, int64(sck.sqlMode))
		sck.hash = codec.EncodeInt(sck.hash, int64(sck.timeZoneOffset))
		sck.hash = codec.EncodeInt(sck.hash, int64(sck.snapshot))
		sck.hash = append(sck.hash, dbBytes...)
		sck.hash = append(sck.hash, sqlBytes...)
	}
	return sck.hash
}

// NewSQLCacheKey creates a new sqlCacheKey object.
func NewSQLCacheKey(schemaVersion int64, sqlMode mysql.SQLMode, timeZoneOffset int, snapshot uint64, database, sql string) Key {
	return &sqlCacheKey{
		schemaVersion:  schemaVersion,
		sqlMode:        sqlMode,
		timeZoneOffset: timeZoneOffset,
		snapshot:       snapshot,
		database:       database,
		sql:            sql,
	}
}

type SQLCacheValue struct {
	Stmt ast.Statement
	Ast  ast.StmtNode
}

func NewSQLCacheValue(stmt ast.Statement, ast ast.StmtNode) *SQLCacheValue {
	return &SQLCacheValue{
		Stmt: stmt,
		Ast:  ast,
	}
}
