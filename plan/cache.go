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

package plan

import (
	"time"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
)

var (
	// PlanCacheEnabled stores the global config "plan-cache-enabled".
	PlanCacheEnabled bool
	// PlanCacheShards stores the global config "plan-cache-shards".
	PlanCacheShards int64
	// PlanCacheCapacity stores the global config "plan-cache-capacity".
	PlanCacheCapacity int64
	// GlobalPlanCache stores the global plan cache for every session in a tidb-server.
	GlobalPlanCache *kvcache.ShardedLRUCache

	// PreparedPlanCacheEnabled stores the global config "prepared-plan-cache-enabled".
	PreparedPlanCacheEnabled bool
	// PreparedPlanCacheCapacity stores the global config "prepared-plan-cache-capacity".
	PreparedPlanCacheCapacity int64
)

type sqlCacheKey struct {
	user           string
	host           string
	database       string
	sql            string
	snapshot       uint64
	schemaVersion  int64
	sqlMode        mysql.SQLMode
	timezoneOffset int
	readOnly       bool // stores the current tidb-server status.

	hash []byte
}

// Hash implements Key interface.
func (key *sqlCacheKey) Hash() []byte {
	if key.hash == nil {
		var (
			userBytes  = hack.Slice(key.user)
			hostBytes  = hack.Slice(key.host)
			dbBytes    = hack.Slice(key.database)
			sqlBytes   = hack.Slice(key.sql)
			bufferSize = len(userBytes) + len(hostBytes) + len(dbBytes) + len(sqlBytes) + 8*4 + 1
		)

		key.hash = make([]byte, 0, bufferSize)
		key.hash = append(key.hash, userBytes...)
		key.hash = append(key.hash, hostBytes...)
		key.hash = append(key.hash, dbBytes...)
		key.hash = append(key.hash, sqlBytes...)
		key.hash = codec.EncodeInt(key.hash, int64(key.snapshot))
		key.hash = codec.EncodeInt(key.hash, key.schemaVersion)
		key.hash = codec.EncodeInt(key.hash, int64(key.sqlMode))
		key.hash = codec.EncodeInt(key.hash, int64(key.timezoneOffset))
		if key.readOnly {
			key.hash = append(key.hash, '1')
		} else {
			key.hash = append(key.hash, '0')
		}
	}
	return key.hash
}

// NewSQLCacheKey creates a new sqlCacheKey object.
func NewSQLCacheKey(sessionVars *variable.SessionVars, sql string, schemaVersion int64, readOnly bool) kvcache.Key {
	timezoneOffset, user, host := 0, "", ""
	if sessionVars.TimeZone != nil {
		_, timezoneOffset = time.Now().In(sessionVars.TimeZone).Zone()
	}
	if sessionVars.User != nil {
		user = sessionVars.User.Username
		host = sessionVars.User.Hostname
	}

	return &sqlCacheKey{
		user:           user,
		host:           host,
		database:       sessionVars.CurrentDB,
		sql:            sql,
		snapshot:       sessionVars.SnapshotTS,
		schemaVersion:  schemaVersion,
		sqlMode:        sessionVars.SQLMode,
		timezoneOffset: timezoneOffset,
		readOnly:       readOnly,
	}
}

type pstmtPlanCacheKey struct {
	database       string
	connID         uint64
	pstmtID        uint32
	snapshot       uint64
	schemaVersion  int64
	sqlMode        mysql.SQLMode
	timezoneOffset int

	hash []byte
}

// Hash implements Key interface.
func (key *pstmtPlanCacheKey) Hash() []byte {
	if key.hash == nil {
		var (
			dbBytes    = hack.Slice(key.database)
			bufferSize = len(dbBytes) + 8*6
		)
		key.hash = make([]byte, 0, bufferSize)
		key.hash = append(key.hash, dbBytes...)
		key.hash = codec.EncodeInt(key.hash, int64(key.connID))
		key.hash = codec.EncodeInt(key.hash, int64(key.pstmtID))
		key.hash = codec.EncodeInt(key.hash, int64(key.snapshot))
		key.hash = codec.EncodeInt(key.hash, key.schemaVersion)
		key.hash = codec.EncodeInt(key.hash, int64(key.sqlMode))
		key.hash = codec.EncodeInt(key.hash, int64(key.timezoneOffset))
	}
	return key.hash
}

// NewPSTMTPlanCacheKey creates a new pstmtPlanCacheKey object.
func NewPSTMTPlanCacheKey(sessionVars *variable.SessionVars, pstmtID uint32, schemaVersion int64) kvcache.Key {
	timezoneOffset := 0
	if sessionVars.TimeZone != nil {
		_, timezoneOffset = time.Now().In(sessionVars.TimeZone).Zone()
	}
	return &pstmtPlanCacheKey{
		database:       sessionVars.CurrentDB,
		connID:         sessionVars.ConnectionID,
		pstmtID:        pstmtID,
		snapshot:       sessionVars.SnapshotTS,
		schemaVersion:  schemaVersion,
		sqlMode:        sessionVars.SQLMode,
		timezoneOffset: timezoneOffset,
	}
}

// SQLCacheValue stores the cached Statement and StmtNode.
type SQLCacheValue struct {
	StmtNode  ast.StmtNode
	Plan      Plan
	Expensive bool
}

// NewSQLCacheValue creates a SQLCacheValue.
func NewSQLCacheValue(ast ast.StmtNode, plan Plan, expensive bool) *SQLCacheValue {
	return &SQLCacheValue{
		StmtNode:  ast,
		Plan:      plan,
		Expensive: expensive,
	}
}

// PSTMTPlanCacheValue stores the cached Statement and StmtNode.
type PSTMTPlanCacheValue struct {
	Plan Plan
}

// NewPSTMTPlanCacheValue creates a SQLCacheValue.
func NewPSTMTPlanCacheValue(plan Plan) *PSTMTPlanCacheValue {
	return &PSTMTPlanCacheValue{
		Plan: plan,
	}
}
