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
	"github.com/pingcap/tidb/util/codec"
)

type sqlCacheKey struct {
	schemaVersion int64
	database      string
	sql           string
	hash          []byte
}

// Hash implements Key interface
func (sck *sqlCacheKey) Hash() []byte {
	if sck.hash == nil {
		dbBytes := []byte(sck.database)
		sqlBytes := []byte(sck.sql)

		bufferSize := 8 + len(dbBytes) + len(sqlBytes)
		sck.hash = make([]byte, 0, bufferSize)

		sck.hash = codec.EncodeInt(sck.hash, sck.schemaVersion)
		sck.hash = append(sck.hash, dbBytes...)
		sck.hash = append(sck.hash, sqlBytes...)
	}
	return sck.hash
}

// NewSQLCacheKey creates a new sqlCacheKey object.
func NewSQLCacheKey(schemaVersion int64, database, sql string) Key {
	return &sqlCacheKey{
		schemaVersion: schemaVersion,
		database:      database,
		sql:           sql,
	}
}
