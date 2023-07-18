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

package executor

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/connection"
)

// GetConn get connection of first connection
func (e *Executor) GetConn() *connection.Connection {
	return e.conn
}

// ReConnect rebuild connection
func (e *Executor) ReConnect() error {
	return e.conn.ReConnect()
}

// Close close connection
func (e *Executor) Close() error {
	return e.conn.CloseDB()
}

// Exec function for quick executor some SQLs
func (e *Executor) Exec(sql string) error {
	if err := e.conn.Exec(sql); err != nil {
		return errors.Errorf("sql: %s, err: %+v", sql, err)
	}
	return e.conn.Commit()
}

// ExecIgnoreErr function for quick executor some SQLs with error tolerance
func (e *Executor) ExecIgnoreErr(sql string) {
	_ = e.conn.Exec(sql)
	_ = e.conn.Commit()
}
