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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/logger"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/mysql"
)

// Option struct
type Option struct {
	Name       string
	Log        string
	Mute       bool
	GeneralLog bool
}

// Connection define connection struct
type Connection struct {
	logger *logger.Logger
	db     *mysql.DBConnect
	opt    *Option
}

// New create Connection instance from dsn
func New(dsn string, opt *Option) (*Connection, error) {
	l, err := logger.New(opt.Name, "", opt.Mute)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := mysql.OpenDB(dsn, 1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c := &Connection{
		logger: l,
		db:     db,
		opt:    opt,
	}
	if err := c.Prepare(); err != nil {
		return nil, errors.Trace(err)
	}
	return c, nil
}

// Prepare connection
func (c *Connection) Prepare() error {
	if c.opt.GeneralLog {
		c.GeneralLog(1)
	}
	return nil
}

// CloseDB close connection
func (c *Connection) CloseDB() error {
	return c.db.CloseDB()
}

// ReConnect rebuild connection
func (c *Connection) ReConnect() error {
	if err := c.db.ReConnect(); err != nil {
		return err
	}
	return c.Prepare()
}
