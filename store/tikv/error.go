// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

var (
	// errBodyMissing response body is missing error
	errBodyMissing   = errors.New("response body is missing")
	errMayFallBehind = errors.New("start timestamp may fall behind safe point")
	errFallBehind    = errors.New("start timestamp fall behind safe point")
)

// TiDB decides whether to retry transaction by checking if error message contains
// string "try again later" literally.
// In TiClient we use `errors.Annotate(err, txnRetryableMark)` to direct TiDB to
// restart a transaction.
// Note that it should be only used if i) the error occurs inside a transaction
// and ii) the error is not totally unexpected and hopefully will recover soon.
const txnRetryableMark = "[try again later]"

// MySQL error instances.
var (
	ErrTiKVServerTimeout  = terror.ClassTiKV.New(mysql.ErrTiKVServerTimeout, mysql.MySQLErrName[mysql.ErrTiKVServerTimeout]+txnRetryableMark)
	ErrResolveLockTimeout = terror.ClassTiKV.New(mysql.ErrResolveLockTimeout, mysql.MySQLErrName[mysql.ErrResolveLockTimeout]+txnRetryableMark)
	ErrPDServerTimeout    = terror.ClassTiKV.New(mysql.ErrPDServerTimeout, mysql.MySQLErrName[mysql.ErrPDServerTimeout]+txnRetryableMark)
	ErrRegionUnavaiable   = terror.ClassTiKV.New(mysql.ErrRegionUnavaiable, mysql.MySQLErrName[mysql.ErrRegionUnavaiable]+txnRetryableMark)
	ErrTiKVServerBusy     = terror.ClassTiKV.New(mysql.ErrTiKVServerBusy, mysql.MySQLErrName[mysql.ErrTiKVServerBusy]+txnRetryableMark)
)

func init() {
	tikvMySQLErrCodes := map[terror.ErrCode]uint16{
		mysql.ErrTiKVServerTimeout:  mysql.ErrTiKVServerTimeout,
		mysql.ErrResolveLockTimeout: mysql.ErrResolveLockTimeout,
		mysql.ErrPDServerTimeout:    mysql.ErrPDServerTimeout,
		mysql.ErrRegionUnavaiable:   mysql.ErrRegionUnavaiable,
		mysql.ErrTiKVServerBusy:     mysql.ErrTiKVServerBusy,
	}
	terror.ErrClassToMySQLCodes[terror.ClassTiKV] = tikvMySQLErrCodes
}
