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

package autoid

import (
	pterror "github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

// Error instances.
var (
	errInvalidTableID            = terror.New(pterror.ClassAutoid, mysql.ErrInvalidTableID, mysql.MySQLErrName[mysql.ErrInvalidTableID])
	errInvalidIncrementAndOffset = terror.New(pterror.ClassAutoid, mysql.ErrInvalidIncrementAndOffset, mysql.MySQLErrName[mysql.ErrInvalidIncrementAndOffset])
	ErrAutoincReadFailed         = terror.New(pterror.ClassAutoid, mysql.ErrAutoincReadFailed, mysql.MySQLErrName[mysql.ErrAutoincReadFailed])
	ErrWrongAutoKey              = terror.New(pterror.ClassAutoid, mysql.ErrWrongAutoKey, mysql.MySQLErrName[mysql.ErrWrongAutoKey])
	errInvalidAllocatorType      = terror.New(pterror.ClassAutoid, mysql.ErrUnknownAllocatorType, mysql.MySQLErrName[mysql.ErrUnknownAllocatorType])
	ErrAutoRandReadFailed        = terror.New(pterror.ClassAutoid, mysql.ErrAutoRandReadFailed, mysql.MySQLErrName[mysql.ErrAutoRandReadFailed])
)

func init() {
	// Map error codes to mysql error codes.
	tableMySQLErrCodes := map[pterror.ErrCode]uint16{
		mysql.ErrAutoincReadFailed:         mysql.ErrAutoincReadFailed,
		mysql.ErrWrongAutoKey:              mysql.ErrWrongAutoKey,
		mysql.ErrInvalidTableID:            mysql.ErrInvalidTableID,
		mysql.ErrUnknownAllocatorType:      mysql.ErrUnknownAllocatorType,
		mysql.ErrAutoRandReadFailed:        mysql.ErrAutoRandReadFailed,
		mysql.ErrInvalidIncrementAndOffset: mysql.ErrInvalidIncrementAndOffset,
	}
	terror.ErrClassToMySQLCodes[pterror.ClassAutoid] = tableMySQLErrCodes
}

const (
	// AutoRandomPKisNotHandleErrMsg indicates the auto_random column attribute is defined on a non-primary key column, or the table's primary key is not a single integer column.
	AutoRandomPKisNotHandleErrMsg = "column %s is not the single integer primary key, or alter-primary-key is enabled"
	// AutoRandomExperimentalDisabledErrMsg is reported when the experimental option allow-auto-random is not enabled.
	AutoRandomExperimentalDisabledErrMsg = "auto_random is an experimental feature, which can only be used when allow-auto-random is enabled. This can be changed in the configuration."
	// AutoRandomIncompatibleWithAutoIncErrMsg is reported when auto_random and auto_increment are specified on the same column.
	AutoRandomIncompatibleWithAutoIncErrMsg = "auto_random is incompatible with auto_increment"
	// AutoRandomIncompatibleWithDefaultValueErrMsg is reported when auto_random and default are specified on the same column.
	AutoRandomIncompatibleWithDefaultValueErrMsg = "auto_random is incompatible with default"
	// AutoRandomOverflowErrMsg is reported when auto_random is greater than max length of a MySQL data type.
	AutoRandomOverflowErrMsg = "auto_random = %d will overflow. The max length of bits is %d"
	// AutoRandomModifyColTypeErrMsg is reported when a user is trying to modify the type of a column specified with auto_random.
	AutoRandomModifyColTypeErrMsg = "modifying the auto_random column type is not supported"
	// AutoRandomAlterErrMsg is reported when a user is trying to add/drop/modify the value of auto_random attribute.
	AutoRandomAlterErrMsg = "adding/dropping/modifying auto_random is not supported"
	// AutoRandomNonPositive is reported then a user specifies a non-positive value for auto_random.
	AutoRandomNonPositive = "the value of auto_random should be positive"
)
