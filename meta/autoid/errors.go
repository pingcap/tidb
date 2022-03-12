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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package autoid

import (
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/dbterror"
)

// Error instances.
var (
	errInvalidTableID            = dbterror.ClassAutoid.NewStd(mysql.ErrInvalidTableID)
	errInvalidIncrementAndOffset = dbterror.ClassAutoid.NewStd(mysql.ErrInvalidIncrementAndOffset)
	errNotImplemented            = dbterror.ClassAutoid.NewStd(mysql.ErrNotImplemented)
	ErrAutoincReadFailed         = dbterror.ClassAutoid.NewStd(mysql.ErrAutoincReadFailed)
	ErrWrongAutoKey              = dbterror.ClassAutoid.NewStd(mysql.ErrWrongAutoKey)
	ErrInvalidAllocatorType      = dbterror.ClassAutoid.NewStd(mysql.ErrUnknownAllocatorType)
	ErrAutoRandReadFailed        = dbterror.ClassAutoid.NewStd(mysql.ErrAutoRandReadFailed)
)

const (
	// AutoRandomPKisNotHandleErrMsg indicates the auto_random column attribute is defined on a non-primary key column, or the primary key is nonclustered.
	AutoRandomPKisNotHandleErrMsg = "column %s is not the integer primary key, or the primary key is nonclustered"
	// AutoRandomIncompatibleWithAutoIncErrMsg is reported when auto_random and auto_increment are specified on the same column.
	AutoRandomIncompatibleWithAutoIncErrMsg = "auto_random is incompatible with auto_increment"
	// AutoRandomIncompatibleWithDefaultValueErrMsg is reported when auto_random and default are specified on the same column.
	AutoRandomIncompatibleWithDefaultValueErrMsg = "auto_random is incompatible with default"
	// AutoRandomOverflowErrMsg is reported when auto_random is greater than max length of a MySQL data type.
	AutoRandomOverflowErrMsg = "max allowed auto_random shard bits is %d, but got %d on column `%s`"
	// AutoRandomModifyColTypeErrMsg is reported when a user is trying to modify the type of a column specified with auto_random.
	AutoRandomModifyColTypeErrMsg = "modifying the auto_random column type is not supported"
	// AutoRandomAlterErrMsg is reported when a user is trying to add/drop/modify the value of auto_random attribute.
	AutoRandomAlterErrMsg = "adding/dropping/modifying auto_random is not supported"
	// AutoRandomDecreaseBitErrMsg is reported when the auto_random shard bits is decreased.
	AutoRandomDecreaseBitErrMsg = "decreasing auto_random shard bits is not supported"
	// AutoRandomNonPositive is reported then a user specifies a non-positive value for auto_random.
	AutoRandomNonPositive = "the value of auto_random should be positive"
	// AutoRandomAvailableAllocTimesNote is reported when a table containing auto_random is created.
	AutoRandomAvailableAllocTimesNote = "Available implicit allocation times: %d"
	// AutoRandomExplicitInsertDisabledErrMsg is reported when auto_random column value is explicitly specified, but the session var 'allow_auto_random_explicit_insert' is false.
	AutoRandomExplicitInsertDisabledErrMsg = "Explicit insertion on auto_random column is disabled. Try to set @@allow_auto_random_explicit_insert = true."
	// AutoRandomOnNonBigIntColumn is reported when define auto random to non bigint column
	AutoRandomOnNonBigIntColumn = "auto_random option must be defined on `bigint` column, but not on `%s` column"
	// AutoRandomRebaseNotApplicable is reported when alter auto_random base on a non auto_random table.
	AutoRandomRebaseNotApplicable = "alter auto_random_base of a non auto_random table"
	// AutoRandomRebaseOverflow is reported when alter auto_random_base to a value that overflows the incremental bits.
	AutoRandomRebaseOverflow = "alter auto_random_base to %d overflows the incremental bits, max allowed base is %d"
	// AutoRandomAlterAddColumn is reported when adding an auto_random column.
	AutoRandomAlterAddColumn = "unsupported add column '%s' constraint AUTO_RANDOM when altering '%s.%s'"
	// AutoRandomAlterChangeFromAutoInc is reported when the column is changing from a non-auto_increment or a non-primary key.
	AutoRandomAlterChangeFromAutoInc = "auto_random can only be converted from auto_increment clustered primary key"
	// AutoRandomAllocatorNotFound is reported when auto_random ID allocator not found during changing from auto_inc to auto_random.
	AutoRandomAllocatorNotFound = "auto_random ID allocator not found in table '%s.%s'"
)
