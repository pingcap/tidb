// Copyright 2015 PingCAP, Inc.
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

package kv

import (
	"strings"

	pmysql "github.com/pingcap/parser/mysql"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/dbterror"
)

// TxnRetryableMark is used to uniform the commit error messages which could retry the transaction.
// *WARNING*: changing this string will affect the backward compatibility.
const TxnRetryableMark = "[try again later]"

var (
	// ErrNotExist is used when try to get an entry with an unexist key from KV store.
	ErrNotExist = dbterror.ClassKV.NewStd(mysql.ErrNotExist)
	// ErrTxnRetryable is used when KV store occurs retryable error which SQL layer can safely retry the transaction.
	// When using TiKV as the storage node, the error is returned ONLY when lock not found (txnLockNotFound) in Commit,
	// subject to change it in the future.
	ErrTxnRetryable = dbterror.ClassKV.NewStdErr(mysql.ErrTxnRetryable,
		pmysql.Message(mysql.MySQLErrName[mysql.ErrTxnRetryable].Raw+TxnRetryableMark, []int{0}))
	// ErrCannotSetNilValue is the error when sets an empty value.
	ErrCannotSetNilValue = dbterror.ClassKV.NewStd(mysql.ErrCannotSetNilValue)
	// ErrInvalidTxn is the error when commits or rollbacks in an invalid transaction.
	ErrInvalidTxn = dbterror.ClassKV.NewStd(mysql.ErrInvalidTxn)
	// ErrTxnTooLarge is the error when transaction is too large, lock time reached the maximum value.
	ErrTxnTooLarge = dbterror.ClassKV.NewStd(mysql.ErrTxnTooLarge)
	// ErrEntryTooLarge is the error when a key value entry is too large.
	ErrEntryTooLarge = dbterror.ClassKV.NewStd(mysql.ErrEntryTooLarge)
	// ErrKeyExists returns when key is already exist.
	ErrKeyExists = dbterror.ClassKV.NewStd(mysql.ErrDupEntry)
	// ErrNotImplemented returns when a function is not implemented yet.
	ErrNotImplemented = dbterror.ClassKV.NewStd(mysql.ErrNotImplemented)
	// ErrWriteConflict is the error when the commit meets an write conflict error.
	ErrWriteConflict = dbterror.ClassKV.NewStdErr(mysql.ErrWriteConflict,
		pmysql.Message(mysql.MySQLErrName[mysql.ErrWriteConflict].Raw+" "+TxnRetryableMark, []int{3}))
	// ErrWriteConflictInTiDB is the error when the commit meets an write conflict error when local latch is enabled.
	ErrWriteConflictInTiDB = dbterror.ClassKV.NewStdErr(mysql.ErrWriteConflictInTiDB,
		pmysql.Message(mysql.MySQLErrName[mysql.ErrWriteConflictInTiDB].Raw+" "+TxnRetryableMark, nil))
	// ErrLockExpire is the error when the lock is expired.
	ErrLockExpire = dbterror.ClassTiKV.NewStd(mysql.ErrLockExpire)
)

// IsTxnRetryableError checks if the error could safely retry the transaction.
func IsTxnRetryableError(err error) bool {
	if err == nil {
		return false
	}

	if ErrTxnRetryable.Equal(err) || ErrWriteConflict.Equal(err) || ErrWriteConflictInTiDB.Equal(err) {
		return true
	}

	return false
}

// IsErrNotFound checks if err is a kind of NotFound error.
func IsErrNotFound(err error) bool {
	return ErrNotExist.Equal(err)
}

// GetDuplicateErrorHandleString is used to concat the handle columns data with '-'.
// This is consistent with MySQL.
func GetDuplicateErrorHandleString(handle Handle) string {
	dt, err := handle.Data()
	if err != nil {
		return err.Error()
	}
	var sb strings.Builder
	for i, d := range dt {
		if i != 0 {
			sb.WriteString("-")
		}
		s, err := d.ToString()
		if err != nil {
			return err.Error()
		}
		sb.WriteString(s)
	}
	return sb.String()
}
