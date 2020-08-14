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
	"github.com/pingcap/parser/terror"
	mysql "github.com/pingcap/tidb/errno"
)

// TxnRetryableMark is used to uniform the commit error messages which could retry the transaction.
// *WARNING*: changing this string will affect the backward compatibility.
const TxnRetryableMark = "[try again later]"

var (
	// ErrNotExist is used when try to get an entry with an unexist key from KV store.
	ErrNotExist = terror.ClassKV.New(mysql.ErrNotExist, mysql.MySQLErrName[mysql.ErrNotExist])
	// ErrTxnRetryable is used when KV store occurs retryable error which SQL layer can safely retry the transaction.
	// When using TiKV as the storage node, the error is returned ONLY when lock not found (txnLockNotFound) in Commit,
	// subject to change it in the future.
	ErrTxnRetryable = terror.ClassKV.New(mysql.ErrTxnRetryable,
		mysql.MySQLErrName[mysql.ErrTxnRetryable]+TxnRetryableMark)
	// ErrCannotSetNilValue is the error when sets an empty value.
	ErrCannotSetNilValue = terror.ClassKV.New(mysql.ErrCannotSetNilValue, mysql.MySQLErrName[mysql.ErrCannotSetNilValue])
	// ErrInvalidTxn is the error when commits or rollbacks in an invalid transaction.
	ErrInvalidTxn = terror.ClassKV.New(mysql.ErrInvalidTxn, mysql.MySQLErrName[mysql.ErrInvalidTxn])
	// ErrTxnTooLarge is the error when transaction is too large, lock time reached the maximum value.
	ErrTxnTooLarge = terror.ClassKV.New(mysql.ErrTxnTooLarge, mysql.MySQLErrName[mysql.ErrTxnTooLarge])
	// ErrEntryTooLarge is the error when a key value entry is too large.
	ErrEntryTooLarge = terror.ClassKV.New(mysql.ErrEntryTooLarge, mysql.MySQLErrName[mysql.ErrEntryTooLarge])
	// ErrKeyExists returns when key is already exist.
	ErrKeyExists = terror.ClassKV.New(mysql.ErrDupEntry, mysql.MySQLErrName[mysql.ErrDupEntry])
	// ErrNotImplemented returns when a function is not implemented yet.
	ErrNotImplemented = terror.ClassKV.New(mysql.ErrNotImplemented, mysql.MySQLErrName[mysql.ErrNotImplemented])
	// ErrWriteConflict is the error when the commit meets an write conflict error.
	ErrWriteConflict = terror.ClassKV.New(mysql.ErrWriteConflict,
		mysql.MySQLErrName[mysql.ErrWriteConflict]+" "+TxnRetryableMark)
	// ErrWriteConflictInTiDB is the error when the commit meets an write conflict error when local latch is enabled.
	ErrWriteConflictInTiDB = terror.ClassKV.New(mysql.ErrWriteConflictInTiDB,
		mysql.MySQLErrName[mysql.ErrWriteConflictInTiDB]+" "+TxnRetryableMark)
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
