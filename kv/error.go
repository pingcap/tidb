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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
)

// KV error codes.
const (
	codeClosed          terror.ErrCode = 1
	codeNotExist                       = 2
	codeTxnRetryable                   = 6
	codeCantSetNilValue                = 7
	codeInvalidTxn                     = 8
	codeNotImplemented                 = 10
	codeTxnTooLarge                    = 11
	codeEntryTooLarge                  = 12
	codeKeyExists                      = 1062
)

// TxnRetryableMark is used to uniform the commit error messages which could retry the transaction.
// *WARNING*: changing this string will affect the backward compatibility.
const TxnRetryableMark = "[try again later]"

var (
	// ErrClosed is used when close an already closed txn.
	ErrClosed = terror.ClassKV.New(codeClosed, "Error: Transaction already closed")
	// ErrNotExist is used when try to get an entry with an unexist key from KV store.
	ErrNotExist = terror.ClassKV.New(codeNotExist, "Error: key not exist")
	// ErrTxnRetryable is used when KV store occurs retryable error which SQL layer can safely retry the transaction.
	// When using TiKV as the storage node, the error is returned ONLY when lock not found (txnLockNotFound) in Commit,
	// subject to change it in the future.
	ErrTxnRetryable = terror.ClassKV.New(codeTxnRetryable, "Error: KV error safe to retry %s "+TxnRetryableMark)
	// ErrCannotSetNilValue is the error when sets an empty value.
	ErrCannotSetNilValue = terror.ClassKV.New(codeCantSetNilValue, "can not set nil value")
	// ErrInvalidTxn is the error when commits or rollbacks in an invalid transaction.
	ErrInvalidTxn = terror.ClassKV.New(codeInvalidTxn, "invalid transaction")
	// ErrTxnTooLarge is the error when transaction is too large, lock time reached the maximum value.
	ErrTxnTooLarge = terror.ClassKV.New(codeTxnTooLarge, "transaction is too large")
	// ErrEntryTooLarge is the error when a key value entry is too large.
	ErrEntryTooLarge = terror.ClassKV.New(codeEntryTooLarge, "entry too large, the max entry size is %d, the size of data is %d")
	// ErrKeyExists returns when key is already exist.
	ErrKeyExists = terror.ClassKV.New(codeKeyExists, "key already exist")
	// ErrNotImplemented returns when a function is not implemented yet.
	ErrNotImplemented = terror.ClassKV.New(codeNotImplemented, "not implemented")
	// ErrWriteConflict is the error when the commit meets an write conflict error.
	ErrWriteConflict = terror.ClassKV.New(mysql.ErrWriteConflict,
		mysql.MySQLErrName[mysql.ErrWriteConflict]+" "+TxnRetryableMark)
	// ErrWriteConflictInTiDB is the error when the commit meets an write conflict error when local latch is enabled.
	ErrWriteConflictInTiDB = terror.ClassKV.New(mysql.ErrWriteConflictInTiDB,
		mysql.MySQLErrName[mysql.ErrWriteConflictInTiDB]+" "+TxnRetryableMark)
)

func init() {
	kvMySQLErrCodes := map[terror.ErrCode]uint16{
		codeKeyExists:                mysql.ErrDupEntry,
		codeEntryTooLarge:            mysql.ErrTooBigRowsize,
		codeTxnTooLarge:              mysql.ErrTxnTooLarge,
		mysql.ErrWriteConflict:       mysql.ErrWriteConflict,
		mysql.ErrWriteConflictInTiDB: mysql.ErrWriteConflictInTiDB,
	}
	terror.ErrClassToMySQLCodes[terror.ClassKV] = kvMySQLErrCodes
}

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
