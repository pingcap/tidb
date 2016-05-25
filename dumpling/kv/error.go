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

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
)

// KV error codes.
const (
	codeClosed                     terror.ErrCode = 1
	codeNotExist                                  = 2
	codeCondithinNotMatch                         = 3
	codeLockConfilict                             = 4
	codeLazyConditionPairsNotMatch                = 5
	codeRetryable                                 = 6
	codeCantSetNilValue                           = 7
	codeInvalidTxn                                = 8
	codeNotCommitted                              = 9
	codeNotImplemented                            = 10

	codeKeyExists = 1062
)

var (
	// ErrClosed is used when close an already closed txn.
	ErrClosed = terror.ClassKV.New(codeClosed, "Error: Transaction already closed")
	// ErrNotExist is used when try to get an entry with an unexist key from KV store.
	ErrNotExist = terror.ClassKV.New(codeNotExist, "Error: key not exist")
	// ErrConditionNotMatch is used when condition is not met.
	ErrConditionNotMatch = terror.ClassKV.New(codeCondithinNotMatch, "Error: Condition not match")
	// ErrLockConflict is used when try to lock an already locked key.
	ErrLockConflict = terror.ClassKV.New(codeLockConfilict, "Error: Lock conflict")
	// ErrLazyConditionPairsNotMatch is used when value in store differs from expect pairs.
	ErrLazyConditionPairsNotMatch = terror.ClassKV.New(codeLazyConditionPairsNotMatch, "Error: Lazy condition pairs not match")
	// ErrRetryable is used when KV store occurs RPC error or some other
	// errors which SQL layer can safely retry.
	ErrRetryable = terror.ClassKV.New(codeRetryable, "Error: KV error safe to retry")
	// ErrCannotSetNilValue is the error when sets an empty value.
	ErrCannotSetNilValue = terror.ClassKV.New(codeCantSetNilValue, "can not set nil value")
	// ErrInvalidTxn is the error when commits or rollbacks in an invalid transaction.
	ErrInvalidTxn = terror.ClassKV.New(codeInvalidTxn, "invalid transaction")

	// ErrNotCommitted is the error returned by CommitVersion when this
	// transaction is not committed.
	ErrNotCommitted = terror.ClassKV.New(codeNotCommitted, "this transaction has not committed")

	// ErrKeyExists returns when key is already exist.
	ErrKeyExists = terror.ClassKV.New(codeKeyExists, "key already exist")
	// ErrNotImplemented returns when a function is not implemented yet.
	ErrNotImplemented = terror.ClassKV.New(codeNotImplemented, "not implemented")
)

func init() {
	kvMySQLErrCodes := map[terror.ErrCode]uint16{
		codeKeyExists: mysql.ErrDupEntry,
	}
	terror.ErrClassToMySQLCodes[terror.ClassKV] = kvMySQLErrCodes
}

// IsRetryableError checks if the err is a fatal error and the under going operation is worth to retry.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	if terror.ErrorEqual(err, ErrRetryable) ||
		terror.ErrorEqual(err, ErrLockConflict) ||
		terror.ErrorEqual(err, ErrConditionNotMatch) ||
		// HBase exception message will tell you if you should retry or not
		strings.Contains(err.Error(), "try again later") {
		return true
	}

	return false
}

// IsErrNotFound checks if err is a kind of NotFound error.
func IsErrNotFound(err error) bool {
	if terror.ErrorEqual(err, ErrNotExist) {
		return true
	}

	return false
}
