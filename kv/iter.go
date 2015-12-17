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

import "github.com/juju/errors"

var (
	// ErrClosed is used when close an already closed txn.
	ErrClosed = errors.New("Error: Transaction already closed")
	// ErrNotExist is used when try to get an entry with an unexist key from KV store.
	ErrNotExist = errors.New("Error: key not exist")
	// ErrConditionNotMatch is used when condition is not met.
	ErrConditionNotMatch = errors.New("Error: Condition not match")
	// ErrLockConflict is used when try to lock an already locked key.
	ErrLockConflict = errors.New("Error: Lock conflict")
	// ErrLazyConditionPairsNotMatch is used when value in store differs from expect pairs.
	ErrLazyConditionPairsNotMatch = errors.New("Error: Lazy condition pairs not match")
	// ErrRetryable is used when KV store occurs RPC error or some other
	// errors which SQL layer can safely retry.
	ErrRetryable = errors.New("Error: KV error safe to retry")
	// ErrCannotSetNilValue is the error when sets an empty value.
	ErrCannotSetNilValue = errors.New("can not set nil value")
	// ErrInvalidTxn is the error when commits or rollbacks in an invalid transaction.
	ErrInvalidTxn = errors.New("invalid transaction")
)

// NextUntil applies FnKeyCmp to each entry of the iterator until meets some condition.
// It will stop when fn returns true, or iterator is invalid or an error occurs.
func NextUntil(it Iterator, fn FnKeyCmp) error {
	var err error
	for it.Valid() && !fn([]byte(it.Key())) {
		err = it.Next()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
