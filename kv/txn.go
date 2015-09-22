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
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/util/errors2"
)

// IsRetryableError check if the err is not a fatal error and the under going operation is worth to retried.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	if errors2.ErrorEqual(err, ErrLockConflict) || errors2.ErrorEqual(err, ErrConditionNotMatch) {
		return true
	}

	return false
}

// RunInNewTxn will run the f in a new transaction evnironment.
func RunInNewTxn(store Storage, retryable bool, f func(txn Transaction) error) error {
	for {
		txn, err := store.Begin()
		if err != nil {
			log.Error(err)
			return errors.Trace(err)
		}

		err = f(txn)
		if retryable && IsRetryableError(err) {
			log.Warnf("Retry txn %v", txn)
			txn.Rollback()
			continue
		}
		if err != nil {
			return errors.Trace(err)
		}

		_, err = txn.Commit()
		if retryable && IsRetryableError(err) {
			log.Warnf("Retry txn %v", txn)
			txn.Rollback()
			continue
		}
		if err != nil {
			return errors.Trace(err)
		}
		break
	}

	return nil
}
