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

package ddl

import (
	"fmt"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/kv"
)

var _ context.Context = &reorgContext{}

// reorgContext implements context.Context interface for reorgnization use.
type reorgContext struct {
	store kv.Storage
	m     map[fmt.Stringer]interface{}
	txn   kv.Transaction
}

func (c *reorgContext) GetTxn(forceNew bool) (kv.Transaction, error) {
	if forceNew {
		if c.txn != nil {
			if err := c.txn.Commit(); err != nil {
				return nil, errors.Trace(err)
			}
			c.txn = nil
		}
	}

	if c.txn != nil {
		return c.txn, nil
	}

	txn, err := c.store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}

	c.txn = txn
	return c.txn, nil
}

func (c *reorgContext) FinishTxn(rollback bool) error {
	if c.txn == nil {
		return nil
	}

	var err error
	if rollback {
		err = c.txn.Rollback()
	} else {
		err = c.txn.Commit()
	}

	c.txn = nil

	return errors.Trace(err)
}

func (c *reorgContext) SetValue(key fmt.Stringer, value interface{}) {
	c.m[key] = value
}

func (c *reorgContext) Value(key fmt.Stringer) interface{} {
	return c.m[key]
}

func (c *reorgContext) ClearValue(key fmt.Stringer) {
	delete(c.m, key)
}

func (d *ddl) newReorgContext() context.Context {
	c := &reorgContext{
		store: d.store,
		m:     make(map[fmt.Stringer]interface{}),
	}

	return c
}

const waitReorgTimeout = 10 * time.Second

var errWaitReorgTimeout = errors.New("wait for reorgnization timeout")

func (d *ddl) runReorgJob(f func() error) error {
	if d.reorgDoneCh == nil {
		// start a reorgnization job
		d.reorgDoneCh = make(chan error, 1)
		go func() {
			d.reorgDoneCh <- f()
		}()
	}

	waitTimeout := chooseLeaseTime(d.lease, waitReorgTimeout)

	// wait reorgnization job done or timeout
	select {
	case err := <-d.reorgDoneCh:
		d.reorgDoneCh = nil
		return errors.Trace(err)
	case <-time.After(waitTimeout):
		// if timeout, we will return, check the owner and retry wait job done again.
		return errWaitReorgTimeout
	case <-d.quitCh:
		// we return errWaitReorgTimeout here too, so that outer loop will break.
		return errWaitReorgTimeout
	}
}

func (d *ddl) delKeysWithPrefix(prefix string) error {
	keys := make([]string, maxBatchSize)

	for {
		keys := keys[0:0]
		err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
			iter, err := txn.Seek([]byte(prefix))
			if err != nil {
				return errors.Trace(err)
			}

			defer iter.Close()
			for i := 0; i < maxBatchSize; i++ {
				if iter.Valid() && strings.HasPrefix(iter.Key(), prefix) {
					keys = append(keys, iter.Key())
					err = iter.Next()
					if err != nil {
						return errors.Trace(err)
					}
				} else {
					break
				}
			}

			for _, key := range keys {
				err := txn.Delete([]byte(key))
				if err != nil {
					return errors.Trace(err)
				}
			}

			return nil
		})

		if err != nil {
			return errors.Trace(err)
		}

		// delete no keys, return.
		if len(keys) == 0 {
			return nil
		}
	}
}
