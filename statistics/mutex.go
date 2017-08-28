// Copyright 2017 PingCAP, Inc.
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

package statistics

import (
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"golang.org/x/net/context"
)

// Mutex is a mutex built on etcd.
type Mutex struct {
	s   *concurrency.Session
	key string
}

// NewMutex creates a new Mutex.
func NewMutex(s *concurrency.Session, key string) *Mutex {
	return &Mutex{s, key}
}

// TryToLock will try to lock the mutex. If the mutex is already locked, it will
// not try to hold the lock.
func (m *Mutex) TryToLock(ctx context.Context) (bool, error) {
	client := m.s.Client()

	cmp := clientv3.Compare(clientv3.CreateRevision(m.key), "=", 0)
	put := clientv3.OpPut(m.key, "", clientv3.WithLease(m.s.Lease()))
	resp, err := client.Txn(ctx).If(cmp).Then(put).Commit()
	if err != nil {
		return false, err
	}
	if !resp.Succeeded {
		return false, nil
	}
	return true, nil
}

// Unlock unlocks the mutex.
func (m *Mutex) Unlock(ctx context.Context) error {
	client := m.s.Client()
	if _, err := client.Delete(ctx, m.key); err != nil {
		return err
	}
	return nil
}
