// Copyright 2016 PingCAP, Inc.
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
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
)

// Lock represents a lock from tikv server.
type Lock struct {
	Key             []byte
	Primary         []byte
	TxnID           uint64
	TTL             uint64
	TxnSize         uint64
	LockType        kvrpcpb.Op
	UseAsyncCommit  bool
	LockForUpdateTS uint64
	MinCommitTS     uint64
}

func (l *Lock) String() string {
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	buf.WriteString("key: ")
	buf.WriteString(hex.EncodeToString(l.Key))
	buf.WriteString(", primary: ")
	buf.WriteString(hex.EncodeToString(l.Primary))
	return fmt.Sprintf("%s, txnStartTS: %d, lockForUpdateTS:%d, minCommitTs:%d, ttl: %d, type: %s, UseAsyncCommit: %t",
		buf.String(), l.TxnID, l.LockForUpdateTS, l.MinCommitTS, l.TTL, l.LockType, l.UseAsyncCommit)
}

// NewLock creates a new *Lock.
func NewLock(l *kvrpcpb.LockInfo) *Lock {
	return &Lock{
		Key:             l.GetKey(),
		Primary:         l.GetPrimaryLock(),
		TxnID:           l.GetLockVersion(),
		TTL:             l.GetLockTtl(),
		TxnSize:         l.GetTxnSize(),
		LockType:        l.LockType,
		UseAsyncCommit:  l.UseAsyncCommit,
		LockForUpdateTS: l.LockForUpdateTs,
		MinCommitTS:     l.MinCommitTs,
	}
}
