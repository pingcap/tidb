// Copyright 2021 PingCAP, Inc.
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
package txn

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/require"
)

func TestLockNotFoundPrint(t *testing.T) {
	msg := "Txn(Mvcc(TxnLockNotFound { start_ts: 408090278408224772, commit_ts: 408090279311835140, " +
		"key: [116, 128, 0, 0, 0, 0, 0, 50, 137, 95, 105, 128, 0, 0, 0, 0,0 ,0, 1, 1, 67, 49, 57, 48, 57, 50, 57, 48, 255, 48, 48, 48, 48, 48, 52, 56, 54, 255, 50, 53, 53, 50, 51, 0, 0, 0, 252] }))"
	key := prettyLockNotFoundKey(msg)
	expected := "{tableID=12937, indexID=1, indexValues={C19092900000048625523, }}"
	require.Equal(t, expected, key)
}

func TestWriteConflictPrettyFormat(t *testing.T) {
	conflict := &kvrpcpb.WriteConflict{
		StartTs:          399402937522847774,
		ConflictTs:       399402937719455772,
		ConflictCommitTs: 399402937719455773,
		Key:              []byte{116, 128, 0, 0, 0, 0, 0, 1, 155, 95, 105, 128, 0, 0, 0, 0, 0, 0, 1, 1, 82, 87, 48, 49, 0, 0, 0, 0, 251, 1, 55, 54, 56, 50, 50, 49, 49, 48, 255, 57, 0, 0, 0, 0, 0, 0, 0, 248, 1, 0, 0, 0, 0, 0, 0, 0, 0, 247},
		Primary:          []byte{116, 128, 0, 0, 0, 0, 0, 1, 155, 95, 105, 128, 0, 0, 0, 0, 0, 0, 1, 1, 82, 87, 48, 49, 0, 0, 0, 0, 251, 1, 55, 54, 56, 50, 50, 49, 49, 48, 255, 57, 0, 0, 0, 0, 0, 0, 0, 248, 1, 0, 0, 0, 0, 0, 0, 0, 0, 247},
	}

	expectedStr := "[kv:9007]Write conflict, " +
		"txnStartTS=399402937522847774, conflictStartTS=399402937719455772, conflictCommitTS=399402937719455773, " +
		"key={tableID=411, indexID=1, indexValues={RW01, 768221109, , }} " +
		"primary={tableID=411, indexID=1, indexValues={RW01, 768221109, , }} " +
		kv.TxnRetryableMark
	require.EqualError(t, newWriteConflictError(conflict), expectedStr)

	conflict = &kvrpcpb.WriteConflict{
		StartTs:          399402937522847774,
		ConflictTs:       399402937719455772,
		ConflictCommitTs: 399402937719455773,
		Key:              []byte{0x6d, 0x44, 0x42, 0x3a, 0x35, 0x36, 0x0, 0x0, 0x0, 0xfc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x31, 0x30, 0x38, 0x0, 0xfe},
		Primary:          []byte{0x6d, 0x44, 0x42, 0x3a, 0x35, 0x36, 0x0, 0x0, 0x0, 0xfc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x68, 0x54, 0x49, 0x44, 0x3a, 0x31, 0x30, 0x38, 0x0, 0xfe},
	}
	expectedStr = "[kv:9007]Write conflict, " +
		"txnStartTS=399402937522847774, conflictStartTS=399402937719455772, conflictCommitTS=399402937719455773, " +
		"key={metaKey=true, key=DB:56, field=TID:108} " +
		"primary={metaKey=true, key=DB:56, field=TID:108} " +
		kv.TxnRetryableMark
	require.EqualError(t, newWriteConflictError(conflict), expectedStr)
}
