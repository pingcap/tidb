// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

type sharedLockMemBufferForTest struct {
	kv.MemBuffer
	getLocal func(key []byte) ([]byte, error)
	rLocks   int
	rUnlocks int
}

func (m *sharedLockMemBufferForTest) GetLocal(_ context.Context, key []byte) ([]byte, error) {
	return m.getLocal(key)
}

func (m *sharedLockMemBufferForTest) RLock() {
	m.rLocks++
}

func (m *sharedLockMemBufferForTest) RUnlock() {
	m.rUnlocks++
}

type sharedLockTxnForTest struct {
	kv.Transaction
	memBuffer kv.MemBuffer
}

func (t *sharedLockTxnForTest) GetMemBuffer() kv.MemBuffer {
	return t.memBuffer
}

func TestMoveWrittenSharedLockKeysToExclusive(t *testing.T) {
	injectedErr := errors.New("injected get local error")

	tests := []struct {
		name              string
		exclusiveKeys     []kv.Key
		sharedKeys        []kv.Key
		writtenKeys       map[string]struct{}
		getLocalErrors    map[string]error
		wantExclusiveKeys []kv.Key
		wantSharedKeys    []kv.Key
		wantErr           error
	}{
		{
			name:              "no shared keys",
			exclusiveKeys:     []kv.Key{kv.Key("exclusive")},
			wantExclusiveKeys: []kv.Key{kv.Key("exclusive")},
		},
		{
			name:          "deduplicate exclusive and promote written keys",
			exclusiveKeys: []kv.Key{kv.Key("exclusive")},
			sharedKeys: []kv.Key{
				kv.Key("exclusive"),
				kv.Key("written"),
				kv.Key("shared"),
			},
			writtenKeys: map[string]struct{}{
				"written": {},
			},
			wantExclusiveKeys: []kv.Key{kv.Key("exclusive"), kv.Key("written")},
			wantSharedKeys:    []kv.Key{kv.Key("shared")},
		},
		{
			name: "propagate get local error",
			sharedKeys: []kv.Key{
				kv.Key("bad"),
			},
			getLocalErrors: map[string]error{
				"bad": injectedErr,
			},
			wantErr: injectedErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memBuffer := &sharedLockMemBufferForTest{
				getLocal: func(key []byte) ([]byte, error) {
					if err, ok := tt.getLocalErrors[string(key)]; ok {
						return nil, err
					}
					if _, ok := tt.writtenKeys[string(key)]; ok {
						return []byte("value"), nil
					}
					return nil, kv.ErrNotExist
				},
			}
			txn := &sharedLockTxnForTest{memBuffer: memBuffer}

			exclusiveKeys, sharedKeys, err := moveWrittenSharedLockKeysToExclusive(
				context.Background(),
				txn,
				tt.exclusiveKeys,
				tt.sharedKeys,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, exclusiveKeys)
				require.Nil(t, sharedKeys)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantExclusiveKeys, exclusiveKeys)
				require.Equal(t, tt.wantSharedKeys, sharedKeys)
			}
			if len(tt.sharedKeys) > 0 {
				require.Equal(t, 1, memBuffer.rLocks)
				require.Equal(t, 1, memBuffer.rUnlocks)
			} else {
				require.Zero(t, memBuffer.rLocks)
				require.Zero(t, memBuffer.rUnlocks)
			}
		})
	}
}
