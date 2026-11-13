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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

// countedKeysSnapshot is a snapshot whose every scan yields keys entries,
// and which kills the statement once killAfter of them have been read.
type countedKeysSnapshot struct {
	kv.Snapshot
	keys      int
	killAfter int
	killer    *sqlkiller.SQLKiller
}

func (s *countedKeysSnapshot) Iter(kv.Key, kv.Key) (kv.Iterator, error) {
	return &countedKeysIterator{snapshot: s}, nil
}

type countedKeysIterator struct {
	snapshot *countedKeysSnapshot
	read     int
}

func (it *countedKeysIterator) Valid() bool { return it.read < it.snapshot.keys }
func (*countedKeysIterator) Key() kv.Key    { return nil }
func (*countedKeysIterator) Value() []byte  { return nil }
func (*countedKeysIterator) Close()         {}
func (it *countedKeysIterator) Next() error {
	it.read++
	if it.read == it.snapshot.killAfter {
		it.snapshot.killer.SendKillSignal(sqlkiller.QueryInterrupted)
	}
	return nil
}

// TestFullTextProbeCountKeys covers the count a probe reads: exact below the
// limit, a lower bound at it, and given up, with the killer's error, once the
// statement is killed, before the scan or in the middle of it.
func TestFullTextProbeCountKeys(t *testing.T) {
	killer := &sqlkiller.SQLKiller{}
	snapshot := &countedKeysSnapshot{keys: 5, killer: killer}

	n, saturated, err := countKeys(snapshot, nil, nil, 10, killer)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.False(t, saturated)

	n, saturated, err = countKeys(snapshot, nil, nil, 5, killer)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.False(t, saturated)

	n, saturated, err = countKeys(snapshot, nil, nil, 3, killer)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.True(t, saturated)

	// Killed in the middle of a scan: no partial count is returned.
	snapshot.killAfter = 2
	n, _, err = countKeys(snapshot, nil, nil, 10, killer)
	require.Error(t, err)
	require.Zero(t, n)

	// Still killed: the next scan is not started.
	snapshot.killAfter = 0
	snapshot.keys = 0
	_, _, err = countKeys(snapshot, nil, nil, 10, killer)
	require.Error(t, err)
}
