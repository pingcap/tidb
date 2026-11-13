// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"errors"
	"testing"

	tikverr "github.com/tikv/client-go/v2/error"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
)

func TestPrototypeNamespacesAndEncoding(t *testing.T) {
	p := newPlan(1)
	next := newPlan(2)
	for route := int64(0); route < 200; route++ {
		q, err := p.target(route)
		require.NoError(t, err)
		parent, err := sourcePID(route)
		require.NoError(t, err)
		require.Equal(t, parent, p.Parents[q])
		require.NotContains(t, next.Parents, q)
	}
	for _, route := range []int64{-1, 200} {
		_, err := p.target(route)
		require.Error(t, err)
	}
	r := row{Route: 76, Value: 5, Identity: 100, Null: true}
	encoded, err := encodeRow(r)
	require.NoError(t, err)
	decoded, err := decodeRow(encoded)
	require.NoError(t, err)
	require.Equal(t, r, decoded)
	require.Greater(t, len(encoded), 500, "exercise default and write CF, not just short values")
	key := recordKey(p.Targets[0], 1)
	require.True(t, allowedKey(p, "record", key))
	require.False(t, allowedKey(p, "record", recordKey(p.Sources[0], 1)))
	require.False(t, allowedKey(next, "record", key))
	require.False(t, allowedKey(p, "local", key))
	require.False(t, allowedKey(p, "record", markerKey(p, p.Targets[0], 1)))
	for _, global := range []bool{false, true} {
		kind := "local"
		if global {
			kind = "global"
		}
		for _, idx := range indexes(p, global) {
			a, err := indexPair(p, p.Targets[0], 1, r, idx)
			require.NoError(t, err)
			b, err := indexPair(p, p.Targets[2], 1, r, idx)
			require.NoError(t, err)
			require.True(t, allowedKey(p, kind, a.Key))
			require.False(t, bytes.Equal(a.Key, b.Key), "NULL keys distinguish repeated handles in different partitions")
			if global {
				require.False(t, allowedKey(next, kind, a.Key))
			}
		}
	}
	require.False(t, allowedKey(p, "global", tablecodec.EncodeIndexSeekKey(logicalID, 9999, nil)))
	q, h, err := markerCoordinates(markerKey(p, p.Targets[0], -5), p)
	require.NoError(t, err)
	require.Equal(t, p.Targets[0], q)
	require.EqualValues(t, -5, h)
	_, _, err = markerCoordinates(markerKey(p, p.Targets[0], 1), next)
	require.Error(t, err)
}

func TestPrototypeExternalSort(t *testing.T) {
	p := newPlan(1)
	im := &importer{dir: t.TempDir(), workers: 3}
	var input []pair
	for i := range 600 {
		b, err := encodeRow(row{Route: int64(i % 50), Value: int64(i)})
		require.NoError(t, err)
		input = append(input, pair{recordKey(p.Targets[i%4], int64(i)), b})
	}
	sorted, err := im.sorted(context.Background(), p, "record", input)
	require.NoError(t, err)
	require.NoError(t, equalPairs(input, sorted))
	require.Greater(t, im.files, 3, "small memory budget must produce multiple sorted runs per writer")
	// Cross-run duplicates must fail too; per-writer duplicate checks alone miss them.
	im.dir = t.TempDir()
	_, err = im.sorted(context.Background(), p, "record", []pair{input[0], input[0]})
	require.ErrorContains(t, err, "duplicate")
	im.dir = t.TempDir()
	_, err = im.sorted(context.Background(), p, "record", []pair{{Key: recordKey(1000, 1), Value: []byte("source")}})
	require.ErrorContains(t, err, "forbidden")
}

func TestPrototypeTempHistory(t *testing.T) {
	p := newPlan(1)
	idx := indexes(p, true)[1]
	r := row{Route: 10, Value: 1, Identity: 777}
	a, err := indexPair(p, p.Targets[0], 7, r, idx)
	require.NoError(t, err)
	b, err := indexPair(p, p.Targets[2], 7, r, idx)
	require.NoError(t, err)
	require.Equal(t, a.Key, b.Key, "distinct unique key is shared across partitions")
	require.NotEqual(t, a.Value, b.Value)
	temp := bytes.Clone(a.Key)
	tablecodec.IndexKey2TempIndexKey(temp)
	ha := kv.NewPartitionHandle(p.Targets[0], kv.IntHandle(7))
	hb := kv.NewPartitionHandle(p.Targets[2], kv.IntHandle(7))
	require.False(t, equalIndexHandles(ha, hb))
	require.False(t, equalIndexHandles(ha, kv.IntHandle(7)))
	del := func(h kv.Handle, version byte) tablecodec.TempIndexValueElem {
		return tablecodec.TempIndexValueElem{Handle: h, Delete: true, Distinct: true, Global: true, KeyVer: version}
	}
	put := func(value []byte, version byte) tablecodec.TempIndexValueElem {
		return tablecodec.TempIndexValueElem{Value: value, Distinct: true, KeyVer: version}
	}
	history := func(elems ...tablecodec.TempIndexValueElem) []byte {
		var raw []byte
		for _, elem := range elems {
			raw = elem.Encode(raw)
		}
		return raw
	}
	cases := []struct {
		name                  string
		origin, raw, expected []byte
		live                  bool
		duplicate             bool
	}{
		{name: "delete old owner and reuse with same inner handle in other PID", origin: a.Value,
			raw: history(del(ha, 'b'), put(b.Value, 'b')), expected: b.Value},
		{name: "late old delete preserves live replacement owner", origin: b.Value,
			raw: history(del(ha, 'b')), expected: b.Value, live: true},
		{name: "delete mismatched vanished owner", origin: a.Value,
			raw: history(del(hb, 'b')), expected: nil},
		{name: "m version already applied preserves replacement", origin: b.Value,
			raw: history(del(ha, 'b'), put(b.Value, 'b'), put(b.Value, 'm')), expected: b.Value, live: true},
		{name: "m delete overwrites b put", origin: nil,
			raw: history(put(b.Value, 'b'), del(hb, 'm')), expected: nil},
		{name: "delete-only event is ignored", origin: a.Value,
			raw: history(del(ha, 'd')), expected: a.Value},
		{name: "unreleased unique owner is rejected", origin: a.Value,
			raw: history(put(b.Value, 'b')), duplicate: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := applyTempHistory(temp, tc.raw, tc.origin, func(h kv.Handle) (bool, error) {
				_, ok := h.(kv.PartitionHandle)
				require.True(t, ok, "global row lookup needs full PID")
				return tc.live, nil
			})
			if tc.duplicate {
				require.ErrorContains(t, err, "duplicate")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
	r.Null = true
	nullEntry, err := indexPair(p, p.Targets[0], 7, r, idx)
	require.NoError(t, err)
	nullKey := bytes.Clone(nullEntry.Key)
	tablecodec.IndexKey2TempIndexKey(nullKey)
	elem := tablecodec.TempIndexValueElem{Delete: true, Distinct: false, KeyVer: 'b'}
	actual, err := applyTempHistory(nullKey, elem.Encode(nil), nullEntry.Value, func(kv.Handle) (bool, error) { t.Fatal("NULL key needs no unique-owner lookup"); return false, nil })
	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestPrototypeLogicalRowOracle(t *testing.T) {
	p := newPlan(1)
	var rows []pair
	add := func(id int64) {
		value, err := encodeRow(row{Route: 10, Identity: id})
		require.NoError(t, err)
		rows = append(rows, pair{recordKey(p.Targets[0], id), value})
	}
	// Intended user rows: identity 1 is deleted, another identity moves, and 33 inserts.
	for id := int64(2); id <= 33; id++ {
		add(id)
	}
	for _, id := range []int64{1001000, 1001001, 1001002, 1001100, 1001101, 1001102} {
		add(id)
	}
	require.NoError(t, verifyLogicalIdentities(rows, 16, p))
	// The original workload accidentally used the moved row's occupied handle
	// for an upsert. Source/shadow equality alone accepted the missing identity 3.
	missing := append([]pair{}, rows[:1]...)
	missing = append(missing, rows[2:]...)
	require.ErrorContains(t, verifyLogicalIdentities(missing, 16, p), "missing identity 3")
	repeated := append(append([]pair{}, rows...), rows[1])
	require.ErrorContains(t, verifyLogicalIdentities(repeated, 16, p), "unexpected/repeated identity 3")
}

func TestPrototypeAppliedSubset(t *testing.T) {
	expected := []pair{{[]byte("a"), []byte("one")}, {[]byte("b"), []byte("two")}}
	require.NoError(t, verifyAppliedSubset(expected, expected[:1]))
	require.NoError(t, verifyAppliedSubset(expected, expected))
	require.Error(t, verifyAppliedSubset(expected, nil))
	require.Error(t, verifyAppliedSubset(expected, []pair{{[]byte("a"), []byte("wrong")}}))
	require.Error(t, verifyAppliedSubset(expected, []pair{{[]byte("c"), []byte("one")}}))
	require.Error(t, verifyAppliedSubset(expected, []pair{expected[0], expected[0]}))
}

func TestPrototypeWriteConflictRetry(t *testing.T) {
	conflict := tikverr.NewErrWriteConflictWithArgs(1, 2, 3, []byte("key"), 0)
	calls := 0
	count, err := retryWriteConflict(context.Background(), func() error {
		calls++
		if calls < 3 {
			return conflict
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, calls)
	require.Equal(t, 2, count)
	unknown := errors.New("undetermined commit result")
	calls = 0
	count, err = retryWriteConflict(context.Background(), func() error { calls++; return unknown })
	require.ErrorIs(t, err, unknown)
	require.Equal(t, 1, calls)
	require.Zero(t, count)
	ctx, cancel := context.WithCancel(context.Background())
	count, err = retryWriteConflict(ctx, func() error { cancel(); return conflict })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, count)
	calls = 0
	_, err = retryWriteConflict(ctx, func() error { calls++; return nil })
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
}
