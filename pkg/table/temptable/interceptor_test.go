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

package temptable

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/driver/txn"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func incLastByte(key kv.Key) kv.Key {
	key = append([]byte{}, key...)
	key[len(key)-1] += 1
	return key
}

func decLastByte(key kv.Key) kv.Key {
	key = append([]byte{}, key...)
	key[len(key)-1] -= 1
	return key
}

func encodeTableKey(tblID int64, suffix ...byte) kv.Key {
	key := tablecodec.EncodeTablePrefix(tblID)
	key = append(key, suffix...)
	return key
}

func TestGetKeyAccessedTableID(t *testing.T) {
	tbPrefix := tablecodec.TablePrefix()
	prefix0 := encodeTableKey(0)
	prefixMax := encodeTableKey(math.MaxInt64)
	prefixNegative := encodeTableKey(-1)
	prefix1 := encodeTableKey(1)
	prefixA := encodeTableKey(math.MaxInt64 / 2)
	prefixB := encodeTableKey(math.MaxInt64 - 1)

	cases := []struct {
		name       string
		key        kv.Key
		ok         bool
		testSuffix bool
		tbID       int64
	}{
		{name: "empty", key: []byte{}, ok: false},
		{name: "replace1", key: incLastByte(tbPrefix), ok: false},
		{name: "replace2", key: decLastByte(tbPrefix), ok: false},
		// key with not enough id len should not be regard as a valid table id
		{name: "tbPrefix", key: tbPrefix, ok: false},
		{name: "back1", key: prefix1[:len(prefix1)-1], tbID: 1, ok: false},
		{name: "back2", key: prefix1[:len(tbPrefix)+1], tbID: 1, ok: false},
		// table with an id 0 should not be regard as a valid table id
		{name: "prefix0", key: prefix0, testSuffix: true, ok: false},
		// table with id math.MaxInt64 should not regard as a valid table id
		{name: "prefixMax", key: prefixMax, testSuffix: true, ok: false},
		// table with id negative should not regard as a valid table id
		{name: "prefixNegative", key: prefixNegative, testSuffix: true, ok: false},
		// table with id > 0 && id < math.MaxInt64 regard as a valid table id
		{name: "prefix1", key: prefix1, tbID: 1, testSuffix: true, ok: true},
		{name: "prefixA", key: prefixA, tbID: math.MaxInt64 / 2, testSuffix: true, ok: true},
		{name: "prefixB", key: prefixB, tbID: math.MaxInt64 - 1, testSuffix: true, ok: true},
	}

	for _, c := range cases {
		keys := []kv.Key{c.key}
		if c.testSuffix {
			for _, s := range [][]byte{
				{0},
				{1},
				{0xFF},
				codec.EncodeInt(nil, 0),
				codec.EncodeInt(nil, math.MaxInt64/2),
				codec.EncodeInt(nil, math.MaxInt64),
			} {
				newKey := append([]byte{}, c.key...)
				newKey = append(newKey, s...)
				keys = append(keys, newKey)
			}
		}

		for i, key := range keys {
			tblID, ok := getKeyAccessedTableID(key)
			require.Equal(t, c.ok, ok, "%s %d", c.name, i)
			if c.ok {
				require.Equal(t, c.tbID, tblID, "%s %d", c.name, i)
			} else {
				require.Equal(t, int64(0), tblID, "%s %d", c.name, i)
			}
		}
	}
}

func TestGetRangeAccessedTableID(t *testing.T) {
	cases := []struct {
		start kv.Key
		end   kv.Key
		ok    bool
		tbID  int64
	}{
		{
			start: encodeTableKey(1),
			end:   encodeTableKey(1),
			ok:    true,
			tbID:  1,
		},
		{
			start: encodeTableKey(1),
			end:   append(encodeTableKey(1), 0),
			ok:    true,
			tbID:  1,
		},
		{
			start: encodeTableKey(1),
			end:   append(encodeTableKey(1), 0xFF),
			ok:    true,
			tbID:  1,
		},
		{
			start: encodeTableKey(1),
			end:   encodeTableKey(2),
			ok:    true,
			tbID:  1,
		},
		{
			start: tablecodec.TablePrefix(),
			end:   encodeTableKey(1),
			ok:    false,
		},
		{
			start: tablecodec.TablePrefix(),
			end:   nil,
			ok:    false,
		},
		{
			start: encodeTableKey(0),
			end:   encodeTableKey(1),
			ok:    false,
		},
		{
			start: encodeTableKey(0),
			end:   nil,
			ok:    false,
		},
		{
			start: encodeTableKey(1),
			end:   encodeTableKey(5),
			ok:    false,
		},
		{
			start: encodeTableKey(1),
			end:   nil,
			ok:    false,
		},
		{
			start: encodeTableKey(1),
			end:   incLastByte(tablecodec.TablePrefix()),
			ok:    false,
		},
		{
			start: encodeTableKey(1),
			end:   encodeTableKey(1)[:len(encodeTableKey(1))-1],
			ok:    false,
		},
		{
			start: encodeTableKey(1)[:len(encodeTableKey(1))-1],
			end:   encodeTableKey(1),
			ok:    false,
		},
		{
			start: encodeTableKey(math.MaxInt64),
			end:   encodeTableKey(math.MaxInt64, 0),
			ok:    false,
		},
		{
			start: nil,
			end:   nil,
			ok:    false,
		},
		{
			start: nil,
			end:   encodeTableKey(2),
			ok:    false,
		},
	}

	for _, c := range cases {
		tblID, ok := getRangeAccessedTableID(c.start, c.end)
		require.Equal(t, c.ok, ok)
		if ok {
			require.Equal(t, c.tbID, tblID)
		} else {
			require.Equal(t, int64(0), tblID)
		}
	}
}

func TestNotTableRange(t *testing.T) {
	falseCases := [][]kv.Key{
		{nil, nil},
		{nil, encodeTableKey(1, 0)},
		{nil, encodeTableKey(1, 1)},
		{encodeTableKey(1), nil},
		{encodeTableKey(1, 0), nil},
		{encodeTableKey(1), encodeTableKey(1)},
		{encodeTableKey(1), encodeTableKey(1, 0)},
		{encodeTableKey(1), encodeTableKey(1, 1)},
		{encodeTableKey(1), encodeTableKey(2)},
		{encodeTableKey(1), encodeTableKey(2, 0)},
		{encodeTableKey(1), encodeTableKey(2, 1)},
		{encodeTableKey(1, 0), encodeTableKey(1, 1)},
		{encodeTableKey(1), incLastByte(tablecodec.TablePrefix())},
	}

	tablePrefix := tablecodec.TablePrefix()
	trueCases := [][]kv.Key{
		{nil, decLastByte(tablePrefix)},
		{decLastByte(tablePrefix), append(decLastByte(tablePrefix), 1)},
		{incLastByte(tablePrefix), nil},
		{incLastByte(tablePrefix), append(incLastByte(tablePrefix), 1)},
	}

	for _, c := range falseCases {
		require.False(t, notTableRange(c[0], c[1]))
	}

	for _, c := range trueCases {
		require.True(t, notTableRange(c[0], c[1]))
	}
}

func TestGetSessionTemporaryTableKey(t *testing.T) {
	localTempTableData := []*kv.Entry{
		{Key: encodeTableKey(5), Value: []byte("v5")},
		{Key: encodeTableKey(5, 0), Value: []byte("v50")},
		{Key: encodeTableKey(5, 1), Value: []byte("v51")},
		{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
		{Key: encodeTableKey(5, 2), Value: []byte("")},
		{Key: encodeTableKey(5, 3), Value: nil},
	}

	is := newMockedInfoSchema(t).
		AddTable(model.TempTableNone, 1).
		AddTable(model.TempTableGlobal, 3).
		AddTable(model.TempTableLocal, 5)

	normalTb, ok := is.TableByID(1)
	require.True(t, ok)
	require.Equal(t, model.TempTableNone, normalTb.Meta().TempTableType)
	globalTb, ok := is.TableByID(3)
	require.True(t, ok)
	require.Equal(t, model.TempTableGlobal, globalTb.Meta().TempTableType)
	localTb, ok := is.TableByID(5)
	require.True(t, ok)
	require.Equal(t, model.TempTableLocal, localTb.Meta().TempTableType)

	retriever := newMockedRetriever(t).SetAllowedMethod("Get").SetData(localTempTableData)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// test local temporary table should read from session
	cases := append(append([]*kv.Entry{}, localTempTableData...), &kv.Entry{
		// also add a test case for key not exist in retriever
		Key: encodeTableKey(5, 'n'), Value: []byte("non-exist-key"),
	})
	for i, c := range cases {
		val, err := getSessionKey(ctx, localTb.Meta(), retriever, c.Key)
		if len(c.Value) == 0 || string(c.Value) == "non-exist-key" {
			require.True(t, kv.ErrNotExist.Equal(err), i)
			require.Nil(t, val, i)
		} else {
			require.NoError(t, err, i)
			require.Equal(t, c.Value, val, i)
		}
		invokes := retriever.GetInvokes()
		require.Equal(t, 1, len(invokes), i)
		require.Equal(t, "Get", invokes[0].Method, i)
		require.Equal(t, []any{ctx, c.Key}, invokes[0].Args)
		retriever.ResetInvokes()

		// test for nil session
		val, err = getSessionKey(ctx, localTb.Meta(), nil, c.Key)
		require.True(t, kv.ErrNotExist.Equal(err), i)
		require.Nil(t, val, i)
		require.Equal(t, 0, len(retriever.GetInvokes()), i)
	}

	// test global temporary table should return empty data directly
	val, err := getSessionKey(ctx, globalTb.Meta(), retriever, encodeTableKey(3))
	require.True(t, kv.ErrNotExist.Equal(err))
	require.Nil(t, val)
	require.Equal(t, 0, len(retriever.GetInvokes()))

	// test normal table should not be allowed
	val, err = getSessionKey(ctx, normalTb.Meta(), retriever, encodeTableKey(1))
	require.Error(t, err, "Cannot get normal table key from session")
	require.Nil(t, val)
	require.Equal(t, 0, len(retriever.GetInvokes()))

	// test for other errors
	injectedErr := errors.New("err")
	retriever.InjectMethodError("Get", injectedErr)
	val, err = getSessionKey(ctx, localTb.Meta(), retriever, encodeTableKey(5))
	require.Nil(t, val)
	require.Equal(t, injectedErr, err)
}

func TestInterceptorTemporaryTableInfoByID(t *testing.T) {
	is := newMockedInfoSchema(t).
		AddTable(model.TempTableNone, 1, 5).
		AddTable(model.TempTableGlobal, 2, 6).
		AddTable(model.TempTableLocal, 3, 7)

	interceptor := NewTemporaryTableSnapshotInterceptor(is, newMockedRetriever(t))

	// normal table should return nil
	tblInfo, ok := interceptor.temporaryTableInfoByID(1)
	require.False(t, ok)
	require.Nil(t, tblInfo)

	tblInfo, ok = interceptor.temporaryTableInfoByID(5)
	require.False(t, ok)
	require.Nil(t, tblInfo)

	// global temporary table
	tblInfo, ok = interceptor.temporaryTableInfoByID(2)
	require.True(t, ok)
	require.Equal(t, "tb2", tblInfo.Name.O)
	require.Equal(t, model.TempTableGlobal, tblInfo.TempTableType)

	tblInfo, ok = interceptor.temporaryTableInfoByID(6)
	require.True(t, ok)
	require.Equal(t, "tb6", tblInfo.Name.O)
	require.Equal(t, model.TempTableGlobal, tblInfo.TempTableType)

	// local temporary table
	tblInfo, ok = interceptor.temporaryTableInfoByID(3)
	require.True(t, ok)
	require.Equal(t, "tb3", tblInfo.Name.O)
	require.Equal(t, model.TempTableLocal, tblInfo.TempTableType)

	tblInfo, ok = interceptor.temporaryTableInfoByID(7)
	require.True(t, ok)
	require.Equal(t, "tb7", tblInfo.Name.O)
	require.Equal(t, model.TempTableLocal, tblInfo.TempTableType)

	// non exists table
	tblInfo, ok = interceptor.temporaryTableInfoByID(4)
	require.False(t, ok)
	require.Nil(t, tblInfo)

	tblInfo, ok = interceptor.temporaryTableInfoByID(8)
	require.False(t, ok)
	require.Nil(t, tblInfo)
}

func TestInterceptorOnGet(t *testing.T) {
	is := newMockedInfoSchema(t).
		AddTable(model.TempTableNone, 1).
		AddTable(model.TempTableGlobal, 3).
		AddTable(model.TempTableLocal, 5)
	noTempTableData := []*kv.Entry{
		// normal table data
		{Key: encodeTableKey(1), Value: []byte("v1")},
		{Key: encodeTableKey(1, 1), Value: []byte("v11")},
		// no exist table data
		{Key: encodeTableKey(2), Value: []byte("v2")},
		{Key: encodeTableKey(2, 1), Value: []byte("v21")},
		// other data
		{Key: kv.Key("s"), Value: []byte("vs")},
		{Key: kv.Key("s0"), Value: []byte("vs0")},
		{Key: kv.Key("u"), Value: []byte("vu")},
		{Key: kv.Key("u0"), Value: []byte("vu0")},
		{Key: tablecodec.TablePrefix(), Value: []byte("vt")},
		{Key: encodeTableKey(0), Value: []byte("v0")},
		{Key: encodeTableKey(0, 1), Value: []byte("v01")},
		{Key: encodeTableKey(math.MaxInt64), Value: []byte("vm")},
		{Key: encodeTableKey(math.MaxInt64, 1), Value: []byte("vm1")},
	}

	localTempTableData := []*kv.Entry{
		{Key: encodeTableKey(5), Value: []byte("v5")},
		{Key: encodeTableKey(5, 0), Value: []byte("v50")},
		{Key: encodeTableKey(5, 1), Value: []byte("v51")},
		{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
		{Key: encodeTableKey(5, 2), Value: []byte("")},
		{Key: encodeTableKey(5, 3), Value: nil},
	}

	snap := newMockedSnapshot(newMockedRetriever(t).SetAllowedMethod("Get").SetData(noTempTableData))
	retriever := newMockedRetriever(t).SetAllowedMethod("Get").SetData(localTempTableData)
	interceptor := NewTemporaryTableSnapshotInterceptor(is, retriever)
	emptyRetrieverInterceptor := NewTemporaryTableSnapshotInterceptor(is, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// test normal table and no table key should read from snapshot
	cases := append(append([]*kv.Entry{}, noTempTableData...), []*kv.Entry{
		// also add a test case for key not exist in snap
		{Key: encodeTableKey(1, 'n'), Value: []byte("non-exist-key")},
		{Key: encodeTableKey(2, 'n'), Value: []byte("non-exist-key")},
		{Key: kv.Key("sn"), Value: []byte("non-exist-key")},
		{Key: kv.Key("un"), Value: []byte("non-exist-key")},
	}...)
	for i, c := range cases {
		for _, emptyRetriever := range []bool{false, true} {
			inter := interceptor
			if emptyRetriever {
				inter = emptyRetrieverInterceptor
			}
			val, err := inter.OnGet(ctx, snap, c.Key)
			if string(c.Value) == "non-exist-key" {
				require.True(t, kv.ErrNotExist.Equal(err), i)
				require.Nil(t, val, i)
			} else {
				require.NoError(t, err, i)
				require.Equal(t, c.Value, val, i)
			}
			require.Equal(t, 0, len(retriever.GetInvokes()))
			invokes := snap.GetInvokes()
			require.Equal(t, 1, len(invokes), i)
			require.Equal(t, "Get", invokes[0].Method, i)
			require.Equal(t, []any{ctx, c.Key}, invokes[0].Args)
			snap.ResetInvokes()
		}
	}

	// test global temporary table should return kv.ErrNotExist
	val, err := interceptor.OnGet(ctx, snap, encodeTableKey(3))
	require.True(t, kv.ErrNotExist.Equal(err))
	require.Nil(t, val)
	require.Equal(t, 0, len(retriever.GetInvokes()))
	require.Equal(t, 0, len(snap.GetInvokes()))

	val, err = interceptor.OnGet(ctx, snap, encodeTableKey(3, 1))
	require.True(t, kv.ErrNotExist.Equal(err))
	require.Nil(t, val)
	require.Equal(t, 0, len(retriever.GetInvokes()))
	require.Equal(t, 0, len(snap.GetInvokes()))

	val, err = emptyRetrieverInterceptor.OnGet(ctx, snap, encodeTableKey(3, 1))
	require.True(t, kv.ErrNotExist.Equal(err))
	require.Nil(t, val)
	require.Equal(t, 0, len(retriever.GetInvokes()))
	require.Equal(t, 0, len(snap.GetInvokes()))

	// test local temporary table should read from session
	cases = append(append([]*kv.Entry{}, localTempTableData...), &kv.Entry{
		// also add a test case for key not exist in retriever
		Key: encodeTableKey(5, 'n'), Value: []byte("non-exist-key"),
	})
	for i, c := range cases {
		val, err = interceptor.OnGet(ctx, snap, c.Key)
		if len(c.Value) == 0 || string(c.Value) == "non-exist-key" {
			require.True(t, kv.ErrNotExist.Equal(err), i)
			require.Nil(t, val, i)
		} else {
			require.NoError(t, err, i)
			require.Equal(t, c.Value, val, i)
		}
		require.Equal(t, 0, len(snap.GetInvokes()), i)
		invokes := retriever.GetInvokes()
		require.Equal(t, 1, len(invokes), i)
		require.Equal(t, "Get", invokes[0].Method, i)
		require.Equal(t, []any{ctx, c.Key}, invokes[0].Args)
		retriever.ResetInvokes()

		val, err = emptyRetrieverInterceptor.OnGet(ctx, snap, c.Key)
		require.True(t, kv.ErrNotExist.Equal(err))
		require.Nil(t, val)
		require.Equal(t, 0, len(snap.GetInvokes()), i)
		require.Equal(t, 0, len(retriever.GetInvokes()), i)
	}

	// test error cases
	injectedErr := errors.New("err1")
	snap.InjectMethodError("Get", injectedErr)
	val, err = interceptor.OnGet(ctx, snap, encodeTableKey(1))
	require.Nil(t, val)
	require.Equal(t, injectedErr, err)
	require.Equal(t, 0, len(retriever.GetInvokes()))
	require.Equal(t, 1, len(snap.GetInvokes()))

	val, err = interceptor.OnGet(ctx, snap, kv.Key("s"))
	require.Nil(t, val)
	require.Equal(t, injectedErr, err)
	require.Equal(t, 0, len(retriever.GetInvokes()))
	require.Equal(t, 2, len(snap.GetInvokes()))
	snap.ResetInvokes()
	require.Equal(t, 0, len(snap.GetInvokes()))

	injectedErr = errors.New("err2")
	retriever.InjectMethodError("Get", injectedErr)
	val, err = interceptor.OnGet(ctx, snap, encodeTableKey(5))
	require.Nil(t, val)
	require.Equal(t, injectedErr, err)
	require.Equal(t, 0, len(snap.GetInvokes()))
	require.Equal(t, 1, len(retriever.GetInvokes()))
	retriever.ResetInvokes()
	require.Equal(t, 0, len(retriever.GetInvokes()))
}

func TestInterceptorBatchGetTemporaryTableKeys(t *testing.T) {
	localTempTableData := []*kv.Entry{
		{Key: encodeTableKey(5), Value: []byte("v5")},
		{Key: encodeTableKey(5, 0), Value: []byte("v50")},
		{Key: encodeTableKey(5, 1), Value: []byte("v51")},
		{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
		{Key: encodeTableKey(5, 2), Value: []byte("")},
		{Key: encodeTableKey(5, 3), Value: nil},
		{Key: encodeTableKey(8), Value: []byte("v8")},
		{Key: encodeTableKey(8, 0), Value: []byte("v80")},
		{Key: encodeTableKey(8, 1), Value: []byte("v81")},
		{Key: encodeTableKey(8, 0, 1), Value: []byte("v801")},
		{Key: encodeTableKey(8, 2), Value: []byte("")},
		{Key: encodeTableKey(8, 3), Value: nil},
	}

	is := newMockedInfoSchema(t).
		AddTable(model.TempTableNone, 1, 4).
		AddTable(model.TempTableGlobal, 3, 6).
		AddTable(model.TempTableLocal, 5, 8)
	retriever := newMockedRetriever(t).SetAllowedMethod("Get").SetData(localTempTableData)
	interceptor := NewTemporaryTableSnapshotInterceptor(is, retriever)
	emptyRetrieverInterceptor := NewTemporaryTableSnapshotInterceptor(is, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cases := []struct {
		keys       []kv.Key
		snapKeys   []kv.Key
		nilSession bool
		result     map[string][]byte
	}{
		{
			keys:     nil,
			snapKeys: nil,
			result:   nil,
		},
		{
			keys: []kv.Key{
				encodeTableKey(3),
				encodeTableKey(3, 1),
				encodeTableKey(6),
			},
			snapKeys: nil,
			result:   nil,
		},
		{
			keys: []kv.Key{
				encodeTableKey(3),
				encodeTableKey(5, 'n'),
			},
			snapKeys: nil,
			result:   nil,
		},
		{
			keys: []kv.Key{
				encodeTableKey(5, 'n'),
			},
			snapKeys: nil,
			result:   nil,
		},
		{
			keys: []kv.Key{
				encodeTableKey(0),
				encodeTableKey(1),
				encodeTableKey(2),
				encodeTableKey(math.MaxInt64),
				tablecodec.TablePrefix(),
				kv.Key("s"),
				kv.Key("v"),
			},
			snapKeys: []kv.Key{
				encodeTableKey(0),
				encodeTableKey(1),
				encodeTableKey(2),
				encodeTableKey(math.MaxInt64),
				tablecodec.TablePrefix(),
				kv.Key("s"),
				kv.Key("v"),
			},
			result: nil,
		},
		{
			keys: []kv.Key{
				encodeTableKey(5),
				encodeTableKey(5, 2),
				encodeTableKey(5, 'n'),
				encodeTableKey(8, 1),
			},
			snapKeys: nil,
			result: map[string][]byte{
				string(encodeTableKey(5)):    []byte("v5"),
				string(encodeTableKey(8, 1)): []byte("v81"),
			},
		},
		{
			keys: []kv.Key{
				encodeTableKey(5),
				encodeTableKey(1),
				encodeTableKey(5, 'n'),
				encodeTableKey(8, 1),
			},
			snapKeys: []kv.Key{encodeTableKey(1)},
			result: map[string][]byte{
				string(encodeTableKey(5)):    []byte("v5"),
				string(encodeTableKey(8, 1)): []byte("v81"),
			},
		},
		{
			keys: []kv.Key{
				tablecodec.TablePrefix(),
				encodeTableKey(5),
				encodeTableKey(1),
				encodeTableKey(5, 2),
				encodeTableKey(5, 'n'),
				encodeTableKey(8, 1),
			},
			snapKeys: []kv.Key{tablecodec.TablePrefix(), encodeTableKey(1)},
			result: map[string][]byte{
				string(encodeTableKey(5)):    []byte("v5"),
				string(encodeTableKey(8, 1)): []byte("v81"),
			},
		},
		{
			keys: []kv.Key{
				tablecodec.TablePrefix(),
				encodeTableKey(5),
				encodeTableKey(1),
				encodeTableKey(5, 2),
				encodeTableKey(5, 'n'),
				encodeTableKey(8, 1),
			},
			snapKeys:   []kv.Key{tablecodec.TablePrefix(), encodeTableKey(1)},
			nilSession: true,
			result:     nil,
		},
	}
	for i, c := range cases {
		inter := interceptor
		if c.nilSession {
			inter = emptyRetrieverInterceptor
		}
		snapKeys, result, err := inter.batchGetTemporaryTableKeys(ctx, c.keys)
		require.NoError(t, err, i)
		if c.snapKeys == nil {
			require.Nil(t, snapKeys, i)
		} else {
			require.Equal(t, c.snapKeys, snapKeys, i)
		}

		if c.result == nil {
			require.Nil(t, result, i)
		} else {
			require.Equal(t, c.result, result, i)
		}

		if c.nilSession {
			require.Equal(t, 0, len(retriever.GetInvokes()))
		}

		for j, invoke := range retriever.GetInvokes() {
			require.Equal(t, "Get", invoke.Method, "%d, %d", i, j)
			require.Equal(t, ctx, invoke.Args[0], "%d, %d", i, j)
		}
		retriever.ResetInvokes()
	}

	// test for error occurs
	injectedErr := errors.New("err")
	retriever.InjectMethodError("Get", injectedErr)
	snapKeys, result, err := interceptor.batchGetTemporaryTableKeys(ctx, []kv.Key{
		tablecodec.TablePrefix(),
		encodeTableKey(5),
		encodeTableKey(1),
		encodeTableKey(5, 'n'),
		encodeTableKey(8, 1),
	})
	require.Nil(t, snapKeys)
	require.Nil(t, result)
	require.Equal(t, injectedErr, err)
	retriever.ResetInvokes()
}

func TestInterceptorOnBatchGet(t *testing.T) {
	is := newMockedInfoSchema(t).
		AddTable(model.TempTableNone, 1).
		AddTable(model.TempTableGlobal, 3).
		AddTable(model.TempTableLocal, 5)
	noTempTableData := []*kv.Entry{
		// normal table data
		{Key: encodeTableKey(1), Value: []byte("v1")},
		{Key: encodeTableKey(1, 1), Value: []byte("v11")},
		// no exist table data
		{Key: encodeTableKey(2), Value: []byte("v2")},
		{Key: encodeTableKey(2, 1), Value: []byte("v21")},
		// other data
		{Key: kv.Key("s"), Value: []byte("vs")},
		{Key: kv.Key("s0"), Value: []byte("vs0")},
		{Key: kv.Key("u"), Value: []byte("vu")},
		{Key: kv.Key("u0"), Value: []byte("vu0")},
		{Key: tablecodec.TablePrefix(), Value: []byte("vt")},
		{Key: encodeTableKey(0), Value: []byte("v0")},
		{Key: encodeTableKey(0, 1), Value: []byte("v01")},
		{Key: encodeTableKey(math.MaxInt64), Value: []byte("vm")},
		{Key: encodeTableKey(math.MaxInt64, 1), Value: []byte("vm1")},
	}

	localTempTableData := []*kv.Entry{
		{Key: encodeTableKey(5), Value: []byte("v5")},
		{Key: encodeTableKey(5, 0), Value: []byte("v50")},
		{Key: encodeTableKey(5, 1), Value: []byte("v51")},
		{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
		{Key: encodeTableKey(5, 2), Value: []byte("")},
		{Key: encodeTableKey(5, 3), Value: nil},
	}

	snap := newMockedSnapshot(newMockedRetriever(t).SetAllowedMethod("BatchGet").SetData(noTempTableData))
	retriever := newMockedRetriever(t).SetAllowedMethod("Get").SetData(localTempTableData)
	interceptor := NewTemporaryTableSnapshotInterceptor(is, retriever)
	emptyRetrieverInterceptor := NewTemporaryTableSnapshotInterceptor(is, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cases := []struct {
		keys       []kv.Key
		snapKeys   []kv.Key
		nilSession bool
		result     map[string][]byte
	}{
		{
			keys:     nil,
			snapKeys: nil,
			result:   make(map[string][]byte),
		},
		{
			keys: []kv.Key{
				encodeTableKey(3),
				encodeTableKey(5, 'n'),
			},
			snapKeys: nil,
			result:   make(map[string][]byte),
		},
		{
			keys: []kv.Key{
				encodeTableKey(7),
				encodeTableKey(1, 'n'),
				[]byte("o"),
			},
			snapKeys: []kv.Key{
				encodeTableKey(7),
				encodeTableKey(1, 'n'),
				[]byte("o"),
			},
			result: make(map[string][]byte),
		},
		{
			keys: []kv.Key{
				encodeTableKey(3),
				encodeTableKey(5),
				encodeTableKey(5, 1),
				encodeTableKey(5, 2),
			},
			snapKeys: nil,
			result: map[string][]byte{
				string(encodeTableKey(5)):    []byte("v5"),
				string(encodeTableKey(5, 1)): []byte("v51"),
			},
		},
		{
			keys: []kv.Key{
				encodeTableKey(0),
				encodeTableKey(1),
				encodeTableKey(2),
				encodeTableKey(math.MaxInt64),
				tablecodec.TablePrefix(),
				kv.Key("s"),
				kv.Key("u"),
				encodeTableKey(9),
			},
			snapKeys: []kv.Key{
				encodeTableKey(0),
				encodeTableKey(1),
				encodeTableKey(2),
				encodeTableKey(math.MaxInt64),
				tablecodec.TablePrefix(),
				kv.Key("s"),
				kv.Key("u"),
				encodeTableKey(9),
			},
			result: map[string][]byte{
				string(encodeTableKey(0)):             []byte("v0"),
				string(encodeTableKey(1)):             []byte("v1"),
				string(encodeTableKey(2)):             []byte("v2"),
				string(encodeTableKey(math.MaxInt64)): []byte("vm"),
				string(tablecodec.TablePrefix()):      []byte("vt"),
				"s":                                   []byte("vs"),
				"u":                                   []byte("vu"),
			},
		},
		{
			keys: []kv.Key{
				tablecodec.TablePrefix(),
				encodeTableKey(5),
				encodeTableKey(1),
				encodeTableKey(5, 2),
				encodeTableKey(5, 'n'),
				encodeTableKey(1, 'n'),
			},
			snapKeys: []kv.Key{
				tablecodec.TablePrefix(),
				encodeTableKey(1),
				encodeTableKey(1, 'n'),
			},
			result: map[string][]byte{
				string(tablecodec.TablePrefix()): []byte("vt"),
				string(encodeTableKey(5)):        []byte("v5"),
				string(encodeTableKey(1)):        []byte("v1"),
			},
		},
		{
			keys: []kv.Key{
				tablecodec.TablePrefix(),
				encodeTableKey(5),
				encodeTableKey(1),
				encodeTableKey(5, 2),
				encodeTableKey(5, 'n'),
				encodeTableKey(1, 'n'),
			},
			nilSession: true,
			snapKeys: []kv.Key{
				tablecodec.TablePrefix(),
				encodeTableKey(1),
				encodeTableKey(1, 'n'),
			},
			result: map[string][]byte{
				string(tablecodec.TablePrefix()): []byte("vt"),
				string(encodeTableKey(1)):        []byte("v1"),
			},
		},
	}

	for i, c := range cases {
		inter := interceptor
		if c.nilSession {
			inter = emptyRetrieverInterceptor
		}
		result, err := inter.OnBatchGet(ctx, snap, c.keys)
		require.NoError(t, err, i)
		require.NotNil(t, result, i)
		require.Equal(t, c.result, result, i)
		if c.nilSession {
			require.Equal(t, 0, len(retriever.GetInvokes()))
		}
		for j, invoke := range retriever.GetInvokes() {
			require.Equal(t, "Get", invoke.Method, "%d, %d", i, j)
			require.Equal(t, ctx, invoke.Args[0], "%d, %d", i, j)
		}
		if len(c.snapKeys) > 0 {
			require.Equal(t, 1, len(snap.GetInvokes()), i)
			require.Equal(t, "BatchGet", snap.GetInvokes()[0].Method, i)
			require.Equal(t, ctx, snap.GetInvokes()[0].Args[0], i)
			require.Equal(t, c.snapKeys, snap.GetInvokes()[0].Args[1], i)
		} else {
			require.Equal(t, 0, len(snap.GetInvokes()), i)
		}

		retriever.ResetInvokes()
		snap.ResetInvokes()
	}

	// test session error occurs
	sessionErr := errors.New("errSession")
	retriever.InjectMethodError("Get", sessionErr)
	result, err := interceptor.OnBatchGet(ctx, snap, []kv.Key{encodeTableKey(5)})
	require.Nil(t, result)
	require.Equal(t, sessionErr, err)

	snapErr := errors.New("errSnap")
	snap.InjectMethodError("BatchGet", snapErr)
	result, err = interceptor.OnBatchGet(ctx, snap, []kv.Key{encodeTableKey(1)})
	require.Nil(t, result)
	require.Equal(t, snapErr, err)
}

func TestCreateUnionIter(t *testing.T) {
	retriever := newMockedRetriever(t).SetData([]*kv.Entry{
		{Key: kv.Key("k1"), Value: []byte("v1")},
		{Key: kv.Key("k10"), Value: []byte("")},
		{Key: kv.Key("k11"), Value: []byte("v11")},
		{Key: kv.Key("k5"), Value: []byte("v5")},
	})

	snap := newMockedSnapshot(newMockedRetriever(t).SetData([]*kv.Entry{
		{Key: kv.Key("k2"), Value: []byte("v2")},
		{Key: kv.Key("k20"), Value: []byte("v20")},
		{Key: kv.Key("k21"), Value: []byte("v21")},
	}))

	cases := []struct {
		args    []kv.Key
		reverse bool
		nilSess bool
		nilSnap bool
		result  []kv.Entry
	}{
		{
			args: []kv.Key{kv.Key("k1"), kv.Key("k21")},
			result: []kv.Entry{
				{Key: kv.Key("k1"), Value: []byte("v1")},
				{Key: kv.Key("k11"), Value: []byte("v11")},
				{Key: kv.Key("k2"), Value: []byte("v2")},
				{Key: kv.Key("k20"), Value: []byte("v20")},
			},
		},
		{
			args:    []kv.Key{kv.Key("k21")},
			reverse: true,
			result: []kv.Entry{
				{Key: kv.Key("k20"), Value: []byte("v20")},
				{Key: kv.Key("k2"), Value: []byte("v2")},
				{Key: kv.Key("k11"), Value: []byte("v11")},
				{Key: kv.Key("k1"), Value: []byte("v1")},
			},
		},
		{
			args:    []kv.Key{kv.Key("k1"), kv.Key("k21")},
			nilSnap: true,
			result: []kv.Entry{
				{Key: kv.Key("k1"), Value: []byte("v1")},
				{Key: kv.Key("k11"), Value: []byte("v11")},
			},
		},
		{
			args:    []kv.Key{kv.Key("k21")},
			nilSnap: true,
			reverse: true,
			result: []kv.Entry{
				{Key: kv.Key("k11"), Value: []byte("v11")},
				{Key: kv.Key("k1"), Value: []byte("v1")},
			},
		},
		{
			args:    []kv.Key{kv.Key("k1"), kv.Key("k21")},
			nilSess: true,
			result: []kv.Entry{
				{Key: kv.Key("k2"), Value: []byte("v2")},
				{Key: kv.Key("k20"), Value: []byte("v20")},
			},
		},
		{
			args:    []kv.Key{kv.Key("k21")},
			nilSess: true,
			reverse: true,
			result: []kv.Entry{
				{Key: kv.Key("k20"), Value: []byte("v20")},
				{Key: kv.Key("k2"), Value: []byte("v2")},
			},
		},
	}

	retriever.SetAllowedMethod("Iter", "IterReverse")
	snap.SetAllowedMethod("Iter", "IterReverse")
	for i, c := range cases {
		var iter kv.Iterator
		var err error
		var method string
		var sessArg kv.Retriever
		if !c.nilSess {
			sessArg = retriever
		}

		var snapArg kv.Snapshot
		if !c.nilSnap {
			snapArg = snap
		}

		if c.reverse {
			method = "IterReverse"
			iter, err = createUnionIter(sessArg, snapArg, nil, c.args[0], c.reverse)
		} else {
			method = "Iter"
			iter, err = createUnionIter(sessArg, snapArg, c.args[0], c.args[1], c.reverse)
		}

		require.NoError(t, err, i)

		if c.nilSess && c.nilSnap {
			require.IsType(t, &kv.EmptyIterator{}, iter, i)
		} else if !c.nilSess {
			require.IsType(t, &txn.UnionIter{}, iter, i)
		} else {
			require.IsType(t, &mock.MockedIter{}, iter, i)
		}

		if !c.nilSess {
			require.Equal(t, 1, len(retriever.GetInvokes()), i)
			require.Equal(t, method, retriever.GetInvokes()[0].Method, i)
			require.Equal(t, c.args[0], retriever.GetInvokes()[0].Args[0].(kv.Key), i)
			if !c.reverse {
				require.Equal(t, c.args[1], retriever.GetInvokes()[0].Args[1].(kv.Key), i)
			}
		}

		if !c.nilSnap {
			require.Equal(t, 1, len(snap.GetInvokes()), i)
			require.Equal(t, method, snap.GetInvokes()[0].Method, i)
			require.Equal(t, c.args[0], snap.GetInvokes()[0].Args[0].(kv.Key), i)
			if !c.reverse {
				require.Equal(t, c.args[1], snap.GetInvokes()[0].Args[1].(kv.Key), i)
			}
		}

		result := make([]kv.Entry, 0)
		for iter.Valid() {
			result = append(result, kv.Entry{Key: iter.Key(), Value: iter.Value()})
			require.NoError(t, iter.Next(), i)
		}
		require.Equal(t, c.result, result, i)

		retriever.ResetInvokes()
		snap.ResetInvokes()
	}
}

func TestErrorCreateUnionIter(t *testing.T) {
	retriever := newMockedRetriever(t).SetAllowedMethod("Iter", "IterReverse").SetData([]*kv.Entry{
		{Key: kv.Key("k1"), Value: []byte("")},
	})
	snap := newMockedSnapshot(newMockedRetriever(t).SetAllowedMethod("Iter", "IterReverse").SetData([]*kv.Entry{
		{Key: kv.Key("k1"), Value: []byte("v1")},
	}))

	// test for iter next error
	iterNextErr := errors.New("iterNextErr")
	retriever.InjectMethodError("IterNext", iterNextErr)
	iter, err := createUnionIter(retriever, snap, kv.Key("k1"), kv.Key("k2"), false)
	require.Equal(t, iterNextErr, err)
	require.Nil(t, iter)
	checkCreatedIterClosed(t, retriever, snap, false)
	retriever.ResetInvokes()
	snap.ResetInvokes()

	// test for iter reverse next error
	iterReverseNextErr := errors.New("iterReverseNextErr")
	retriever.InjectMethodError("IterReverseNext", iterReverseNextErr)
	iter, err = createUnionIter(retriever, snap, nil, kv.Key("k2"), true)
	require.Equal(t, iterReverseNextErr, err)
	require.Nil(t, iter)
	checkCreatedIterClosed(t, retriever, snap, true)
	retriever.ResetInvokes()
	snap.ResetInvokes()

	// test for creating session iter error occurs
	sessionIterErr := errors.New("sessionIterErr")
	retriever.InjectMethodError("Iter", sessionIterErr)

	iter, err = createUnionIter(retriever, snap, kv.Key("k1"), kv.Key("k2"), false)
	require.Equal(t, sessionIterErr, err)
	require.Nil(t, iter)
	checkCreatedIterClosed(t, retriever, snap, false)
	retriever.ResetInvokes()
	snap.ResetInvokes()

	iter, err = createUnionIter(retriever, nil, kv.Key("k1"), kv.Key("k2"), false)
	require.Equal(t, sessionIterErr, err)
	require.Nil(t, iter)
	checkCreatedIterClosed(t, retriever, snap, false)
	retriever.ResetInvokes()
	snap.ResetInvokes()

	// test for creating session reverse iter occurs
	sessionIterReverseErr := errors.New("sessionIterReverseErr")
	retriever.InjectMethodError("IterReverse", sessionIterReverseErr)

	iter, err = createUnionIter(retriever, snap, nil, kv.Key("k2"), true)
	require.Equal(t, sessionIterReverseErr, err)
	require.Nil(t, iter)
	checkCreatedIterClosed(t, retriever, snap, true)
	retriever.ResetInvokes()
	snap.ResetInvokes()

	iter, err = createUnionIter(retriever, snap, nil, kv.Key("k2"), true)
	require.Equal(t, sessionIterReverseErr, err)
	require.Nil(t, iter)
	checkCreatedIterClosed(t, retriever, snap, true)
	retriever.ResetInvokes()
	snap.ResetInvokes()

	// test for creating snap iter error occurs
	snapIterErr := errors.New("snapIterError")
	snap.InjectMethodError("Iter", snapIterErr)

	iter, err = createUnionIter(nil, snap, kv.Key("k1"), kv.Key("k2"), false)
	require.Equal(t, snapIterErr, err)
	require.Nil(t, iter)
	checkCreatedIterClosed(t, retriever, snap, false)
	retriever.ResetInvokes()
	snap.ResetInvokes()

	iter, err = createUnionIter(nil, snap, kv.Key("k1"), kv.Key("k2"), false)
	require.Equal(t, snapIterErr, err)
	require.Nil(t, iter)
	checkCreatedIterClosed(t, retriever, snap, false)
	retriever.ResetInvokes()
	snap.ResetInvokes()

	// test for creating snap reverse iter error occurs
	snapIterReverseErr := errors.New("snapIterError")
	snap.InjectMethodError("IterReverse", snapIterReverseErr)

	iter, err = createUnionIter(nil, snap, nil, kv.Key("k2"), true)
	require.Equal(t, snapIterReverseErr, err)
	require.Nil(t, iter)
	checkCreatedIterClosed(t, retriever, snap, true)
	retriever.ResetInvokes()
	snap.ResetInvokes()

	iter, err = createUnionIter(nil, snap, nil, kv.Key("k2"), true)
	require.Equal(t, snapIterReverseErr, err)
	require.Nil(t, iter)
	checkCreatedIterClosed(t, retriever, snap, true)
	retriever.ResetInvokes()
	snap.ResetInvokes()
}

func checkCreatedIterClosed(t *testing.T, retriever *mockedRetriever, snap *mockedSnapshot, reverse bool) {
	method := "Iter"
	if reverse {
		method = "IterReverse"
	}

	require.True(t, len(retriever.GetInvokes()) <= 1)
	for _, invoke := range retriever.GetInvokes() {
		require.Equal(t, method, invoke.Method)
		err := invoke.Ret[1]
		if err == nil {
			require.NotNil(t, invoke.Ret[0])
			iter := invoke.Ret[0].(*mock.MockedIter)
			require.True(t, iter.Closed())
		}
	}

	require.True(t, len(snap.GetInvokes()) <= 1)
	for _, invoke := range snap.GetInvokes() {
		require.Equal(t, method, invoke.Method)
		err := invoke.Ret[1]
		if err == nil {
			require.NotNil(t, invoke.Ret[0])
			iter := invoke.Ret[0].(*mock.MockedIter)
			require.True(t, iter.Closed())
		}
	}
}

func TestIterTable(t *testing.T) {
	is := newMockedInfoSchema(t).
		AddTable(model.TempTableNone, 1).
		AddTable(model.TempTableGlobal, 3).
		AddTable(model.TempTableLocal, 5)

	noTempTableData := []*kv.Entry{
		// normal table data
		{Key: encodeTableKey(1), Value: []byte("v1")},
		{Key: encodeTableKey(1, 1), Value: []byte("v11")},
		// no exist table data
		{Key: encodeTableKey(2), Value: []byte("v2")},
		{Key: encodeTableKey(2, 1), Value: []byte("v21")},
	}

	localTempTableData := []*kv.Entry{
		{Key: encodeTableKey(5), Value: []byte("v5")},
		{Key: encodeTableKey(5, 0), Value: []byte("v50")},
		{Key: encodeTableKey(5, 1), Value: []byte("v51")},
		{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
		{Key: encodeTableKey(5, 2), Value: []byte("")},
		{Key: encodeTableKey(5, 3), Value: nil},
	}
	retriever := newMockedRetriever(t).SetData(localTempTableData).SetAllowedMethod("Iter")
	snap := newMockedSnapshot(newMockedRetriever(t).SetData(noTempTableData).SetAllowedMethod("Iter"))
	interceptor := NewTemporaryTableSnapshotInterceptor(is, retriever)
	emptyRetrieverInterceptor := NewTemporaryTableSnapshotInterceptor(is, nil)

	cases := []struct {
		tblID      int64
		nilSession bool
		args       []kv.Key
		result     []kv.Entry
	}{
		{
			tblID: 1,
			args:  []kv.Key{encodeTableKey(1), encodeTableKey(2)},
			result: []kv.Entry{
				{Key: encodeTableKey(1), Value: []byte("v1")},
				{Key: encodeTableKey(1, 1), Value: []byte("v11")},
			},
		},
		{
			tblID: 1,
			args:  []kv.Key{encodeTableKey(1), encodeTableKey(1, 1)},
			result: []kv.Entry{
				{Key: encodeTableKey(1), Value: []byte("v1")},
			},
		},
		{
			tblID: 2,
			args:  []kv.Key{encodeTableKey(2), encodeTableKey(3)},
			result: []kv.Entry{
				{Key: encodeTableKey(2), Value: []byte("v2")},
				{Key: encodeTableKey(2, 1), Value: []byte("v21")},
			},
		},
		{
			tblID:  3,
			args:   []kv.Key{encodeTableKey(3), encodeTableKey(4)},
			result: []kv.Entry{},
		},
		{
			tblID:  4,
			args:   []kv.Key{encodeTableKey(4), encodeTableKey(5)},
			result: []kv.Entry{},
		},
		{
			tblID: 5,
			args:  []kv.Key{encodeTableKey(5), encodeTableKey(6)},
			result: []kv.Entry{
				{Key: encodeTableKey(5), Value: []byte("v5")},
				{Key: encodeTableKey(5, 0), Value: []byte("v50")},
				{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
				{Key: encodeTableKey(5, 1), Value: []byte("v51")},
			},
		},
		{
			tblID: 5,
			args:  []kv.Key{encodeTableKey(5), encodeTableKey(5, 1)},
			result: []kv.Entry{
				{Key: encodeTableKey(5), Value: []byte("v5")},
				{Key: encodeTableKey(5, 0), Value: []byte("v50")},
				{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
			},
		},
		{
			tblID:      5,
			nilSession: true,
			args:       []kv.Key{encodeTableKey(5), encodeTableKey(5, 1)},
			result:     []kv.Entry{},
		},
	}

	for i, c := range cases {
		inter := interceptor
		if c.nilSession {
			inter = emptyRetrieverInterceptor
		}
		iter, err := inter.iterTable(c.tblID, snap, c.args[0], c.args[1])
		require.NoError(t, err)
		result := make([]kv.Entry, 0, i)
		for iter.Valid() {
			result = append(result, kv.Entry{Key: iter.Key(), Value: iter.Value()})
			require.NoError(t, iter.Next(), i)
		}
		require.Equal(t, c.result, result, i)

		tbl, ok := is.TableByID(c.tblID)
		if !ok || tbl.Meta().TempTableType == model.TempTableNone {
			require.Equal(t, 0, len(retriever.GetInvokes()), i)
			require.Equal(t, 1, len(snap.GetInvokes()), i)
			require.Equal(t, "Iter", snap.GetInvokes()[0].Method)
			require.Equal(t, []any{c.args[0], c.args[1]}, snap.GetInvokes()[0].Args, i)
		}

		if ok && tbl.Meta().TempTableType == model.TempTableGlobal {
			require.Equal(t, 0, len(retriever.GetInvokes()), i)
			require.Equal(t, 0, len(snap.GetInvokes()), i)
		}

		if ok && tbl.Meta().TempTableType == model.TempTableLocal {
			require.Equal(t, 0, len(snap.GetInvokes()), i)
			if c.nilSession {
				require.Equal(t, 0, len(retriever.GetInvokes()), i)
			} else {
				require.Equal(t, 1, len(retriever.GetInvokes()), i)
				require.Equal(t, "Iter", retriever.GetInvokes()[0].Method)
				require.Equal(t, []any{c.args[0], c.args[1]}, retriever.GetInvokes()[0].Args, i)
			}
		}

		snap.ResetInvokes()
		retriever.ResetInvokes()
	}

	// test error for snap
	snapErr := errors.New("snapErr")
	snap.InjectMethodError("Iter", snapErr)
	iter, err := interceptor.iterTable(1, snap, encodeTableKey(1), encodeTableKey(2))
	require.Nil(t, iter)
	require.Equal(t, snapErr, err)
	snap.InjectMethodError("Iter", nil)

	// test error for retriever
	retrieverErr := errors.New("retrieverErr")
	retriever.InjectMethodError("Iter", retrieverErr)
	iter, err = interceptor.iterTable(5, snap, encodeTableKey(5), encodeTableKey(6))
	require.Nil(t, iter)
	require.Equal(t, retrieverErr, err)
}

func TestOnIter(t *testing.T) {
	is := newMockedInfoSchema(t).
		AddTable(model.TempTableNone, 1).
		AddTable(model.TempTableGlobal, 3).
		AddTable(model.TempTableLocal, 5)

	noTempTableData := []*kv.Entry{
		// normal table data
		{Key: encodeTableKey(1), Value: []byte("v1")},
		{Key: encodeTableKey(1, 1), Value: []byte("v11")},
		// no exist table data
		{Key: encodeTableKey(2), Value: []byte("v2")},
		{Key: encodeTableKey(2, 1), Value: []byte("v21")},
		// other data
		{Key: kv.Key("s"), Value: []byte("vs")},
		{Key: kv.Key("s0"), Value: []byte("vs0")},
		{Key: kv.Key("u"), Value: []byte("vu")},
		{Key: kv.Key("u0"), Value: []byte("vu0")},
		{Key: tablecodec.TablePrefix(), Value: []byte("vt")},
		{Key: encodeTableKey(0), Value: []byte("v0")},
		{Key: encodeTableKey(0, 1), Value: []byte("v01")},
		{Key: encodeTableKey(math.MaxInt64), Value: []byte("vm")},
		{Key: encodeTableKey(math.MaxInt64, 1), Value: []byte("vm1")},
	}

	localTempTableData := []*kv.Entry{
		{Key: encodeTableKey(5), Value: []byte("v5")},
		{Key: encodeTableKey(5, 0), Value: []byte("v50")},
		{Key: encodeTableKey(5, 1), Value: []byte("v51")},
		{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
		{Key: encodeTableKey(5, 2), Value: []byte("")},
		{Key: encodeTableKey(5, 3), Value: nil},
	}
	retriever := newMockedRetriever(t).SetData(localTempTableData).SetAllowedMethod("Iter")
	snap := newMockedSnapshot(newMockedRetriever(t).SetData(noTempTableData).SetAllowedMethod("Iter"))
	interceptor := NewTemporaryTableSnapshotInterceptor(is, retriever)
	emptyRetrieverInterceptor := NewTemporaryTableSnapshotInterceptor(is, nil)

	cases := []struct {
		nilSession bool
		args       []kv.Key
		result     []kv.Entry
	}{
		{
			args: []kv.Key{nil, nil},
			result: []kv.Entry{
				{Key: kv.Key("s"), Value: []byte("vs")},
				{Key: kv.Key("s0"), Value: []byte("vs0")},
				{Key: tablecodec.TablePrefix(), Value: []byte("vt")},
				{Key: encodeTableKey(0), Value: []byte("v0")},
				{Key: encodeTableKey(0, 1), Value: []byte("v01")},
				{Key: encodeTableKey(1), Value: []byte("v1")},
				{Key: encodeTableKey(1, 1), Value: []byte("v11")},
				{Key: encodeTableKey(2), Value: []byte("v2")},
				{Key: encodeTableKey(2, 1), Value: []byte("v21")},
				{Key: encodeTableKey(5), Value: []byte("v5")},
				{Key: encodeTableKey(5, 0), Value: []byte("v50")},
				{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
				{Key: encodeTableKey(5, 1), Value: []byte("v51")},
				{Key: encodeTableKey(math.MaxInt64), Value: []byte("vm")},
				{Key: encodeTableKey(math.MaxInt64, 1), Value: []byte("vm1")},
				{Key: kv.Key("u"), Value: []byte("vu")},
				{Key: kv.Key("u0"), Value: []byte("vu0")},
			},
		},
		{
			args: []kv.Key{nil, kv.Key("s0")},
			result: []kv.Entry{
				{Key: kv.Key("s"), Value: []byte("vs")},
			},
		},
		{
			args: []kv.Key{kv.Key("u"), nil},
			result: []kv.Entry{
				{Key: kv.Key("u"), Value: []byte("vu")},
				{Key: kv.Key("u0"), Value: []byte("vu0")},
			},
		},
		{
			args: []kv.Key{encodeTableKey(1), nil},
			result: []kv.Entry{
				{Key: encodeTableKey(1), Value: []byte("v1")},
				{Key: encodeTableKey(1, 1), Value: []byte("v11")},
				{Key: encodeTableKey(2), Value: []byte("v2")},
				{Key: encodeTableKey(2, 1), Value: []byte("v21")},
				{Key: encodeTableKey(5), Value: []byte("v5")},
				{Key: encodeTableKey(5, 0), Value: []byte("v50")},
				{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
				{Key: encodeTableKey(5, 1), Value: []byte("v51")},
				{Key: encodeTableKey(math.MaxInt64), Value: []byte("vm")},
				{Key: encodeTableKey(math.MaxInt64, 1), Value: []byte("vm1")},
				{Key: kv.Key("u"), Value: []byte("vu")},
				{Key: kv.Key("u0"), Value: []byte("vu0")},
			},
		},
		{
			args: []kv.Key{nil, encodeTableKey(1, 1)},
			result: []kv.Entry{
				{Key: kv.Key("s"), Value: []byte("vs")},
				{Key: kv.Key("s0"), Value: []byte("vs0")},
				{Key: tablecodec.TablePrefix(), Value: []byte("vt")},
				{Key: encodeTableKey(0), Value: []byte("v0")},
				{Key: encodeTableKey(0, 1), Value: []byte("v01")},
				{Key: encodeTableKey(1), Value: []byte("v1")},
			},
		},
		{
			args: []kv.Key{encodeTableKey(1), encodeTableKey(2)},
			result: []kv.Entry{
				{Key: encodeTableKey(1), Value: []byte("v1")},
				{Key: encodeTableKey(1, 1), Value: []byte("v11")},
			},
		},
		{
			args: []kv.Key{encodeTableKey(1), encodeTableKey(3)},
			result: []kv.Entry{
				{Key: encodeTableKey(1), Value: []byte("v1")},
				{Key: encodeTableKey(1, 1), Value: []byte("v11")},
				{Key: encodeTableKey(2), Value: []byte("v2")},
				{Key: encodeTableKey(2, 1), Value: []byte("v21")},
			},
		},
		{
			args:   []kv.Key{encodeTableKey(3), encodeTableKey(4)},
			result: []kv.Entry{},
		},
		{
			args: []kv.Key{encodeTableKey(5), encodeTableKey(5, 3)},
			result: []kv.Entry{
				{Key: encodeTableKey(5), Value: []byte("v5")},
				{Key: encodeTableKey(5, 0), Value: []byte("v50")},
				{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
				{Key: encodeTableKey(5, 1), Value: []byte("v51")},
			},
		},
		{
			args:       []kv.Key{nil, nil},
			nilSession: true,
			result: []kv.Entry{
				{Key: kv.Key("s"), Value: []byte("vs")},
				{Key: kv.Key("s0"), Value: []byte("vs0")},
				{Key: tablecodec.TablePrefix(), Value: []byte("vt")},
				{Key: encodeTableKey(0), Value: []byte("v0")},
				{Key: encodeTableKey(0, 1), Value: []byte("v01")},
				{Key: encodeTableKey(1), Value: []byte("v1")},
				{Key: encodeTableKey(1, 1), Value: []byte("v11")},
				{Key: encodeTableKey(2), Value: []byte("v2")},
				{Key: encodeTableKey(2, 1), Value: []byte("v21")},
				{Key: encodeTableKey(math.MaxInt64), Value: []byte("vm")},
				{Key: encodeTableKey(math.MaxInt64, 1), Value: []byte("vm1")},
				{Key: kv.Key("u"), Value: []byte("vu")},
				{Key: kv.Key("u0"), Value: []byte("vu0")},
			},
		},
		{
			args:       []kv.Key{encodeTableKey(5), encodeTableKey(5, 3)},
			nilSession: true,
			result:     []kv.Entry{},
		},
	}

	for i, c := range cases {
		inter := interceptor
		if c.nilSession {
			inter = emptyRetrieverInterceptor
		}

		iter, err := inter.OnIter(snap, c.args[0], c.args[1])
		require.NoError(t, err, i)
		require.NotNil(t, iter, i)
		result := make([]kv.Entry, 0, i)
		for iter.Valid() {
			result = append(result, kv.Entry{Key: iter.Key(), Value: iter.Value()})
			require.NoError(t, iter.Next(), i)
		}
		require.Equal(t, c.result, result, i)
	}

	// test error for snap
	snapErr := errors.New("snapErr")
	snap.InjectMethodError("Iter", snapErr)

	iter, err := interceptor.OnIter(snap, nil, nil)
	require.Nil(t, iter)
	require.Equal(t, snapErr, err)

	iter, err = interceptor.OnIter(snap, nil, kv.Key("o"))
	require.Nil(t, iter)
	require.Equal(t, snapErr, err)

	iter, err = interceptor.OnIter(snap, encodeTableKey(4), encodeTableKey(5))
	require.Nil(t, iter)
	require.Equal(t, snapErr, err)

	snap.InjectMethodError("Iter", nil)

	// test error for retriever
	retrieverErr := errors.New("retrieverErr")
	retriever.InjectMethodError("Iter", retrieverErr)

	iter, err = interceptor.OnIter(snap, nil, nil)
	require.Nil(t, iter)
	require.Equal(t, retrieverErr, err)

	iter, err = interceptor.OnIter(snap, encodeTableKey(5), encodeTableKey(6))
	require.Nil(t, iter)
	require.Equal(t, retrieverErr, err)
}

func TestOnIterReverse(t *testing.T) {
	is := newMockedInfoSchema(t).
		AddTable(model.TempTableNone, 1).
		AddTable(model.TempTableGlobal, 3).
		AddTable(model.TempTableLocal, 5)

	noTempTableData := []*kv.Entry{
		// normal table data
		{Key: encodeTableKey(1), Value: []byte("v1")},
		{Key: encodeTableKey(1, 1), Value: []byte("v11")},
		// no exist table data
		{Key: encodeTableKey(2), Value: []byte("v2")},
		{Key: encodeTableKey(2, 1), Value: []byte("v21")},
		// other data
		{Key: kv.Key("s"), Value: []byte("vs")},
		{Key: kv.Key("s0"), Value: []byte("vs0")},
		{Key: kv.Key("u"), Value: []byte("vu")},
		{Key: kv.Key("u0"), Value: []byte("vu0")},
		{Key: tablecodec.TablePrefix(), Value: []byte("vt")},
		{Key: encodeTableKey(0), Value: []byte("v0")},
		{Key: encodeTableKey(0, 1), Value: []byte("v01")},
		{Key: encodeTableKey(math.MaxInt64), Value: []byte("vm")},
		{Key: encodeTableKey(math.MaxInt64, 1), Value: []byte("vm1")},
	}

	localTempTableData := []*kv.Entry{
		{Key: encodeTableKey(5), Value: []byte("v5")},
		{Key: encodeTableKey(5, 0), Value: []byte("v50")},
		{Key: encodeTableKey(5, 1), Value: []byte("v51")},
		{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
		{Key: encodeTableKey(5, 2), Value: []byte("")},
		{Key: encodeTableKey(5, 3), Value: nil},
	}
	retriever := newMockedRetriever(t).SetData(localTempTableData).SetAllowedMethod("IterReverse")
	snap := newMockedSnapshot(newMockedRetriever(t).SetData(noTempTableData).SetAllowedMethod("IterReverse"))
	interceptor := NewTemporaryTableSnapshotInterceptor(is, retriever)
	emptyRetrieverInterceptor := NewTemporaryTableSnapshotInterceptor(is, nil)

	cases := []struct {
		nilSession bool
		args       []kv.Key
		result     []kv.Entry
	}{
		{
			args: []kv.Key{nil, nil},
			result: []kv.Entry{
				{Key: kv.Key("u0"), Value: []byte("vu0")},
				{Key: kv.Key("u"), Value: []byte("vu")},
				{Key: encodeTableKey(math.MaxInt64, 1), Value: []byte("vm1")},
				{Key: encodeTableKey(math.MaxInt64), Value: []byte("vm")},
				{Key: encodeTableKey(5, 1), Value: []byte("v51")},
				{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
				{Key: encodeTableKey(5, 0), Value: []byte("v50")},
				{Key: encodeTableKey(5), Value: []byte("v5")},
				{Key: encodeTableKey(2, 1), Value: []byte("v21")},
				{Key: encodeTableKey(2), Value: []byte("v2")},
				{Key: encodeTableKey(1, 1), Value: []byte("v11")},
				{Key: encodeTableKey(1), Value: []byte("v1")},
				{Key: encodeTableKey(0, 1), Value: []byte("v01")},
				{Key: encodeTableKey(0), Value: []byte("v0")},
				{Key: tablecodec.TablePrefix(), Value: []byte("vt")},
				{Key: kv.Key("s0"), Value: []byte("vs0")},
				{Key: kv.Key("s"), Value: []byte("vs")},
			},
		},
		{
			args: []kv.Key{kv.Key("u"), nil},
			result: []kv.Entry{
				{Key: encodeTableKey(math.MaxInt64, 1), Value: []byte("vm1")},
				{Key: encodeTableKey(math.MaxInt64), Value: []byte("vm")},
				{Key: encodeTableKey(5, 1), Value: []byte("v51")},
				{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
				{Key: encodeTableKey(5, 0), Value: []byte("v50")},
				{Key: encodeTableKey(5), Value: []byte("v5")},
				{Key: encodeTableKey(2, 1), Value: []byte("v21")},
				{Key: encodeTableKey(2), Value: []byte("v2")},
				{Key: encodeTableKey(1, 1), Value: []byte("v11")},
				{Key: encodeTableKey(1), Value: []byte("v1")},
				{Key: encodeTableKey(0, 1), Value: []byte("v01")},
				{Key: encodeTableKey(0), Value: []byte("v0")},
				{Key: tablecodec.TablePrefix(), Value: []byte("vt")},
				{Key: kv.Key("s0"), Value: []byte("vs0")},
				{Key: kv.Key("s"), Value: []byte("vs")},
			},
		},
		{
			args: []kv.Key{encodeTableKey(5, 0, 1), nil},
			result: []kv.Entry{
				{Key: encodeTableKey(5, 0), Value: []byte("v50")},
				{Key: encodeTableKey(5), Value: []byte("v5")},
				{Key: encodeTableKey(2, 1), Value: []byte("v21")},
				{Key: encodeTableKey(2), Value: []byte("v2")},
				{Key: encodeTableKey(1, 1), Value: []byte("v11")},
				{Key: encodeTableKey(1), Value: []byte("v1")},
				{Key: encodeTableKey(0, 1), Value: []byte("v01")},
				{Key: encodeTableKey(0), Value: []byte("v0")},
				{Key: tablecodec.TablePrefix(), Value: []byte("vt")},
				{Key: kv.Key("s0"), Value: []byte("vs0")},
				{Key: kv.Key("s"), Value: []byte("vs")},
			},
		},
		{
			args: []kv.Key{kv.Key("s0"), nil},
			result: []kv.Entry{
				{Key: kv.Key("s"), Value: []byte("vs")},
			},
		},
		{
			args:       []kv.Key{encodeTableKey(5, 0, 1), nil},
			nilSession: true,
			result: []kv.Entry{
				{Key: encodeTableKey(2, 1), Value: []byte("v21")},
				{Key: encodeTableKey(2), Value: []byte("v2")},
				{Key: encodeTableKey(1, 1), Value: []byte("v11")},
				{Key: encodeTableKey(1), Value: []byte("v1")},
				{Key: encodeTableKey(0, 1), Value: []byte("v01")},
				{Key: encodeTableKey(0), Value: []byte("v0")},
				{Key: tablecodec.TablePrefix(), Value: []byte("vt")},
				{Key: kv.Key("s0"), Value: []byte("vs0")},
				{Key: kv.Key("s"), Value: []byte("vs")},
			},
		},
		{
			args:       []kv.Key{encodeTableKey(5, 0, 1), kv.Key("s0")},
			nilSession: true,
			result: []kv.Entry{
				{Key: encodeTableKey(2, 1), Value: []byte("v21")},
				{Key: encodeTableKey(2), Value: []byte("v2")},
				{Key: encodeTableKey(1, 1), Value: []byte("v11")},
				{Key: encodeTableKey(1), Value: []byte("v1")},
				{Key: encodeTableKey(0, 1), Value: []byte("v01")},
				{Key: encodeTableKey(0), Value: []byte("v0")},
				{Key: tablecodec.TablePrefix(), Value: []byte("vt")},
				{Key: kv.Key("s0"), Value: []byte("vs0")},
			},
		},
		{
			args: []kv.Key{kv.Key("u"), encodeTableKey(5, 0, 1)},
			result: []kv.Entry{
				{Key: encodeTableKey(math.MaxInt64, 1), Value: []byte("vm1")},
				{Key: encodeTableKey(math.MaxInt64), Value: []byte("vm")},
				{Key: encodeTableKey(5, 1), Value: []byte("v51")},
				{Key: encodeTableKey(5, 0, 1), Value: []byte("v501")},
			},
		},
	}

	for i, c := range cases {
		inter := interceptor
		if c.nilSession {
			inter = emptyRetrieverInterceptor
		}

		iter, err := inter.OnIterReverse(snap, c.args[0], c.args[1])
		require.NoError(t, err, i)
		require.NotNil(t, iter, i)
		result := make([]kv.Entry, 0, i)
		for iter.Valid() {
			result = append(result, kv.Entry{Key: iter.Key(), Value: iter.Value()})
			require.NoError(t, iter.Next(), i)
		}
		require.Equal(t, c.result, result, i)
	}

	// test error for snap
	snapErr := errors.New("snapErr")
	snap.InjectMethodError("IterReverse", snapErr)

	iter, err := interceptor.OnIterReverse(snap, nil, nil)
	require.Nil(t, iter)
	require.Equal(t, snapErr, err)

	iter, err = interceptor.OnIterReverse(snap, kv.Key("o"), nil)
	require.Nil(t, iter)
	require.Equal(t, snapErr, err)

	snap.InjectMethodError("IterReverse", nil)

	// test error for retriever
	retrieverErr := errors.New("retrieverErr")
	retriever.InjectMethodError("IterReverse", retrieverErr)

	iter, err = interceptor.OnIterReverse(snap, nil, nil)
	require.Nil(t, iter)
	require.Equal(t, retrieverErr, err)
}
