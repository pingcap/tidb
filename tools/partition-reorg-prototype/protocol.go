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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

const logicalID int64 = 9000

// The deliberately small topology preserves both old boundaries: 100 and 200.
// Parent identity, not handle uniqueness across parents, makes the mapping invertible.
type splitPlan struct {
	Sources    []int64         `json:"sources"`
	Targets    []int64         `json:"targets"`
	Parents    map[int64]int64 `json:"parents"`
	Generation int             `json:"generation"`
}

func newPlan(g int) splitPlan {
	p := splitPlan{Sources: []int64{1000, 1001}, Generation: g, Parents: make(map[int64]int64)}
	for i := range 4 {
		q := int64(2000 + g*10 + i)
		p.Targets = append(p.Targets, q)
		p.Parents[q] = p.Sources[i/2]
	}
	return p
}

func (p splitPlan) target(route int64) (int64, error) {
	if route < 0 || route >= 200 {
		return 0, fmt.Errorf("route %d outside fixed split domain", route)
	}
	return p.Targets[route/50], nil
}

func sourcePID(route int64) (int64, error) {
	if route < 0 || route >= 200 {
		return 0, fmt.Errorf("route %d outside source domain", route)
	}
	return 1000 + route/100, nil
}

type row struct {
	Route    int64
	Value    int64
	Identity int64
	Null     bool
}
type pair struct {
	Key   []byte
	Value []byte
}

func recordKey(pid, h int64) []byte { return tablecodec.EncodeRowKeyWithHandle(pid, kv.IntHandle(h)) }
func markerPrefix(p splitPlan) []byte {
	return []byte(fmt.Sprintf("prototype/g%d/dirty/", p.Generation))
}
func markerKey(p splitPlan, q, h int64) []byte {
	return codec.EncodeInt(codec.EncodeInt(markerPrefix(p), q), h)
}
func gateKey(p splitPlan) []byte {
	return []byte(fmt.Sprintf("prototype/g%d/repair-gate", p.Generation))
}
func stateKey(p splitPlan) []byte { return []byte(fmt.Sprintf("prototype/g%d/state", p.Generation)) }

var layoutKey = []byte("prototype/public-layout")

func encodeRow(r row) ([]byte, error) {
	values := types.MakeDatums(r.Route, r.Value, r.Identity, boolInt(r.Null), strings.Repeat("x", 512), r.Value, r.Identity)
	if r.Null {
		values[1] = types.Datum{}
		values[2] = types.Datum{}
	}
	return tablecodec.EncodeRow(time.UTC, values, []int64{1, 2, 3, 4, 5, 6, 7}, nil, nil, nil, &rowcodec.Encoder{Enable: true})
}
func boolInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}
func decodeRow(b []byte) (row, error) {
	cols := make(map[int64]*types.FieldType)
	for _, id := range []int64{1, 2, 3, 4, 6, 7} {
		cols[id] = types.NewFieldType(mysql.TypeLonglong)
	}
	ds, err := tablecodec.DecodeRowToDatumMap(b, cols, time.UTC)
	if err != nil {
		return row{}, err
	}
	for _, id := range []int64{1, 2, 3, 4, 6, 7} {
		if _, ok := ds[id]; !ok {
			return row{}, fmt.Errorf("missing column %d", id)
		}
	}
	route, value, identity, null := ds[1], ds[6], ds[7], ds[4]
	nullableValue, nullableIdentity := ds[2], ds[3]
	if (null.GetInt64() != 0) != nullableValue.IsNull() || nullableValue.IsNull() != nullableIdentity.IsNull() {
		return row{}, fmt.Errorf("physical NULL columns disagree with logical row")
	}

	return row{Route: route.GetInt64(), Value: value.GetInt64(), Identity: identity.GetInt64(), Null: null.GetInt64() != 0}, nil
}

func indexes(p splitPlan, global bool) []*model.IndexInfo {
	base := int64(1)
	version := uint8(0)
	if global {
		base = int64(100 + p.Generation*10)
		version = model.GlobalIndexVersionV1
	}
	return []*model.IndexInfo{
		{ID: base, Global: global, GlobalIndexVersion: version, Columns: []*model.IndexColumn{{Offset: 0, Length: types.UnspecifiedLength}}},
		{ID: base + 1, Global: global, GlobalIndexVersion: version, Unique: true, Columns: []*model.IndexColumn{{Offset: 1, Length: types.UnspecifiedLength}}},
	}
}

func indexPair(p splitPlan, pid, h int64, r row, idx *model.IndexInfo) (pair, error) {
	d := types.NewIntDatum(r.Value)
	if idx.Unique {
		d = types.NewIntDatum(r.Identity)
	}
	if r.Null {
		d = types.Datum{}
	}
	tbl := &model.TableInfo{ID: logicalID, Columns: []*model.ColumnInfo{
		{ID: 2, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeLonglong)},
		{ID: 3, Offset: 1, FieldType: *types.NewFieldType(mysql.TypeLonglong)},
	}}
	physical := pid
	var handle kv.Handle = kv.IntHandle(h)
	if idx.Global {
		physical = logicalID
		handle = kv.NewPartitionHandle(pid, handle)
	}
	key, distinct, err := tablecodec.GenIndexKey(codec.NewEncoder(false), time.UTC, tbl, idx, physical, []types.Datum{d}, handle, nil)
	if err != nil {
		return pair{}, err
	}
	value, err := tablecodec.GenIndexValuePortal(false, time.UTC, tbl, idx, false, distinct, false, []types.Datum{d}, handle, pid, nil, nil)
	return pair{key, value}, err
}

func transact(ctx context.Context, c *txnkv.Client, f func(*transaction.KVTxn) error) error {
	txn, err := c.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = txn.Rollback() }()
	if err = f(txn); err != nil {
		return err
	}
	return txn.Commit(ctx)
}

func putJSON(txn *transaction.KVTxn, key []byte, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return txn.Set(key, b)
}
func getOptional(ctx context.Context, txn *transaction.KVTxn, key []byte) ([]byte, error) {
	v, err := txn.Get(ctx, key)
	if tikverr.IsErrNotFound(err) {
		return nil, nil
	}
	return v.Value, err
}

// mutate is the prototype's only foreground mutation entrypoint. It captures both
// endpoints atomically, including DELETE of a target that has never existed.
// oldPID == 0 explicitly means INSERT with h as an allocator candidate. It must
// not interpret an occupied candidate as the row to update.
// It is not a hook installed in TiDB's SQL table implementation.
func mutate(ctx context.Context, c *txnkv.Client, p splitPlan, oldPID, h int64, next *row, direct bool) error {
	return transact(ctx, c, func(txn *transaction.KVTxn) error {
		return applyMutation(ctx, txn, p, oldPID, h, next, direct)
	})
}

func applyMutation(ctx context.Context, txn *transaction.KVTxn, p splitPlan, oldPID, h int64, next *row, direct bool) error {
	var before *indexRow

	var old []byte
	var err error
	if oldPID != 0 {
		old, err = getOptional(ctx, txn, recordKey(oldPID, h))
		if err != nil {
			return err
		}
	}

	if old != nil {
		r, e := decodeRow(old)
		if e != nil {
			return e
		}
		q, e := p.target(r.Route)
		if e != nil {
			return e
		}
		if p.Parents[q] != oldPID {
			return fmt.Errorf("old source route mismatch")
		}
		before = &indexRow{PID: q, Handle: h, Row: r}
		if !r.Null {
			if e = txn.Delete(uniqueKey(r.Identity)); e != nil {
				return e
			}
		}

		if e = txn.Set(markerKey(p, q, h), []byte(fmt.Sprint(txn.StartTS()))); e != nil {
			return e
		}
		if direct {
			if e = txn.Delete(recordKey(q, h)); e != nil {
				return e
			}
		}
		if e = txn.Delete(recordKey(oldPID, h)); e != nil {
			return e
		}
	}
	if next == nil {
		return maintainIndexes(ctx, txn, p, before, nil)
	}
	pid, e := sourcePID(next.Route)
	if e != nil {
		return e
	}
	q, e := p.target(next.Route)
	if e != nil {
		return e
	}
	// Cross-parent moves can collide on an implicit handle. Reserve a free slot
	// by reading and writing that source key in the same optimistic transaction.
	if pid != oldPID || old == nil {
		for {
			occupied, e := getOptional(ctx, txn, recordKey(pid, h))
			if e != nil {
				return e
			}
			shadow, e := getOptional(ctx, txn, recordKey(q, h))
			if e != nil {
				return e
			}
			if occupied == nil && shadow == nil {
				break
			}
			h++
		}
	}

	if !next.Null {
		key := uniqueKey(next.Identity)
		occupied, e := getOptional(ctx, txn, key)
		if e != nil {
			return e
		}
		if occupied != nil {
			return errDuplicateIdentity
		}
		if e = txn.Set(key, codec.EncodeInt(codec.EncodeInt(nil, pid), h)); e != nil {
			return e
		}
	}
	b, e := encodeRow(*next)
	if e != nil {
		return e
	}
	if e = txn.Set(recordKey(pid, h), b); e != nil {
		return e
	}
	if e = txn.Set(markerKey(p, q, h), []byte(fmt.Sprint(txn.StartTS()))); e != nil {
		return e
	}
	if direct {
		if e = txn.Set(recordKey(q, h), b); e != nil {
			return e
		}
	}
	return maintainIndexes(ctx, txn, p, before, &indexRow{PID: q, Handle: h, Row: *next})
}

// prepareRepair deliberately returns an uncommitted transaction for deterministic
// conflict tests. Empty marker reads do not claim to validate a read set.
func prepareRepair(ctx context.Context, c *txnkv.Client, p splitPlan, key []byte) (*transaction.KVTxn, error) {
	txn, err := c.Begin()
	if err != nil {
		return nil, err
	}
	fail := func(e error) (*transaction.KVTxn, error) { _ = txn.Rollback(); return nil, e }
	gate, err := getOptional(ctx, txn, gateKey(p))
	if err != nil {
		return fail(err)
	}
	if !bytes.Equal(gate, []byte("open")) {
		return fail(fmt.Errorf("repair gate closed or missing"))
	}
	marker, err := getOptional(ctx, txn, key)
	if err != nil {
		return fail(err)
	}
	if marker == nil {
		return fail(fmt.Errorf("marker absent"))
	}
	rest, q, err := codec.DecodeInt(key[len(markerPrefix(p)):])
	if err != nil {
		return fail(err)
	}
	rest, h, err := codec.DecodeInt(rest)
	if err != nil {
		return fail(err)
	}
	if len(rest) != 0 {
		return fail(fmt.Errorf("invalid marker suffix"))
	}
	parent, ok := p.Parents[q]
	if !ok {
		return fail(fmt.Errorf("unknown target %d", q))
	}
	b, err := getOptional(ctx, txn, recordKey(parent, h))
	if err != nil {
		return fail(err)
	}
	keep := false
	if b != nil {
		r, e := decodeRow(b)
		if e != nil {
			return fail(e)
		}
		target, e := p.target(r.Route)
		if e != nil {
			return fail(e)
		}
		keep = target == q
	}
	if keep {
		err = txn.Set(recordKey(q, h), b)
	} else {
		err = txn.Delete(recordKey(q, h))
	}
	if err != nil {
		return fail(err)
	}
	if err = txn.Delete(key); err != nil {
		return fail(err)
	}
	// Every repair writes the same range gate; owner close invalidates stale work.
	if err = txn.Set(gateKey(p), []byte("open")); err != nil {
		return fail(err)
	}
	return txn, nil
}

func repair(ctx context.Context, c *txnkv.Client, p splitPlan) (int, error) {
	ts, err := c.GetTimestamp(ctx)
	if err != nil {
		return 0, err
	}
	pref := kv.Key(markerPrefix(p))
	iter, err := c.GetSnapshot(ts).Iter(pref, pref.PrefixNext())
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	count := 0
	for iter.Valid() {
		key := bytes.Clone(iter.Key())
		txn, err := prepareRepair(ctx, c, p, key)
		if err != nil {
			return count, err
		}
		if err = txn.Commit(ctx); err != nil {
			return count, err
		}
		count++
		if err = iter.Next(); err != nil {
			return count, err
		}
	}
	return count, nil
}
