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

package autoid_test

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/mockstore"
)

func BenchmarkAllocator_Alloc(b *testing.B) {
	b.StopTimer()
	store, err := mockstore.NewMockStore()
	if err != nil {
		return
	}
	defer func() {
		err := store.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()
	dbID := int64(1)
	tblID := int64(2)
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: dbID, Name: model.NewCIStr("a")})
		if err != nil {
			return err
		}
		err = m.CreateTableOrView(dbID, &model.TableInfo{ID: tblID, Name: model.NewCIStr("t")})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return
	}
	ctx := context.Background()
	alloc := autoid.NewAllocator(store, 1, 2, false, autoid.RowIDAllocType)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := alloc.Alloc(ctx, 1, 1, 1)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllocator_SequenceAlloc(b *testing.B) {
	b.StopTimer()
	store, err := mockstore.NewMockStore()
	if err != nil {
		return
	}
	defer func() {
		err := store.Close()
		if err != nil {
			b.Fatal(err)
		}
	}()
	var seq *model.SequenceInfo
	var sequenceBase int64
	err = kv.RunInNewTxn(context.Background(), store, false, func(ctx context.Context, txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		err = m.CreateDatabase(&model.DBInfo{ID: 1, Name: model.NewCIStr("a")})
		if err != nil {
			return err
		}
		seq = &model.SequenceInfo{
			Start:      1,
			Cycle:      true,
			Cache:      false,
			MinValue:   -10,
			MaxValue:   math.MaxInt64,
			Increment:  2,
			CacheValue: 2000000,
		}
		seqTable := &model.TableInfo{
			ID:       1,
			Name:     model.NewCIStr("seq"),
			Sequence: seq,
		}
		sequenceBase = seq.Start - 1
		err = m.CreateSequenceAndSetSeqValue(1, seqTable, sequenceBase)
		return err
	})
	if err != nil {
		return
	}
	alloc := autoid.NewSequenceAllocator(store, 1, 1, seq)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := alloc.AllocSeqCache()
		if err != nil {
			fmt.Println("err")
		}
	}
}

func BenchmarkAllocator_Seek(b *testing.B) {
	base := int64(21421948021)
	offset := int64(-351354365326)
	increment := int64(3)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := autoid.CalcSequenceBatchSize(base, 3, increment, offset, math.MinInt64, math.MaxInt64)
		if err != nil {
			b.Fatal(err)
		}
	}
}
