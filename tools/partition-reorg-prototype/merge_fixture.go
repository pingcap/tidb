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
	"compress/gzip"
	"context"
	"slices"

	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pingcap/tidb/pkg/kv"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
)

// mergeFixture is a single-MVCC-snapshot interchange for the same-package DDL
// test. Rows and input are exact storage bytes; expected indexes are row-derived.
type mergeFixture struct {
	Name            string              `json:"name"`
	AdapterVerified bool                `json:"adapter_verified"`
	Version         int                 `json:"version"`
	SnapshotTS      uint64              `json:"snapshot_ts"`
	Table           *model.TableInfo    `json:"table"`
	Rows            []pair              `json:"rows"`
	Input           []pair              `json:"input"`
	Ranges          []mergeFixtureRange `json:"ranges"`
}
type mergeFixtureRange struct {
	Adapter    []pair `json:"adapter"`
	PhysicalID int64  `json:"physical_id"`
	IndexID    int64  `json:"index_id"`
	Expected   []pair `json:"expected"`
}

func mergeFixtureTable(p splitPlan) *model.TableInfo {
	tbl := &model.TableInfo{ID: logicalID, Name: ast.NewCIStr("partition_proto"), State: model.StatePublic,
		Charset: "utf8mb4", Collate: "utf8mb4_bin"}
	for i, col := range []struct {
		id   int64
		name string
		tp   byte
	}{
		{2, "value", mysql.TypeLonglong}, {3, "identity", mysql.TypeLonglong}, {1, "route", mysql.TypeLonglong},
		{4, "null_flag", mysql.TypeLonglong}, {5, "payload", mysql.TypeVarchar},
		{6, "raw_value", mysql.TypeLonglong}, {7, "logical_identity", mysql.TypeLonglong},
	} {
		ft := types.NewFieldType(col.tp)
		if col.tp == mysql.TypeVarchar {
			ft.SetFlen(512)
			ft.SetCharset("utf8mb4")
			ft.SetCollate("utf8mb4_bin")
		}
		tbl.Columns = append(tbl.Columns, &model.ColumnInfo{ID: col.id, Name: ast.NewCIStr(col.name), Offset: i, State: model.StatePublic, FieldType: *ft})
	}
	for _, global := range []bool{false, true} {
		for _, idx := range indexes(p, global) {
			idx.State = model.StateWriteReorganization
			idx.BackfillState = model.BackfillStateMerging
			idx.Name = ast.NewCIStr(fmt.Sprintf("idx_%d", idx.ID))
			idx.Columns[0].Name = tbl.Columns[idx.Columns[0].Offset].Name
			tbl.Indices = append(tbl.Indices, idx)
		}
	}
	tbl.Partition = &model.PartitionInfo{Type: ast.PartitionTypeRange, Enable: true, Expr: "route"}
	for i, pid := range p.Targets {
		tbl.Partition.Definitions = append(tbl.Partition.Definitions,
			model.PartitionDefinition{ID: pid, Name: ast.NewCIStr(fmt.Sprintf("p%d", i)), LessThan: []string{fmt.Sprint((i + 1) * 50)}})
	}
	return tbl
}

func (im *importer) exportMergeFixture(ctx context.Context, p splitPlan, global bool) error {
	ts, err := im.client.GetTimestamp(ctx)
	if err != nil {
		return err
	}
	rows, err := records(ctx, im.client, ts, p.Targets)
	if err != nil {
		return err
	}
	kind := "local"
	pids := p.Targets
	if global {
		kind = "global"
		pids = []int64{logicalID}
	}
	f := mergeFixture{Name: kind, Version: 1, SnapshotTS: ts, Table: mergeFixtureTable(p), Rows: rows}
	for _, pid := range pids {
		for _, idx := range indexes(p, global) {
			rg := mergeFixtureRange{PhysicalID: pid, IndexID: idx.ID}
			for _, prefix := range [][]byte{
				tablecodec.EncodeIndexSeekKey(pid, idx.ID, nil),
				tablecodec.EncodeIndexSeekKey(pid, tablecodec.TempIndexPrefix|idx.ID, nil),
			} {
				pairs, err := scan(ctx, im.client, ts, prefix)
				if err != nil {
					return err
				}
				f.Input = append(f.Input, pairs...)
			}
			for _, entry := range rows {
				rowPID, h, err := tablecodec.DecodeRecordKey(entry.Key)
				if err != nil {
					return err
				}
				if !global && rowPID != pid {
					continue
				}
				r, err := decodeRow(entry.Value)
				if err != nil {
					return err
				}
				expected, err := indexPair(p, rowPID, h.IntValue(), r, idx)
				if err != nil {
					return err
				}
				rg.Expected = append(rg.Expected, expected)
			}
			f.Ranges = append(f.Ranges, rg)
		}
	}
	if err = verifySnapshotAdapter(&f); err != nil {
		return err
	}
	im.mergeFixtures = append(im.mergeFixtures, f)
	file, err := os.CreateTemp(im.dir, "merge-fixtures-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	compressed := gzip.NewWriter(file)
	if err = json.NewEncoder(compressed).Encode(im.mergeFixtures); err != nil {
		_ = compressed.Close()
		return err
	}
	if err = compressed.Close(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(file.Name(), filepath.Join(im.dir, "merge-fixtures.json.gz")); err != nil {
		return err
	}
	if global {
		im.mergeFixtures = nil
	}
	return nil
}

// verifySnapshotAdapter freezes the standalone adapter to the exact same input
// as the production step replay, even while later foreground DML continues.
func verifySnapshotAdapter(f *mergeFixture) error {
	state := make(map[string][]byte)
	rows := make(map[string]bool)
	for _, entry := range f.Input {
		state[string(entry.Key)] = bytes.Clone(entry.Value)
	}
	for _, entry := range f.Rows {
		rows[string(entry.Key)] = true
	}
	for i := range f.Ranges {
		rg := &f.Ranges[i]
		prefix := tablecodec.EncodeIndexSeekKey(rg.PhysicalID, tablecodec.TempIndexPrefix|rg.IndexID, nil)
		var keys []string
		for key := range state {
			if bytes.HasPrefix([]byte(key), prefix) {
				keys = append(keys, key)
			}
		}
		slices.Sort(keys)
		for _, key := range keys {
			normal := []byte(key)
			tablecodec.TempIndexKey2IndexKey(normal)
			value, err := applyTempHistory([]byte(key), state[key], state[string(normal)], func(h kv.Handle) (bool, error) {
				pid := rg.PhysicalID
				if ph, ok := h.(kv.PartitionHandle); ok {
					pid = ph.PartitionID
					h = ph.Handle
				}
				return rows[string(recordKey(pid, h.IntValue()))], nil
			})
			if err != nil {
				return fmt.Errorf("snapshot adapter: %w", err)
			}
			if value == nil {
				delete(state, string(normal))
			} else {
				state[string(normal)] = value
			}
		}
		normalPrefix := tablecodec.EncodeIndexSeekKey(rg.PhysicalID, rg.IndexID, nil)
		for key, value := range state {
			if bytes.HasPrefix([]byte(key), normalPrefix) {
				rg.Adapter = append(rg.Adapter, pair{[]byte(key), bytes.Clone(value)})
			}
		}
		if err := equalPairs(rg.Expected, rg.Adapter); err != nil {
			return fmt.Errorf("snapshot adapter disagrees with row-derived index: %w", err)
		}
	}
	f.AdapterVerified = true
	return nil
}
