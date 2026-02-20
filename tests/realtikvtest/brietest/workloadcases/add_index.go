// Copyright 2025 PingCAP, Inc.
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

package workloadcases

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/testkit/brhelper/workload"
)

type AddIndexCase struct {
	Suffix string `json:"suffix"`
	N      int    `json:"n"`
	NR     int    `json:"nr"`
}

type addIndexSpec struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
}

type addIndexState struct {
	Suffix string `json:"suffix"`
	DB     string `json:"db"`
	Table  string `json:"table"`
	N      int    `json:"n"`
	NR     int    `json:"nr"`

	Inserted int `json:"inserted"`
	Ticked   int `json:"ticked"`

	NextIndexID int `json:"next_index_id"`

	Indexes []addIndexSpec `json:"indexes"`

	Checksum workload.TableChecksum `json:"checksum"`
	LogDone  bool                   `json:"log_done"`
}

func (c *AddIndexCase) Name() string { return "AddIndex" }

func (c *AddIndexCase) Prepare(ctx workload.Context) (json.RawMessage, error) {
	suffix := c.Suffix
	if suffix == "" {
		var err error
		suffix, err = workload.RandSuffix()
		if err != nil {
			return nil, err
		}
	}
	n := c.N
	if n <= 0 {
		n = 100
	}
	nr := c.NR
	if nr <= 0 {
		nr = 150
	}
	st := addIndexState{
		Suffix:      suffix,
		DB:          fmt.Sprintf("test_add_index_%s", suffix),
		Table:       "t1",
		N:           n,
		NR:          nr,
		NextIndexID: 0,
	}
	if err := workload.ExecAll(ctx, ctx.DB, []string{
		"CREATE DATABASE IF NOT EXISTS " + workload.QIdent(st.DB),
		"CREATE TABLE IF NOT EXISTS " + workload.QTable(st.DB, st.Table) + " (" +
			"id BIGINT PRIMARY KEY AUTO_INCREMENT," +
			"a BIGINT," +
			"b BIGINT," +
			"c BIGINT," +
			"d BIGINT," +
			"e BIGINT" +
			")",
	}); err != nil {
		return nil, err
	}

	return json.Marshal(st)
}

func (c *AddIndexCase) Tick(ctx workload.TickContext, raw json.RawMessage) error {
	var st addIndexState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	normalizeAddIndexState(&st)

	tickNo := st.Ticked + 1

	if err := addIndexInsertRow(ctx, &st); err != nil {
		return err
	}
	if err := c.maybeAddIndex(ctx, &st, tickNo); err != nil {
		return err
	}
	if err := c.maybeDropIndex(ctx, &st, tickNo); err != nil {
		return err
	}

	st.Ticked++
	st.LogDone = true

	updated, err := json.Marshal(st)
	if err != nil {
		return err
	}
	ctx.UpdateState(updated)
	return nil
}

func (c *AddIndexCase) Exit(ctx workload.ExitContext, raw json.RawMessage) error {
	var st addIndexState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}

	checksum, err := workload.AdminChecksumTable(ctx, ctx.DB, st.DB, st.Table)
	if err != nil {
		return err
	}
	st.Checksum = checksum
	st.LogDone = true

	updated, err := json.Marshal(st)
	if err != nil {
		return err
	}
	ctx.UpdateState(updated)
	return nil
}

func (c *AddIndexCase) Verify(ctx workload.Context, raw json.RawMessage) error {
	var st addIndexState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	if err := workload.Require(st.LogDone, "AddIndex: log not executed"); err != nil {
		return err
	}
	if err := workload.Require(st.Checksum.TotalKvs != "", "AddIndex: checksum not recorded; run Exit first"); err != nil {
		return err
	}

	for _, idx := range st.Indexes {
		ok, err := workload.IndexExists(ctx, ctx.DB, st.DB, st.Table, idx.Name)
		if err != nil {
			return err
		}
		if err := workload.Require(ok, "AddIndex: index %q not found", idx.Name); err != nil {
			return err
		}
	}

	checksum, err := workload.AdminChecksumTable(ctx, ctx.DB, st.DB, st.Table)
	if err != nil {
		return err
	}
	if err := workload.Require(checksum.TotalKvs == st.Checksum.TotalKvs, "AddIndex: Total_kvs mismatch: got %q want %q", checksum.TotalKvs, st.Checksum.TotalKvs); err != nil {
		return err
	}
	if st.Checksum.TotalBytes != "" {
		return workload.Require(checksum.TotalBytes == st.Checksum.TotalBytes, "AddIndex: Total_bytes mismatch: got %q want %q", checksum.TotalBytes, st.Checksum.TotalBytes)
	}
	return nil
}

func hasAddIndexSpec(indexes []addIndexSpec, name string) bool {
	for _, idx := range indexes {
		if idx.Name == name {
			return true
		}
	}
	return false
}

func normalizeAddIndexState(st *addIndexState) {
	if st.N <= 0 {
		st.N = 100
	}
	if st.NR <= 0 {
		st.NR = 150
	}
	if st.NextIndexID < len(st.Indexes) {
		st.NextIndexID = len(st.Indexes)
	}
}

func addIndexInsertRow(ctx workload.TickContext, st *addIndexState) error {
	v := int64(st.Inserted)
	if _, err := ctx.DB.ExecContext(ctx, "INSERT INTO "+workload.QTable(st.DB, st.Table)+" (a,b,c,d,e) VALUES (?,?,?,?,?)",
		v, v*7+1, v*11+2, v*13+3, v*17+4,
	); err != nil {
		return err
	}
	st.Inserted++
	return nil
}

func (c *AddIndexCase) maybeAddIndex(ctx workload.TickContext, st *addIndexState, tickNo int) error {
	if !workload.EveryNTick(tickNo, st.N) {
		return nil
	}
	allCols := []string{"a", "b", "c", "d", "e"}
	idxID := st.NextIndexID
	idxName := fmt.Sprintf("idx_%d", idxID)

	colN := 1 + (idxID % 3)
	start := idxID % len(allCols)
	cols := make([]string, 0, colN)
	for i := 0; i < colN; i++ {
		cols = append(cols, allCols[(start+i)%len(allCols)])
	}

	exists, err := workload.IndexExists(ctx, ctx.DB, st.DB, st.Table, idxName)
	if err != nil {
		return err
	}
	if !exists {
		colSQL := make([]string, 0, len(cols))
		for _, col := range cols {
			colSQL = append(colSQL, workload.QIdent(col))
		}
		stmt := "CREATE INDEX " + workload.QIdent(idxName) + " ON " + workload.QTable(st.DB, st.Table) + " (" + strings.Join(colSQL, ",") + ")"
		if _, err := ctx.DB.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}

	spec := addIndexSpec{Name: idxName, Columns: cols}
	if !hasAddIndexSpec(st.Indexes, idxName) {
		st.Indexes = append(st.Indexes, spec)
	}
	st.NextIndexID++
	return nil
}

func (c *AddIndexCase) maybeDropIndex(ctx workload.TickContext, st *addIndexState, tickNo int) error {
	if !workload.EveryNTick(tickNo, st.NR) || len(st.Indexes) == 0 {
		return nil
	}
	idx := ctx.RNG.IntN(len(st.Indexes))
	dropSpec := st.Indexes[idx]

	exists, err := workload.IndexExists(ctx, ctx.DB, st.DB, st.Table, dropSpec.Name)
	if err != nil {
		return err
	}
	if exists {
		stmt := "DROP INDEX " + workload.QIdent(dropSpec.Name) + " ON " + workload.QTable(st.DB, st.Table)
		if _, err := ctx.DB.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	st.Indexes = append(st.Indexes[:idx], st.Indexes[idx+1:]...)
	return nil
}
