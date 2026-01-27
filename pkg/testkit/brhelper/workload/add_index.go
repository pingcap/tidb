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

package workload

import (
	"encoding/json"
	"fmt"
	"strings"
)

type AddIndexCase struct {
	Suffix string `json:"suffix"`
	N      int    `json:"n"`
	NR     int    `json:"nr"`

	indexesAdded   []addIndexSpec
	indexesDropped []addIndexSpec
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

	Checksum TableChecksum `json:"checksum"`
	LogDone  bool                `json:"log_done"`
}

type addIndexSummary struct {
	DB     string `json:"db"`
	Table  string `json:"table"`
	N      int    `json:"n"`
	NR     int    `json:"nr"`
	Ticked int    `json:"ticked"`

	IndexesAdded   []addIndexSpec `json:"indexes_added,omitempty"`
	IndexesDropped []addIndexSpec `json:"indexes_dropped,omitempty"`
}

func (s addIndexSummary) SummaryTable() string {
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "db=%s table=%s n=%d nr=%d ticked=%d", s.DB, s.Table, s.N, s.NR, s.Ticked)
	if len(s.IndexesAdded) > 0 {
		b.WriteString("\nindexes added:")
		for _, idx := range s.IndexesAdded {
			b.WriteString("\n  - ")
			b.WriteString(idx.Name)
			if len(idx.Columns) > 0 {
				b.WriteString("(" + joinWithComma(idx.Columns) + ")")
			}
		}
	}
	if len(s.IndexesDropped) > 0 {
		b.WriteString("\nindexes dropped:")
		for _, idx := range s.IndexesDropped {
			b.WriteString("\n  - ")
			b.WriteString(idx.Name)
			if len(idx.Columns) > 0 {
				b.WriteString("(" + joinWithComma(idx.Columns) + ")")
			}
		}
	}
	return b.String()
}

func (c *AddIndexCase) Name() string { return "AddIndex" }

func (c *AddIndexCase) Prepare(ctx Context) (json.RawMessage, error) {
	c.indexesAdded = nil
	c.indexesDropped = nil

	suffix := c.Suffix
	if suffix == "" {
		var err error
		suffix, err = RandSuffix()
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
	if err := ExecAll(ctx, ctx.DB, []string{
		"CREATE DATABASE IF NOT EXISTS " + QIdent(st.DB),
		"CREATE TABLE IF NOT EXISTS " + QTable(st.DB, st.Table) + " (" +
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

	ctx.SetSummary(addIndexSummary{
		DB:    st.DB,
		Table: st.Table,
		N:     st.N,
		NR:    st.NR,
	})
	return json.Marshal(st)
}

func (c *AddIndexCase) Tick(ctx TickContext, raw json.RawMessage) error {
	var st addIndexState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	if st.N <= 0 {
		st.N = 100
	}
	if st.NR <= 0 {
		st.NR = 150
	}
	if st.NextIndexID < len(st.Indexes) {
		st.NextIndexID = len(st.Indexes)
	}

	// Insert data each tick.
	v := int64(st.Inserted)
	if _, err := ctx.DB.ExecContext(ctx, "INSERT INTO "+QTable(st.DB, st.Table)+" (a,b,c,d,e) VALUES (?,?,?,?,?)",
		v, v*7+1, v*11+2, v*13+3, v*17+4,
	); err != nil {
		return err
	}
	st.Inserted++

	tickNo := st.Ticked + 1

	// Every N ticks, add a new index on 1~3 columns.
	if st.N > 0 && tickNo%st.N == 0 {
		allCols := []string{"a", "b", "c", "d", "e"}
		idxID := st.NextIndexID
		idxName := fmt.Sprintf("idx_%d", idxID)

		colN := 1 + (idxID % 3)
		start := idxID % len(allCols)
		cols := make([]string, 0, colN)
		for i := 0; i < colN; i++ {
			cols = append(cols, allCols[(start+i)%len(allCols)])
		}

		exists, err := IndexExists(ctx, ctx.DB, st.DB, st.Table, idxName)
		if err != nil {
			return err
		}
		if !exists {
			colSQL := make([]string, 0, len(cols))
			for _, c := range cols {
				colSQL = append(colSQL, QIdent(c))
			}
			stmt := "CREATE INDEX " + QIdent(idxName) + " ON " + QTable(st.DB, st.Table) + " (" + joinWithComma(colSQL) + ")"
			if _, err := ctx.DB.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}

		spec := addIndexSpec{Name: idxName, Columns: cols}
		if !hasAddIndexSpec(st.Indexes, idxName) {
			st.Indexes = append(st.Indexes, spec)
		}
		if !hasAddIndexSpec(c.indexesAdded, idxName) {
			c.indexesAdded = append(c.indexesAdded, spec)
		}
		st.NextIndexID++
	}

	// Every NR ticks, randomly drop an index.
	if st.NR > 0 && tickNo%st.NR == 0 && len(st.Indexes) > 0 {
		idx := pickIndex(st.Ticked, len(st.Indexes))
		dropSpec := st.Indexes[idx]

		exists, err := IndexExists(ctx, ctx.DB, st.DB, st.Table, dropSpec.Name)
		if err != nil {
			return err
		}
		if exists {
			stmt := "DROP INDEX " + QIdent(dropSpec.Name) + " ON " + QTable(st.DB, st.Table)
			if _, err := ctx.DB.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
		c.indexesDropped = append(c.indexesDropped, dropSpec)
		st.Indexes = append(st.Indexes[:idx], st.Indexes[idx+1:]...)
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

func (c *AddIndexCase) Exit(ctx ExitContext, raw json.RawMessage) error {
	var st addIndexState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}

	checksum, err := AdminChecksumTable(ctx, ctx.DB, st.DB, st.Table)
	if err != nil {
		return err
	}
	st.Checksum = checksum
	st.LogDone = true

	ctx.SetSummary(addIndexSummary{
		DB:             st.DB,
		Table:          st.Table,
		N:              st.N,
		NR:             st.NR,
		Ticked:         st.Ticked,
		IndexesAdded:   c.indexesAdded,
		IndexesDropped: c.indexesDropped,
	})

	updated, err := json.Marshal(st)
	if err != nil {
		return err
	}
	ctx.UpdateState(updated)
	return nil
}

func (c *AddIndexCase) Verify(ctx Context, raw json.RawMessage) error {
	var st addIndexState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	if err := Require(st.LogDone, "AddIndex: log not executed"); err != nil {
		return err
	}
	if err := Require(st.Checksum.TotalKvs != "", "AddIndex: checksum not recorded; run Exit first"); err != nil {
		return err
	}

	for _, idx := range st.Indexes {
		ok, err := IndexExists(ctx, ctx.DB, st.DB, st.Table, idx.Name)
		if err != nil {
			return err
		}
		if err := Require(ok, "AddIndex: index %q not found", idx.Name); err != nil {
			return err
		}
	}

	checksum, err := AdminChecksumTable(ctx, ctx.DB, st.DB, st.Table)
	if err != nil {
		return err
	}
	if err := Require(checksum.TotalKvs == st.Checksum.TotalKvs, "AddIndex: Total_kvs mismatch: got %q want %q", checksum.TotalKvs, st.Checksum.TotalKvs); err != nil {
		return err
	}
	if st.Checksum.TotalBytes != "" {
		return Require(checksum.TotalBytes == st.Checksum.TotalBytes, "AddIndex: Total_bytes mismatch: got %q want %q", checksum.TotalBytes, st.Checksum.TotalBytes)
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

func joinWithComma(parts []string) string {
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += "," + p
	}
	return out
}
