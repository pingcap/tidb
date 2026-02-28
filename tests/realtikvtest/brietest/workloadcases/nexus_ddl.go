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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/pingcap/tidb/pkg/testkit/brhelper/workload"
)

type NexusDDLCase struct {
	Suffix string `json:"suffix"`
	N      int    `json:"n"`
}

func (c *NexusDDLCase) Name() string { return "NexusDDL" }

func (c *NexusDDLCase) Prepare(ctx workload.Context) (json.RawMessage, error) {
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
		n = 50
	}
	st := nexusState{
		Suffix:      suffix,
		DB:          fmt.Sprintf("test_nexus_ddl_%s", suffix),
		N:           n,
		Ticked:      0,
		NextTableID: 1,
		Tables:      []nexusTableState{{Name: "t_0"}},
	}
	if err := nexusExecDDL(ctx, ctx.DB, 0, "CREATE DATABASE IF NOT EXISTS "+workload.QIdent(st.DB)); err != nil {
		return nil, err
	}
	if err := nexusCreateTable(ctx, ctx.DB, 0, st.DB, st.Tables[0].Name); err != nil {
		return nil, err
	}
	return json.Marshal(st)
}

func (c *NexusDDLCase) Tick(ctx workload.TickContext, raw json.RawMessage) error {
	var st nexusState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	st.N = nexusDefaultN(st.N)
	if st.NextTableID <= 0 {
		st.NextTableID = len(st.Tables)
	}
	for i := range st.Tables {
		if st.Tables[i].NextColID < len(st.Tables[i].Cols) {
			st.Tables[i].NextColID = len(st.Tables[i].Cols)
		}
	}

	tickNo := st.Ticked + 1
	half := nexusHalf(st.N)

	if workload.EveryNTick(tickNo, 2*st.N) && len(st.Tables) > 0 {
		oldest := st.Tables[0].Name
		stmt := "DROP TABLE IF EXISTS " + workload.QTable(st.DB, oldest)
		if err := nexusExecDDL(ctx, ctx.DB, tickNo, stmt); err != nil {
			return err
		}
		st.Tables = st.Tables[1:]
	}

	if workload.EveryNTick(tickNo, st.N) {
		name := nexusTableName(st.NextTableID)
		st.NextTableID++
		if err := nexusCreateTable(ctx, ctx.DB, tickNo, st.DB, name); err != nil {
			return err
		}
		st.Tables = append(st.Tables, nexusTableState{Name: name})
	}

	if workload.EveryNTick(tickNo, half) && len(st.Tables) > 0 {
		youngest := &st.Tables[len(st.Tables)-1]
		if err := nexusAddOneColumn(ctx, ctx.DB, &st, tickNo, youngest); err != nil {
			return err
		}
	}

	if workload.EveryNTick(tickNo, st.N) && len(st.Tables) > 0 {
		oldest := &st.Tables[0]
		if err := nexusDropOneColumn(ctx, ctx.DB, &st, tickNo, oldest); err != nil {
			return err
		}
	}

	for _, t := range st.Tables {
		if err := nexusInsertRow(ctx, ctx.DB, st.DB, t.Name, tickNo); err != nil {
			return err
		}
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

func (c *NexusDDLCase) Exit(ctx workload.ExitContext, raw json.RawMessage) error {
	var st nexusState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}

	sums, err := nexusRecordChecksums(ctx, ctx.DB, st.DB, st.Tables)
	if err != nil {
		return err
	}
	st.Checksums = sums
	st.LogDone = true

	updated, err := json.Marshal(st)
	if err != nil {
		return err
	}
	ctx.UpdateState(updated)
	return nil
}

func (c *NexusDDLCase) Verify(ctx workload.Context, raw json.RawMessage) error {
	var st nexusState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	if err := workload.Require(st.LogDone, "NexusDDL: log not executed"); err != nil {
		return err
	}
	if err := workload.Require(len(st.Checksums) > 0, "NexusDDL: checksum not recorded; run Exit first"); err != nil {
		return err
	}

	for _, t := range st.Tables {
		ok, err := workload.TableExists(ctx, ctx.DB, st.DB, t.Name)
		if err != nil {
			return err
		}
		if err := workload.Require(ok, "NexusDDL: table %s.%s not found", st.DB, t.Name); err != nil {
			return err
		}

		for _, col := range t.Cols {
			has, err := workload.ColumnExists(ctx, ctx.DB, st.DB, t.Name, col)
			if err != nil {
				return err
			}
			if err := workload.Require(has, "NexusDDL: %s.%s column %q not found", st.DB, t.Name, col); err != nil {
				return err
			}
		}

		want, ok := st.Checksums[t.Name]
		if !ok {
			return fmt.Errorf("NexusDDL: missing checksum for table %s.%s", st.DB, t.Name)
		}
		got, err := workload.AdminChecksumTable(ctx, ctx.DB, st.DB, t.Name)
		if err != nil {
			return err
		}
		if err := workload.Require(got.TotalKvs == want.TotalKvs, "NexusDDL: Total_kvs mismatch for %s.%s: got %q want %q", st.DB, t.Name, got.TotalKvs, want.TotalKvs); err != nil {
			return err
		}
		if want.TotalBytes != "" {
			if err := workload.Require(got.TotalBytes == want.TotalBytes, "NexusDDL: Total_bytes mismatch for %s.%s: got %q want %q", st.DB, t.Name, got.TotalBytes, want.TotalBytes); err != nil {
				return err
			}
		}
	}
	return nil
}

func nexusAddOneColumn(ctx context.Context, db *sql.DB, st *nexusState, tick int, t *nexusTableState) error {
	if t == nil {
		return nil
	}
	if t.NextColID < len(t.Cols) {
		t.NextColID = len(t.Cols)
	}

	col := fmt.Sprintf("c_%d", t.NextColID)
	exists, err := workload.ColumnExists(ctx, db, st.DB, t.Name, col)
	if err != nil {
		return err
	}
	if exists {
		if !slices.Contains(t.Cols, col) {
			t.Cols = append(t.Cols, col)
		}
		t.NextColID++
		return nil
	}

	stmt := "ALTER TABLE " + workload.QTable(st.DB, t.Name) + " ADD COLUMN " + workload.QIdent(col) + " BIGINT"
	if err := nexusExecDDL(ctx, db, tick, stmt); err != nil {
		return err
	}
	t.Cols = append(t.Cols, col)
	t.NextColID++
	return nil
}

func nexusDropOneColumn(ctx context.Context, db *sql.DB, st *nexusState, tick int, t *nexusTableState) error {
	if t == nil || len(t.Cols) == 0 {
		return nil
	}
	col := t.Cols[0]
	exists, err := workload.ColumnExists(ctx, db, st.DB, t.Name, col)
	if err != nil {
		return err
	}
	if exists {
		stmt := "ALTER TABLE " + workload.QTable(st.DB, t.Name) + " DROP COLUMN " + workload.QIdent(col)
		if err := nexusExecDDL(ctx, db, tick, stmt); err != nil {
			return err
		}
	}
	t.Cols = t.Cols[1:]
	return nil
}
