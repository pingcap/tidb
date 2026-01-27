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
)

type NexusDDLDestructiveCase struct {
	Suffix string `json:"suffix"`
	N      int    `json:"n"`

	ddls []nexusDDLEvent
}

func (c *NexusDDLDestructiveCase) Name() string { return "NexusDDLDestructive" }

func (c *NexusDDLDestructiveCase) Prepare(ctx Context) (json.RawMessage, error) {
	c.ddls = nil

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
		n = 50
	}
	st := nexusState{
		Suffix:      suffix,
		DB:          fmt.Sprintf("test_nexus_ddl_destructive_%s", suffix),
		N:           n,
		Ticked:      0,
		NextTableID: 1,
		Tables:      []nexusTableState{{Name: "t_0"}},
	}
	if err := nexusExecDDL(ctx, ctx.DB, &c.ddls, 0, "CREATE DATABASE IF NOT EXISTS "+QIdent(st.DB)); err != nil {
		return nil, err
	}
	if err := nexusCreateTable(ctx, ctx.DB, &c.ddls, 0, st.DB, st.Tables[0].Name); err != nil {
		return nil, err
	}

	ctx.SetSummary(nexusSummary{
		DB:     st.DB,
		N:      st.N,
		Ticked: st.Ticked,
		DDLs:   c.ddls,
	})
	return json.Marshal(st)
}

func (c *NexusDDLDestructiveCase) Tick(ctx TickContext, raw json.RawMessage) error {
	var st nexusState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	st.N = nexusDefaultN(st.N)
	if st.NextTableID <= 0 {
		st.NextTableID = len(st.Tables)
	}

	tickNo := st.Ticked + 1
	half := nexusHalf(st.N)

	if EveryNTick(tickNo, st.N) {
		name := nexusTableName(st.NextTableID)
		st.NextTableID++
		if err := nexusCreateTable(ctx, ctx.DB, &c.ddls, tickNo, st.DB, name); err != nil {
			return err
		}
		st.Tables = append(st.Tables, nexusTableState{Name: name})
	}

	if EveryNTick(tickNo, half) && len(st.Tables) > 0 {
		idx := ctx.RNG.IntN(len(st.Tables))
		oldName := st.Tables[idx].Name
		newName := nexusTableName(st.NextTableID)
		st.NextTableID++
		stmt := "RENAME TABLE " + QTable(st.DB, oldName) + " TO " + QTable(st.DB, newName)
		if err := nexusExecDDL(ctx, ctx.DB, &c.ddls, tickNo, stmt); err != nil {
			return err
		}
		st.Tables[idx].Name = newName
	}

	if EveryNTick(tickNo, 2*st.N) && len(st.Tables) > 0 {
		idx := ctx.RNG.IntN(len(st.Tables))
		stmt := "TRUNCATE TABLE " + QTable(st.DB, st.Tables[idx].Name)
		if err := nexusExecDDL(ctx, ctx.DB, &c.ddls, tickNo, stmt); err != nil {
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

func (c *NexusDDLDestructiveCase) Exit(ctx ExitContext, raw json.RawMessage) error {
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

	ctx.SetSummary(nexusSummary{
		DB:     st.DB,
		N:      st.N,
		Ticked: st.Ticked,
		DDLs:   c.ddls,
	})

	updated, err := json.Marshal(st)
	if err != nil {
		return err
	}
	ctx.UpdateState(updated)
	return nil
}

func (c *NexusDDLDestructiveCase) Verify(ctx Context, raw json.RawMessage) error {
	var st nexusState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	if err := Require(st.LogDone, "NexusDDLDestructive: log not executed"); err != nil {
		return err
	}
	if err := Require(len(st.Checksums) > 0, "NexusDDLDestructive: checksum not recorded; run Exit first"); err != nil {
		return err
	}

	for _, t := range st.Tables {
		ok, err := TableExists(ctx, ctx.DB, st.DB, t.Name)
		if err != nil {
			return err
		}
		if err := Require(ok, "NexusDDLDestructive: table %s.%s not found", st.DB, t.Name); err != nil {
			return err
		}

		want, ok := st.Checksums[t.Name]
		if !ok {
			return fmt.Errorf("NexusDDLDestructive: missing checksum for table %s.%s", st.DB, t.Name)
		}
		got, err := AdminChecksumTable(ctx, ctx.DB, st.DB, t.Name)
		if err != nil {
			return err
		}
		if err := Require(got.TotalKvs == want.TotalKvs, "NexusDDLDestructive: Total_kvs mismatch for %s.%s: got %q want %q", st.DB, t.Name, got.TotalKvs, want.TotalKvs); err != nil {
			return err
		}
		if want.TotalBytes != "" {
			if err := Require(got.TotalBytes == want.TotalBytes, "NexusDDLDestructive: Total_bytes mismatch for %s.%s: got %q want %q", st.DB, t.Name, got.TotalBytes, want.TotalBytes); err != nil {
				return err
			}
		}
	}
	return nil
}
