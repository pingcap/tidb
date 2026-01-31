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

	"github.com/pingcap/tidb/pkg/testkit/brhelper/workload"
)

type NexusDDLDestructiveCase struct {
	Suffix string `json:"suffix"`
	N      int    `json:"n"`
}

func (c *NexusDDLDestructiveCase) Name() string { return "NexusDDLDestructive" }

func (c *NexusDDLDestructiveCase) Prepare(ctx workload.Context) (json.RawMessage, error) {
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
		DB:          fmt.Sprintf("test_nexus_ddl_destructive_%s", suffix),
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

func (c *NexusDDLDestructiveCase) Tick(ctx workload.TickContext, raw json.RawMessage) error {
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

	if workload.EveryNTick(tickNo, st.N) {
		name := nexusTableName(st.NextTableID)
		st.NextTableID++
		if err := nexusCreateTable(ctx, ctx.DB, tickNo, st.DB, name); err != nil {
			return err
		}
		st.Tables = append(st.Tables, nexusTableState{Name: name})
	}

	if workload.EveryNTick(tickNo, half) && len(st.Tables) > 0 {
		idx := ctx.RNG.IntN(len(st.Tables))
		oldName := st.Tables[idx].Name
		newName := nexusTableName(st.NextTableID)
		st.NextTableID++
		stmt := "RENAME TABLE " + workload.QTable(st.DB, oldName) + " TO " + workload.QTable(st.DB, newName)
		if err := nexusExecDDL(ctx, ctx.DB, tickNo, stmt); err != nil {
			return err
		}
		st.Tables[idx].Name = newName
	}

	if workload.EveryNTick(tickNo, 2*st.N) && len(st.Tables) > 0 {
		idx := ctx.RNG.IntN(len(st.Tables))
		stmt := "TRUNCATE TABLE " + workload.QTable(st.DB, st.Tables[idx].Name)
		if err := nexusExecDDL(ctx, ctx.DB, tickNo, stmt); err != nil {
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

func (c *NexusDDLDestructiveCase) Exit(ctx workload.ExitContext, raw json.RawMessage) error {
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

func (c *NexusDDLDestructiveCase) Verify(ctx workload.Context, raw json.RawMessage) error {
	var st nexusState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	if err := workload.Require(st.LogDone, "NexusDDLDestructive: log not executed"); err != nil {
		return err
	}
	if err := workload.Require(len(st.Checksums) > 0, "NexusDDLDestructive: checksum not recorded; run Exit first"); err != nil {
		return err
	}

	for _, t := range st.Tables {
		ok, err := workload.TableExists(ctx, ctx.DB, st.DB, t.Name)
		if err != nil {
			return err
		}
		if err := workload.Require(ok, "NexusDDLDestructive: table %s.%s not found", st.DB, t.Name); err != nil {
			return err
		}

		want, ok := st.Checksums[t.Name]
		if !ok {
			return fmt.Errorf("NexusDDLDestructive: missing checksum for table %s.%s", st.DB, t.Name)
		}
		got, err := workload.AdminChecksumTable(ctx, ctx.DB, st.DB, t.Name)
		if err != nil {
			return err
		}
		if err := workload.Require(got.TotalKvs == want.TotalKvs, "NexusDDLDestructive: Total_kvs mismatch for %s.%s: got %q want %q", st.DB, t.Name, got.TotalKvs, want.TotalKvs); err != nil {
			return err
		}
		if want.TotalBytes != "" {
			if err := workload.Require(got.TotalBytes == want.TotalBytes, "NexusDDLDestructive: Total_bytes mismatch for %s.%s: got %q want %q", st.DB, t.Name, got.TotalBytes, want.TotalBytes); err != nil {
				return err
			}
		}
	}
	return nil
}
