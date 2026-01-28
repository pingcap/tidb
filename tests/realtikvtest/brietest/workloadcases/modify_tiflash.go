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

type ModifyTiFlashCase struct {
	Suffix string `json:"suffix"`
	N      int    `json:"n"`
	NAP    int    `json:"nap"`
}

type modifyTiFlashState struct {
	Suffix string `json:"suffix"`
	DB     string `json:"db"`
	Table  string `json:"table"`
	N      int    `json:"n"`
	NAP    int    `json:"nap"`

	Ticked   int `json:"ticked"`
	Inserted int `json:"inserted"`

	Replica int `json:"replica"`

	Checksum workload.TableChecksum `json:"checksum"`
	LogDone  bool                   `json:"log_done"`
}

func (c *ModifyTiFlashCase) Name() string { return "ModifyTiFlash" }

func (c *ModifyTiFlashCase) Prepare(ctx workload.Context) (json.RawMessage, error) {
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
	nap := c.NAP
	if nap <= 0 {
		nap = 1
	}
	st := modifyTiFlashState{
		Suffix:  suffix,
		DB:      fmt.Sprintf("test_modify_tiflash_%s", suffix),
		Table:   "t1",
		N:       n,
		NAP:     nap,
		Replica: 0,
	}
	if err := workload.ExecAll(ctx, ctx.DB, []string{
		"CREATE DATABASE IF NOT EXISTS " + workload.QIdent(st.DB),
		"CREATE TABLE IF NOT EXISTS " + workload.QTable(st.DB, st.Table) + " (" +
			"id BIGINT PRIMARY KEY AUTO_INCREMENT," +
			"a BIGINT," +
			"b BIGINT," +
			"c BIGINT" +
			")",
		"ALTER TABLE " + workload.QTable(st.DB, st.Table) + " SET TIFLASH REPLICA 0",
	}); err != nil {
		return nil, err
	}

	return json.Marshal(st)
}

func (c *ModifyTiFlashCase) Tick(ctx workload.TickContext, raw json.RawMessage) error {
	var st modifyTiFlashState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	if st.N <= 0 {
		st.N = 100
	}
	if st.NAP <= 0 {
		st.NAP = 2
	}

	tickNo := st.Ticked + 1

	if _, err := ctx.DB.ExecContext(ctx, "INSERT INTO "+workload.QTable(st.DB, st.Table)+" (a,b,c) VALUES (?,?,?)",
		int64(st.Inserted), int64(st.Inserted*7+1), int64(st.Inserted*11+2),
	); err != nil {
		return err
	}
	st.Inserted++

	if workload.EveryNTick(tickNo, st.N) {
		max := st.NAP
		if max > 0 {
			next := tickNo % (max + 1)
			if next == st.Replica {
				next = (next + 1) % (max + 1)
			}
			stmt := fmt.Sprintf("ALTER TABLE %s SET TIFLASH REPLICA %d", workload.QTable(st.DB, st.Table), next)
			if _, err := ctx.DB.ExecContext(ctx, stmt); err != nil {
				return err
			}
			st.Replica = next
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

func (c *ModifyTiFlashCase) Exit(ctx workload.ExitContext, raw json.RawMessage) error {
	var st modifyTiFlashState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}

	sum, err := workload.AdminChecksumTable(ctx, ctx.DB, st.DB, st.Table)
	if err != nil {
		return err
	}
	replica, err := workload.TiFlashReplicaCount(ctx, ctx.DB, st.DB, st.Table)
	if err != nil {
		return err
	}
	st.Checksum = sum
	st.Replica = replica
	st.LogDone = true

	updated, err := json.Marshal(st)
	if err != nil {
		return err
	}
	ctx.UpdateState(updated)
	return nil
}

func (c *ModifyTiFlashCase) Verify(ctx workload.Context, raw json.RawMessage) error {
	var st modifyTiFlashState
	if err := json.Unmarshal(raw, &st); err != nil {
		return err
	}
	if err := workload.Require(st.LogDone, "ModifyTiFlash: log not executed"); err != nil {
		return err
	}
	if err := workload.Require(st.Checksum.TotalKvs != "", "ModifyTiFlash: checksum not recorded; run Exit first"); err != nil {
		return err
	}

	sum, err := workload.AdminChecksumTable(ctx, ctx.DB, st.DB, st.Table)
	if err != nil {
		return err
	}
	if err := workload.Require(sum.TotalKvs == st.Checksum.TotalKvs, "ModifyTiFlash: Total_kvs mismatch: got %q want %q", sum.TotalKvs, st.Checksum.TotalKvs); err != nil {
		return err
	}
	if st.Checksum.TotalBytes != "" {
		if err := workload.Require(sum.TotalBytes == st.Checksum.TotalBytes, "ModifyTiFlash: Total_bytes mismatch: got %q want %q", sum.TotalBytes, st.Checksum.TotalBytes); err != nil {
			return err
		}
	}

	replica, err := workload.TiFlashReplicaCount(ctx, ctx.DB, st.DB, st.Table)
	if err != nil {
		return err
	}
	return workload.Require(replica == st.Replica, "ModifyTiFlash: tiflash replica mismatch: got %d want %d", replica, st.Replica)
}
