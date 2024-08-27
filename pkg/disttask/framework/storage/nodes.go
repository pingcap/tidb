// Copyright 2024 PingCAP, Inc.
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

package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// InitMeta insert the manager information into dist_framework_meta.
func (mgr *TaskManager) InitMeta(ctx context.Context, tidbID string, role string) error {
	return mgr.WithNewSession(func(se sessionctx.Context) error {
		return mgr.InitMetaSession(ctx, se, tidbID, role)
	})
}

// InitMetaSession insert the manager information into dist_framework_meta.
// if the record exists, update the cpu_count and role.
func (*TaskManager) InitMetaSession(ctx context.Context, se sessionctx.Context, execID string, role string) error {
	cpuCount := cpu.GetCPUCount()
	_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
		insert into mysql.dist_framework_meta(host, role, cpu_count, keyspace_id)
		values (%?, %?, %?, -1)
		on duplicate key
		update cpu_count = %?, role = %?`,
		execID, role, cpuCount, cpuCount, role)
	return err
}

// RecoverMeta insert the manager information into dist_framework_meta.
// if the record exists, update the cpu_count.
// Don't update role for we only update it in `set global tidb_service_scope`.
// if not there might has a data race.
func (mgr *TaskManager) RecoverMeta(ctx context.Context, execID string, role string) error {
	cpuCount := cpu.GetCPUCount()
	_, err := mgr.ExecuteSQLWithNewSession(ctx, `
		insert into mysql.dist_framework_meta(host, role, cpu_count, keyspace_id)
		values (%?, %?, %?, -1)
		on duplicate key
		update cpu_count = %?`,
		execID, role, cpuCount, cpuCount)
	return err
}

// DeleteDeadNodes deletes the dead nodes from mysql.dist_framework_meta.
func (mgr *TaskManager) DeleteDeadNodes(ctx context.Context, nodes []string) error {
	if len(nodes) == 0 {
		return nil
	}
	return mgr.WithNewTxn(ctx, func(se sessionctx.Context) error {
		deleteSQL := new(strings.Builder)
		if err := sqlescape.FormatSQL(deleteSQL, "delete from mysql.dist_framework_meta where host in("); err != nil {
			return err
		}
		deleteElems := make([]string, 0, len(nodes))
		for _, node := range nodes {
			deleteElems = append(deleteElems, fmt.Sprintf(`"%s"`, node))
		}

		deleteSQL.WriteString(strings.Join(deleteElems, ", "))
		deleteSQL.WriteString(")")
		_, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), deleteSQL.String())
		return err
	})
}

// GetAllNodes gets nodes in dist_framework_meta.
func (mgr *TaskManager) GetAllNodes(ctx context.Context) ([]proto.ManagedNode, error) {
	var nodes []proto.ManagedNode
	err := mgr.WithNewSession(func(se sessionctx.Context) error {
		var err2 error
		nodes, err2 = mgr.getAllNodesWithSession(ctx, se)
		return err2
	})
	return nodes, err
}

func (*TaskManager) getAllNodesWithSession(ctx context.Context, se sessionctx.Context) ([]proto.ManagedNode, error) {
	rs, err := sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), `
		select host, role, cpu_count
		from mysql.dist_framework_meta
		order by host`)
	if err != nil {
		return nil, err
	}
	nodes := make([]proto.ManagedNode, 0, len(rs))
	for _, r := range rs {
		nodes = append(nodes, proto.ManagedNode{
			ID:       r.GetString(0),
			Role:     r.GetString(1),
			CPUCount: int(r.GetInt64(2)),
		})
	}
	return nodes, nil
}

// GetUsedSlotsOnNodes implements the scheduler.TaskManager interface.
func (mgr *TaskManager) GetUsedSlotsOnNodes(ctx context.Context) (map[string]int, error) {
	// concurrency of subtasks of some step is the same, we use max(concurrency)
	// to make group by works.
	rs, err := mgr.ExecuteSQLWithNewSession(ctx, `
		select
			exec_id, sum(concurrency)
		from (
			select exec_id, task_key, max(concurrency) concurrency
			from mysql.tidb_background_subtask
			where state in (%?, %?)
			group by exec_id, task_key
		) a
		group by exec_id`,
		proto.SubtaskStatePending, proto.SubtaskStateRunning,
	)
	if err != nil {
		return nil, err
	}

	slots := make(map[string]int, len(rs))
	for _, r := range rs {
		val, _ := r.GetMyDecimal(1).ToInt()
		slots[r.GetString(0)] = int(val)
	}
	return slots, nil
}

// GetCPUCountOfNode gets the cpu count of node.
func (mgr *TaskManager) GetCPUCountOfNode(ctx context.Context) (int, error) {
	var cnt int
	err := mgr.WithNewSession(func(se sessionctx.Context) error {
		var err2 error
		cnt, err2 = mgr.getCPUCountOfNode(ctx, se)
		return err2
	})
	return cnt, err
}

// getCPUCountOfNode gets the cpu count of managed node.
// returns error when there's no node or no node has valid cpu count.
func (mgr *TaskManager) getCPUCountOfNode(ctx context.Context, se sessionctx.Context) (int, error) {
	nodes, err := mgr.getAllNodesWithSession(ctx, se)
	if err != nil {
		return 0, err
	}
	if len(nodes) == 0 {
		return 0, errors.New("no managed nodes")
	}
	var cpuCount int
	for _, n := range nodes {
		if n.CPUCount > 0 {
			cpuCount = n.CPUCount
			break
		}
	}
	if cpuCount == 0 {
		return 0, errors.New("no managed node have enough resource for dist task")
	}
	return cpuCount, nil
}
