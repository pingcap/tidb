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

package ddl_test

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
	tikv "github.com/tikv/client-go/v2/tikv"
	pdhttp "github.com/tikv/pd/client/http"
)

// Unit tests for keyrange building

type mockCodec struct {
	tikv.Codec
}

type truncateAffinityPDClient struct {
	pdhttp.Client
	manager           affinity.Manager
	failCreate        atomic.Bool
	failDelete        atomic.Bool
	failOnce          atomic.Bool
	failAfterCreate   atomic.Bool
	lastCreatedGroups atomic.Value
	createCalls       atomic.Int64
	createFailures    atomic.Int64
	nilDeleteContext  atomic.Bool
	// labelRules maps a group ID to its key ranges. PD stores them in a label
	// rule that is created and force-deleted together with the group.
	labelRules sync.Map
	// partialCreateLimit, when positive, makes a creation request commit at
	// most that many of its groups and then fail, after partialCreateSkip
	// requests have succeeded. partialCreatedIDs records the committed IDs.
	partialCreateLimit atomic.Int64
	partialCreateSkip  atomic.Int64
	partialCreatedIDs  atomic.Value
	// everCreated records every group ID PD has committed, including deleted ones.
	everCreated sync.Map
}

func (c *truncateAffinityPDClient) requireEverCreated(t *testing.T, groups map[string][]pdhttp.AffinityGroupKeyRange) {
	require.NotEmpty(t, groups)
	for id := range groups {
		_, ok := c.everCreated.Load(id)
		require.True(t, ok, "group %s must have been created before the job was cancelled", id)
	}
}

func (c *truncateAffinityPDClient) labelRule(id string) ([]pdhttp.AffinityGroupKeyRange, bool) {
	ranges, ok := c.labelRules.Load(id)
	if !ok {
		return nil, false
	}
	return ranges.([]pdhttp.AffinityGroupKeyRange), true
}

func (c *truncateAffinityPDClient) CreateAffinityGroups(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange, _ ...pdhttp.CreateAffinityGroupsOption) (map[string]*pdhttp.AffinityGroupState, error) {
	c.createCalls.Add(1)
	if c.failCreate.Load() || c.failOnce.Swap(false) {
		c.createFailures.Add(1)
		return nil, fmt.Errorf("injected affinity creation failure")
	}
	if c.partialCreateLimit.Load() > 0 && c.partialCreateSkip.Add(-1) < 0 {
		return nil, c.createPartially(ctx, groups, int(c.partialCreateLimit.Swap(0)))
	}
	err := c.manager.CreateAffinityGroupsIfNotExists(ctx, groups)
	if err == nil {
		for id, ranges := range groups {
			c.labelRules.LoadOrStore(id, ranges)
			c.everCreated.Store(id, struct{}{})
		}
		c.lastCreatedGroups.Store(groups)
		if c.failAfterCreate.Swap(false) {
			return nil, fmt.Errorf("injected response loss after affinity creation")
		}
	}
	return nil, err
}

// createPartially commits the first limit groups in ID order and then fails.
// PD does this when a request spans several etcd transactions and a later one
// fails after the earlier ones have committed.
func (c *truncateAffinityPDClient) createPartially(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange, limit int) error {
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	committedIDs := ids[:min(limit, len(ids))]
	committed := make(map[string][]pdhttp.AffinityGroupKeyRange, len(committedIDs))
	for _, id := range committedIDs {
		committed[id] = groups[id]
	}
	if err := c.manager.CreateAffinityGroupsIfNotExists(ctx, committed); err != nil {
		return err
	}
	for id, ranges := range committed {
		c.labelRules.LoadOrStore(id, ranges)
		c.everCreated.Store(id, struct{}{})
	}
	c.partialCreatedIDs.Store(committedIDs)
	return fmt.Errorf("injected affinity creation failure after %d of %d groups", len(committedIDs), len(ids))
}

func (c *truncateAffinityPDClient) BatchDeleteAffinityGroups(ctx context.Context, ids []string, _ bool) error {
	// Match the real HTTP client's context validation; the mock manager ignores it.
	if _, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://pd/affinity-groups", nil); err != nil {
		c.nilDeleteContext.Store(ctx == nil)
		return err
	}
	if c.failDelete.Load() {
		return fmt.Errorf("injected affinity deletion failure")
	}
	if err := c.manager.DeleteAffinityGroups(ctx, ids); err != nil {
		return err
	}
	for _, id := range ids {
		c.labelRules.Delete(id)
	}
	return nil
}

func (c *truncateAffinityPDClient) GetAffinityGroups(ctx context.Context, ids []string) (map[string]*pdhttp.AffinityGroupState, error) {
	return c.manager.GetAffinityGroups(ctx, ids)
}

type affinityDDLTest struct {
	store    kv.Storage
	dom      *domain.Domain
	cli      *truncateAffinityPDClient
	tk       *testkit.TestKit
	tkCancel *testkit.TestKit
	schemaID int64
}

func newAffinityDDLTest(t *testing.T) *affinityDDLTest {
	t.Helper()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := &truncateAffinityPDClient{manager: affinity.NewMockManager()}
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })
	waitTime := ddl.GetWaitTimeWhenErrorOccurred()
	ddl.SetWaitTimeWhenErrorOccurred(time.Millisecond)
	t.Cleanup(func() { ddl.SetWaitTimeWhenErrorOccurred(waitTime) })
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)
	return &affinityDDLTest{store, dom, cli, tk, testkit.NewTestKit(t, store), db.ID}
}

func (s *affinityDDLTest) tableInfo(t *testing.T, name string) *model.TableInfo {
	t.Helper()
	tbl, err := s.dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(name))
	require.NoError(t, err)
	return tbl.Meta().Clone()
}

func (s *affinityDDLTest) persisted(t *testing.T, id int64) *model.TableInfo {
	t.Helper()
	info, err := meta.NewReader(s.store.GetSnapshot(kv.MaxVersion)).GetTable(s.schemaID, id)
	require.NoError(t, err)
	return info
}

func (s *affinityDDLTest) groupsOf(t *testing.T, info *model.TableInfo) map[string][]pdhttp.AffinityGroupKeyRange {
	t.Helper()
	groups, err := ddl.BuildAffinityGroupDefinitionsForTest(s.store.GetCodec(), info, nil)
	require.NoError(t, err)
	return groups
}

func TestAffinityTruncatePartitionCreateResponseLoss(t *testing.T) {
	for _, failDelete := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete_failure=%v", failDelete), func(t *testing.T) {
			s := newAffinityDDLTest(t)
			s.tk.MustExec("create table p(a int) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
			s.tk.MustExec("insert into p values (1),(11)")
			before := s.tableInfo(t, "p")
			persistedBefore := s.persisted(t, before.ID)
			originalGroups := s.groupsOf(t, before)
			s.cli.failAfterCreate.Store(true)
			s.cli.failDelete.Store(failDelete)
			require.ErrorContains(t, s.tk.ExecToErr("alter table p truncate partition p0"), "injected response loss")
			s.cli.failDelete.Store(false)
			require.Equal(t, before, s.tableInfo(t, "p"))
			require.Equal(t, persistedBefore, s.persisted(t, before.ID))
			s.tk.MustQuery("select * from p order by a").Check(testkit.Rows("1", "11"))
			requireAffinityGroupsWithLabelRules(t, s.cli, originalGroups, true, "cleanup must preserve both original partition groups")
			replacementGroups := make(map[string][]pdhttp.AffinityGroupKeyRange)
			for id, ranges := range s.cli.lastCreatedGroups.Load().(map[string][]pdhttp.AffinityGroupKeyRange) {
				if _, original := originalGroups[id]; !original {
					replacementGroups[id] = ranges
				}
			}
			require.Len(t, replacementGroups, 1)
			requireAffinityGroupsWithLabelRules(t, s.cli, replacementGroups, failDelete, "failed cleanup must not prevent cancellation")
		})
	}
}

func TestAffinityTruncateTableFailureAtomicity(t *testing.T) {
	for _, withPolicy := range []bool{false, true} {
		t.Run(fmt.Sprintf("masking_policy=%v", withPolicy), func(t *testing.T) {
			s := newAffinityDDLTest(t)
			s.tk.MustExec("create table t(a int) affinity='table'")
			s.tk.MustExec("insert into t values (1)")
			if withPolicy {
				s.tk.MustExec("create masking policy mp on t(a) as a")
			}
			before := s.tableInfo(t, "t")
			persistedBefore := s.persisted(t, before.ID)
			for _, afterCreate := range []bool{false, true} {
				t.Run(fmt.Sprintf("response_loss=%v", afterCreate), func(t *testing.T) {
					s.cli.failCreate.Store(!afterCreate)
					s.cli.failAfterCreate.Store(afterCreate)
					failure := "injected affinity creation failure"
					if afterCreate {
						failure = "injected response loss after affinity creation"
					}
					require.ErrorContains(t, s.tk.ExecToErr("truncate table t"), failure)
					s.cli.failCreate.Store(false)
					require.Equal(t, persistedBefore, s.persisted(t, before.ID), "failed truncate must preserve persistent table metadata")
					tables, err := meta.NewReader(s.store.GetSnapshot(kv.MaxVersion)).ListTables(context.Background(), s.schemaID)
					require.NoError(t, err)
					for _, tbl := range tables {
						if tbl.Name.L == "t" {
							require.Equal(t, before.ID, tbl.ID, "failed truncate must not persist a replacement table")
						}
					}
					if withPolicy {
						s.tk.MustQuery("select table_id from mysql.tidb_masking_policy where policy_name='mp'").Check(testkit.Rows(fmt.Sprint(before.ID)))
					}
					s.tk.MustQuery("select * from t").Check(testkit.Rows("1"))
					requireAffinityGroupsWithLabelRules(t, s.cli, s.groupsOf(t, before), true, "failed truncate must keep the original groups")
					if afterCreate {
						replacementGroups := s.cli.lastCreatedGroups.Load().(map[string][]pdhttp.AffinityGroupKeyRange)
						requireAffinityGroupsWithLabelRules(t, s.cli, replacementGroups, false, "failed truncate must clean up new table groups")
					}
				})
			}
			s.tk.MustExec("truncate table t")
			s.tk.MustQuery("select * from t").Check(testkit.Rows())
		})
	}
}

// requireAffinityGroupsWithLabelRules checks both the groups and the label
// rules that hold their key ranges.
func requireAffinityGroupsWithLabelRules(t *testing.T, cli *truncateAffinityPDClient, groups map[string][]pdhttp.AffinityGroupKeyRange, shouldExist bool, msg string) {
	require.NotEmpty(t, groups, msg)
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	states, err := affinity.GetGroups(context.Background(), ids)
	require.NoError(t, err, msg)
	for id, ranges := range groups {
		rule, ok := cli.labelRule(id)
		if shouldExist {
			require.Contains(t, states, id, msg)
			require.True(t, ok, "label rule of group %s should exist: %s", id, msg)
			require.Equal(t, ranges, rule, msg)
		} else {
			require.NotContains(t, states, id, msg)
			require.False(t, ok, "label rule of group %s should be deleted: %s", id, msg)
		}
	}
}

// failAndCancel injects a failure after PD group creation. Most cases cancel
// the retry through a separate session; event publication uses the error limit,
// and truncateTableErr cancels the retry before it reaches PD.
func (s *affinityDDLTest) failAndCancel(t *testing.T, point string, match func(*model.Job) bool, run func() error) error {
	t.Helper()
	const prefix = "github.com/pingcap/tidb/pkg/ddl/"
	var failed, cancelled atomic.Bool
	cancelDone := make(chan error, 1)
	inject := func(job *model.Job, err *error) {
		if match(job) && failed.CompareAndSwap(false, true) {
			*err = fmt.Errorf("injected schema version update failure")
		}
	}
	switch point {
	case "asyncNotifyEventError":
		s.tk.MustExec("set @@global.tidb_ddl_error_count_limit = 1")
		defer s.tk.MustExec("set @@global.tidb_ddl_error_count_limit = default")
		testfailpoint.Enable(t, prefix+point, "return")
	case "truncateTableErr":
		testfailpoint.EnableCall(t, prefix+"beforeUpdateSchemaVersion", inject)
		defer testfailpoint.Disable(t, prefix+"beforeUpdateSchemaVersion")
		testfailpoint.Enable(t, prefix+point, "1*return(false)->return(true)")
	default:
		testfailpoint.EnableCall(t, prefix+point, inject)
		testfailpoint.EnableCall(t, prefix+"beforeRefreshJob", func(job *model.Job) {
			if match(job) && failed.Load() && cancelled.CompareAndSwap(false, true) {
				errs, err := ddl.CancelJobs(context.Background(), s.tkCancel.Session(), []int64{job.ID})
				if err == nil && len(errs) > 0 {
					err = errs[0]
				}
				cancelDone <- err
			}
		})
		defer testfailpoint.Disable(t, prefix+"beforeRefreshJob")
	}
	ddlErr := run()
	testfailpoint.Disable(t, prefix+point)
	if point != "asyncNotifyEventError" {
		require.True(t, failed.Load(), "the failed attempt must precede cancellation")
		if point != "truncateTableErr" {
			require.True(t, cancelled.Load(), "the retry must be cancelled by the user")
			require.NoError(t, <-cancelDone)
		}
	}
	return ddlErr
}

func TestAffinityTruncateTableCancelKeepsOriginalGroups(t *testing.T) {
	s := newAffinityDDLTest(t)
	for _, level := range []string{"table", "partition"} {
		for _, failure := range []struct {
			point   string
			wantErr string
		}{
			{"beforeUpdateSchemaVersion", "injected schema version update failure"},
			{"asyncNotifyEventError", "mock publish event error"},
			{"afterUpdateJobToTable", "Cancelled DDL job"},
			{"truncateTableErr", "occur an error after dropping table"},
		} {
			if failure.point == "truncateTableErr" && level != "partition" {
				continue
			}
			t.Run(level+"/"+failure.point, func(t *testing.T) {
				s.tk.MustExec("drop table if exists t")
				createSQL := "create table t(a int) affinity='table'"
				if level == "partition" {
					createSQL = "create table t(a int) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)"
				}
				s.tk.MustExec(createSQL)
				s.tk.MustExec("insert into t values (1),(11)")
				before := s.tableInfo(t, "t")
				persistedBefore := s.persisted(t, before.ID)
				originalGroups := s.groupsOf(t, before)
				err := s.failAndCancel(t, failure.point, func(job *model.Job) bool {
					return job.Type == model.ActionTruncateTable && job.TableID == before.ID
				}, func() error { return s.tk.ExecToErr("truncate table t") })
				require.ErrorContains(t, err, failure.wantErr)
				require.Equal(t, persistedBefore, s.persisted(t, before.ID), "cancelled truncate must preserve table metadata")
				s.tk.MustQuery("select * from t order by a").Check(testkit.Rows("1", "11"))
				requireAffinityGroupsWithLabelRules(t, s.cli, originalGroups, true, "cancelled truncate must keep the original groups")
				replacementGroups := s.cli.lastCreatedGroups.Load().(map[string][]pdhttp.AffinityGroupKeyRange)
				require.Len(t, replacementGroups, len(originalGroups))
				for id := range replacementGroups {
					require.NotContains(t, originalGroups, id, "the failed attempt must create replacement groups")
				}
				requireAffinityGroupsWithLabelRules(t, s.cli, replacementGroups, false, "cancelled truncate must delete the replacement groups")

				s.tk.MustExec("truncate table t")
				s.tk.MustQuery("select * from t").Check(testkit.Rows())
				requireAffinityGroupsWithLabelRules(t, s.cli, s.groupsOf(t, s.tableInfo(t, "t")), true, "committed truncate must create the new groups")
				requireAffinityGroupsWithLabelRules(t, s.cli, originalGroups, false, "committed truncate must delete the original groups")
			})
		}
	}
}

func (s *affinityDDLTest) captureCreateJob(t *testing.T, tp model.ActionType) func() []*model.TableInfo {
	t.Helper()
	var captured atomic.Pointer[model.Job]
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
		if job.Type == tp && job.SchemaName == "test" && captured.Load() == nil {
			captured.Store(&model.Job{Type: tp, Version: job.Version, RawArgs: append([]byte(nil), job.RawArgs...)})
		}
	})
	return func() []*model.TableInfo {
		job := captured.Load()
		require.NotNil(t, job, "the create job must have run")
		if tp == model.ActionCreateTables {
			args, err := model.GetBatchCreateTableArgs(job)
			require.NoError(t, err)
			infos := make([]*model.TableInfo, 0, len(args.Tables))
			for _, a := range args.Tables {
				infos = append(infos, a.TableInfo)
			}
			return infos
		}
		args, err := model.GetCreateTableArgs(job)
		require.NoError(t, err)
		return []*model.TableInfo{args.TableInfo}
	}
}

// TestAffinityCancelledDDLGroupCleanup covers partial PD creation, a later
// step failure followed by cancellation, and cancellation within the step.
func TestAffinityCancelledDDLGroupCleanup(t *testing.T) {
	s := newAffinityDDLTest(t)
	const partitions = "partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)"
	withAffinity := func(info *model.TableInfo, level string) *model.TableInfo {
		info = info.Clone()
		info.Affinity = &model.TableAffinityInfo{Level: level}
		return info
	}
	failCreation := func(skip int64) {
		s.cli.partialCreatedIDs.Store([]string(nil))
		s.cli.partialCreateSkip.Store(skip)
		s.cli.partialCreateLimit.Store(1)
	}
	requirePartiallyCreated := func(t *testing.T, groups map[string][]pdhttp.AffinityGroupKeyRange) {
		require.Zero(t, s.cli.partialCreateLimit.Load(), "the partial creation failure must have been injected")
		committed := s.cli.partialCreatedIDs.Load().([]string)
		require.NotEmpty(t, committed)
		for _, id := range committed {
			require.Contains(t, groups, id)
		}
	}

	for _, tc := range []struct {
		level   string
		delayed bool
	}{
		{level: ast.TableAffinityLevelTable},
		{level: ast.TableAffinityLevelPartition},
		{level: ast.TableAffinityLevelPartition, delayed: true},
	} {
		t.Run(fmt.Sprintf("create/%s/delayed=%v", tc.level, tc.delayed), func(t *testing.T) {
			s.tk.MustExec("drop table if exists c")
			createSQL := fmt.Sprintf("create table c(a int) affinity='%s'", tc.level)
			if tc.level == ast.TableAffinityLevelPartition {
				createSQL += " " + partitions
			}
			createdTables := s.captureCreateJob(t, model.ActionCreateTable)
			run := func() error { return s.tk.ExecToErr(createSQL) }
			if tc.delayed {
				require.ErrorContains(t, s.failAndCancel(t, "beforeUpdateSchemaVersion", func(job *model.Job) bool {
					return job.Type == model.ActionCreateTable
				}, run), "injected schema version update failure")
			} else {
				failCreation(0)
				require.ErrorContains(t, run(), "injected affinity creation failure after 1 of")
			}
			groups := s.groupsOf(t, createdTables()[0])
			if tc.delayed {
				s.cli.requireEverCreated(t, groups)
			} else {
				requirePartiallyCreated(t, groups)
			}
			require.Nil(t, s.persisted(t, createdTables()[0].ID))
			require.ErrorContains(t, s.tk.ExecToErr("select * from c"), "doesn't exist")
			requireAffinityGroupsWithLabelRules(t, s.cli, groups, false, "cancelled CREATE must delete unused groups")
			if !tc.delayed {
				s.tk.MustExec(createSQL)
				checkAffinityGroupsInPD(t, s.dom, "test", "c", true)
			}
		})
	}

	s.tk.MustExec("create table src_t(a int) affinity='table'")
	s.tk.MustExec("create table src_p(a int) affinity='partition' " + partitions)
	for _, failure := range []string{"partial creation", "cancel after failure", "failing sequence"} {
		t.Run("batch/"+failure, func(t *testing.T) {
			s.tk.MustExec("drop table if exists b1, b2")
			infos := make([]*model.TableInfo, 0, 2)
			for _, names := range [][2]string{{"src_t", "b1"}, {"src_p", "b2"}} {
				info, err := ddl.BuildTableInfoWithLike(ast.Ident{Name: ast.NewCIStr(names[1])}, s.tableInfo(t, names[0]), &ast.CreateTableStmt{})
				require.NoError(t, err)
				infos = append(infos, info)
			}
			if failure == "failing sequence" {
				s.tk.MustExec("create sequence s0")
				badSeq := s.tableInfo(t, "s0")
				badSeq.ID, badSeq.Name, badSeq.State = 0, ast.NewCIStr("s_bad"), model.StateDeleteOnly
				infos[1] = badSeq
			}
			createdTables := s.captureCreateJob(t, model.ActionCreateTables)
			run := func() error {
				// This API creates jobs without executing a SQL statement.
				s.tk.Session().SetValue(sessionctx.QueryString, "skip")
				return s.dom.DDLExecutor().BatchCreateTableWithInfo(s.tk.Session(), ast.NewCIStr("test"), infos, ddl.WithOnExist(ddl.OnExistError))
			}
			var err error
			switch failure {
			case "partial creation":
				// b1 succeeds, then b2 commits one of its two groups and fails.
				failCreation(1)
				err = run()
				require.ErrorContains(t, err, "injected affinity creation failure after 1 of 2")
			case "cancel after failure":
				err = s.failAndCancel(t, "beforeUpdateSchemaVersion", func(job *model.Job) bool {
					return job.Type == model.ActionCreateTables
				}, run)
				require.ErrorContains(t, err, "injected schema version update failure")
			default:
				require.ErrorContains(t, run(), "sequence")
			}
			created := createdTables()
			require.Len(t, created, 2)
			for i, info := range created {
				if info.Affinity == nil {
					continue
				}
				groups := s.groupsOf(t, info)
				if failure == "partial creation" && i == 1 {
					requirePartiallyCreated(t, groups)
				} else {
					s.cli.requireEverCreated(t, groups)
				}
				require.Nil(t, s.persisted(t, info.ID))
				require.ErrorContains(t, s.tk.ExecToErr("select * from "+info.Name.L), "doesn't exist")
				requireAffinityGroupsWithLabelRules(t, s.cli, groups, false, "cancelled batch must delete groups of all its tables")
			}
			for _, name := range []string{"src_t", "src_p"} {
				requireAffinityGroupsWithLabelRules(t, s.cli, s.groupsOf(t, s.tableInfo(t, name)), true, "source tables must keep their groups")
			}
		})
	}

	for _, tc := range []struct {
		name     string
		oldLevel string
		newLevel string
	}{
		{name: "none to partition", newLevel: ast.TableAffinityLevelPartition},
		{name: "partition to none", oldLevel: ast.TableAffinityLevelPartition},
		{name: "table to partition", oldLevel: ast.TableAffinityLevelTable, newLevel: ast.TableAffinityLevelPartition},
		{name: "unchanged table", oldLevel: ast.TableAffinityLevelTable, newLevel: ast.TableAffinityLevelTable},
		{name: "unchanged partition", oldLevel: ast.TableAffinityLevelPartition, newLevel: ast.TableAffinityLevelPartition},
	} {
		for _, delayed := range []bool{false, true} {
			if (!delayed && tc.newLevel == "") || (delayed && tc.oldLevel == tc.newLevel) {
				continue
			}
			t.Run(fmt.Sprintf("alter/%s/delayed=%v", tc.name, delayed), func(t *testing.T) {
				s.tk.MustExec("drop table if exists a")
				createSQL := "create table a(a int) " + partitions
				if tc.newLevel == ast.TableAffinityLevelTable {
					createSQL = "create table a(a int)"
				}
				s.tk.MustExec(createSQL)
				before := s.tableInfo(t, "a")
				if tc.oldLevel != "" {
					// SQL rejects table affinity on a partitioned table; write the
					// old level directly so the level-switch cleanup is exercised.
					before = withAffinity(before, tc.oldLevel)
					ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
					require.NoError(t, kv.RunInNewTxn(ctx, s.store, true, func(_ context.Context, txn kv.Transaction) error {
						return meta.NewMutator(txn).UpdateTable(s.schemaID, before)
					}))
					require.NoError(t, affinity.CreateGroupsIfNotExists(context.Background(), s.groupsOf(t, before)))
				}
				before = s.persisted(t, before.ID)
				oldGroups := s.groupsOf(t, before)
				var newGroups map[string][]pdhttp.AffinityGroupKeyRange
				if tc.newLevel != "" {
					newGroups = s.groupsOf(t, withAffinity(before, tc.newLevel))
				}
				alterSQL := fmt.Sprintf("alter table a affinity='%s'", tc.newLevel)
				run := func() error { return s.tk.ExecToErr(alterSQL) }
				if delayed {
					err := s.failAndCancel(t, "beforeUpdateSchemaVersion", func(job *model.Job) bool {
						return job.Type == model.ActionAlterTableAffinity && job.TableID == before.ID
					}, run)
					require.ErrorContains(t, err, "injected schema version update failure")
					if newGroups != nil {
						s.cli.requireEverCreated(t, newGroups)
					}
				} else {
					failCreation(0)
					require.ErrorContains(t, run(), "injected affinity creation failure after 1 of")
					requirePartiallyCreated(t, newGroups)
				}
				require.Equal(t, before, s.persisted(t, before.ID), "cancelled ALTER must keep its metadata")
				if len(oldGroups) > 0 {
					requireAffinityGroupsWithLabelRules(t, s.cli, oldGroups, true, "cancelled ALTER must keep its original groups")
				}
				if newGroups != nil {
					requireAffinityGroupsWithLabelRules(t, s.cli, newGroups, tc.oldLevel == tc.newLevel, "cancelled ALTER must delete only groups of a new level")
				}
				if tc.oldLevel != tc.newLevel {
					s.tk.MustExec(alterSQL)
					if newGroups != nil {
						requireAffinityGroupsWithLabelRules(t, s.cli, newGroups, true, "committed ALTER must create the new groups")
					}
					if len(oldGroups) > 0 {
						requireAffinityGroupsWithLabelRules(t, s.cli, oldGroups, false, "committed ALTER must delete the old groups")
					}
				}
			})
		}
	}

	t.Run("create with foreign key keeps persisted groups", func(t *testing.T) {
		s.tk.MustExec("create table parent(a int primary key)")
		var cancelled atomic.Bool
		var tableID atomic.Int64
		cancelDone := make(chan error, 1)
		const beforeStep = "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep"
		testfailpoint.EnableCall(t, beforeStep, func(job *model.Job) {
			if job.Type == model.ActionCreateTable && job.TableName == "child" && job.SchemaState == model.StateDeleteOnly && cancelled.CompareAndSwap(false, true) {
				tableID.Store(job.TableID)
				errs, err := ddl.CancelJobs(context.Background(), s.tkCancel.Session(), []int64{job.ID})
				if err == nil && len(errs) > 0 {
					err = errs[0]
				}
				cancelDone <- err
			}
		})
		err := s.tk.ExecToErr("create table child(a int, foreign key(a) references parent(a)) affinity='table'")
		testfailpoint.Disable(t, beforeStep)
		require.ErrorContains(t, err, "Cancelled DDL job")
		require.True(t, cancelled.Load())
		require.NoError(t, <-cancelDone)
		info := s.persisted(t, tableID.Load())
		require.NotNil(t, info, "the first FK CREATE step has already persisted the table")
		require.Equal(t, model.StateDeleteOnly, info.State)
		groups := s.groupsOf(t, info)
		s.cli.requireEverCreated(t, groups)
		requireAffinityGroupsWithLabelRules(t, s.cli, groups, true, "cancelled FK CREATE must keep groups used by persisted metadata")
	})
}

func TestAffinityTruncatePartitionFailure(t *testing.T) {
	s := newAffinityDDLTest(t)
	store, dom, cli, tk := s.store, s.dom, s.cli, s.tk
	tk.MustExec("create table p(a int, b int, unique key idx(b) global) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	tk.MustExec("insert into p values (1,1),(2,2),(11,11),(12,12)")
	before, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("p"))
	require.NoError(t, err)
	beforeMeta := before.Meta().Clone()
	cli.failCreate.Store(true)
	require.ErrorContains(t, tk.ExecToErr("alter table p truncate partition p0"), "injected affinity creation failure")
	cli.failCreate.Store(false)
	after, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("p"))
	require.NoError(t, err)
	require.Equal(t, beforeMeta, after.Meta().Clone(), "failed affinity creation must not publish new partition IDs or DDL state")
	tk.MustQuery("select * from p order by a").Check(testkit.Rows("1 1", "2 2", "11 11", "12 12"))
	tk.MustQuery("select * from p use index(idx) order by a").Check(testkit.Rows("1 1", "2 2", "11 11", "12 12"))
	tk.MustExec("admin check table p")
	checkAffinityGroupsInPD(t, dom, "test", "p", true)

	// A failure after replacement is visible must retry, never cancel the job.
	// This also protects jobs that were started by an older TiDB version.
	var injected atomic.Bool
	const beforeStep = "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep"
	testfailpoint.EnableCall(t, beforeStep, func(job *model.Job) {
		if job.Type == model.ActionTruncateTablePartition && job.SchemaState == model.StateDeleteReorganization && injected.CompareAndSwap(false, true) {
			cli.failOnce.Store(true)
		}
	})
	tk.MustExec("alter table p truncate partition p0")
	testfailpoint.Disable(t, beforeStep)
	require.True(t, injected.Load())
	require.Equal(t, int64(2), cli.createFailures.Load(), "one failure before replacement and one during finalization")
	after, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("p"))
	require.NoError(t, err)
	require.NotEqual(t, beforeMeta.Partition.Definitions[0].ID, after.Meta().Partition.Definitions[0].ID)
	require.Equal(t, beforeMeta.Partition.Definitions[1].ID, after.Meta().Partition.Definitions[1].ID)
	require.Equal(t, model.StateNone, after.Meta().Partition.DDLState)
	require.Empty(t, after.Meta().Partition.DroppingDefinitions)
	tk.MustQuery("select * from p order by a").Check(testkit.Rows("11 11", "12 12"))
	tk.MustExec("admin check table p")
	checkAffinityGroupsInPD(t, dom, "test", "p", true)
	oldID := ddl.GetPartitionAffinityGroupID(store.GetCodec(), beforeMeta.ID, beforeMeta.Partition.Definitions[0].ID)
	groups, err := affinity.GetGroups(context.Background(), []string{oldID})
	require.NoError(t, err)
	require.Empty(t, groups)
	require.Eventually(t, func() bool {
		rows := tk.MustQuery(fmt.Sprintf("select count(*) from (select end_key from mysql.gc_delete_range union all select end_key from mysql.gc_delete_range_done) ranges_to_delete where end_key='%x'", []byte(tablecodec.EncodeTablePrefix(beforeMeta.Partition.Definitions[0].ID+1)))).Rows()
		return rows[0][0] == "1"
	}, 5*time.Second, 10*time.Millisecond)
	// Both the failed attempt and the successful retry should reach PD.
	require.Greater(t, cli.createCalls.Load(), int64(2))

	for _, scenario := range []string{"delete succeeds", "delete fails", "metadata not committed"} {
		t.Run(scenario, func(t *testing.T) {
			tk.MustExec("drop table if exists pc")
			tk.MustExec("create table pc(a int) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
			tk.MustExec("insert into pc values (1),(11)")
			beforeMeta := s.tableInfo(t, "pc")
			persistedBefore := s.persisted(t, beforeMeta.ID)
			cli.failDelete.Store(scenario == "delete fails")
			t.Cleanup(func() { cli.failDelete.Store(false) })
			var cancelOnce atomic.Bool
			cancelDone := make(chan error, 1)
			var replacementID atomic.Int64
			const updateHook = "github.com/pingcap/tidb/pkg/ddl/mockUpdateVersionAndTableInfoErr"
			if scenario == "metadata not committed" {
				// Fail the first step after PD has created the replacement groups,
				// but before the replacement IDs reach persistent metadata.
				testfailpoint.Enable(t, updateHook, "return(1)")
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
					if job.TableID == beforeMeta.ID && job.SchemaState == model.StateWriteOnly {
						testfailpoint.Disable(t, updateHook)
					}
				})
			}
			testfailpoint.EnableCall(t, beforeStep, func(job *model.Job) {
				if job.TableID == beforeMeta.ID && job.Type == model.ActionTruncateTablePartition && job.SchemaState == model.StateWriteOnly && cancelOnce.CompareAndSwap(false, true) {
					args, err := model.GetTruncateTableArgs(job)
					if err != nil {
						cancelDone <- err
						return
					}
					replacementID.Store(args.NewPartitionIDs[0])
					persisted, err := meta.NewReader(store.GetSnapshot(kv.MaxVersion)).GetTable(s.schemaID, beforeMeta.ID)
					if err != nil {
						cancelDone <- err
						return
					}
					if wantEmpty := scenario == "metadata not committed"; wantEmpty != (len(persisted.Partition.NewPartitionIDs) == 0) {
						cancelDone <- fmt.Errorf("unexpected persisted replacement partition state")
						return
					}
					errs, cancelErr := ddl.CancelJobs(context.Background(), s.tkCancel.Session(), []int64{job.ID})
					if len(errs) > 0 && errs[0] != nil {
						cancelDone <- errs[0]
						return
					}
					cancelDone <- cancelErr
				}
			})
			require.Error(t, tk.ExecToErr("alter table pc truncate partition p0"))
			select {
			case cancelErr := <-cancelDone:
				require.NoError(t, cancelErr)
			default:
				require.FailNow(t, "expected partition truncate cancellation")
			}
			require.True(t, cancelOnce.Load())
			require.NotZero(t, replacementID.Load())
			var afterCancel table.Table
			require.Eventually(t, func() bool {
				afterCancel, err = dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("pc"))
				return err == nil && afterCancel.Meta().Partition.DDLState == model.StateNone
			}, 5*time.Second, 10*time.Millisecond)
			persisted, err := meta.NewReader(store.GetSnapshot(kv.MaxVersion)).GetTable(s.schemaID, beforeMeta.ID)
			require.NoError(t, err)
			require.Equal(t, persistedBefore.Partition, persisted.Partition)
			require.Equal(t, beforeMeta.Partition.Definitions, afterCancel.Meta().Partition.Definitions)
			require.Empty(t, afterCancel.Meta().Partition.NewPartitionIDs)
			require.Equal(t, model.ActionNone, afterCancel.Meta().Partition.DDLAction)
			require.False(t, cli.nilDeleteContext.Load())
			tk.MustQuery("select * from pc order by a").Check(testkit.Rows("1", "11"))
			checkAffinityGroupsInPD(t, dom, "test", "pc", true)
			newGroupID := ddl.GetPartitionAffinityGroupID(store.GetCodec(), beforeMeta.ID, replacementID.Load())
			groups, err := affinity.GetGroups(context.Background(), []string{newGroupID})
			require.NoError(t, err)
			if scenario == "delete fails" {
				require.Contains(t, groups, newGroupID, "failed cleanup must not prevent metadata rollback")
			} else {
				require.Empty(t, groups, "successful rollback must remove replacement groups")
			}
		})
	}
}

func (mockCodec) GetKeyspaceMeta() *keyspacepb.KeyspaceMeta {
	return nil
}

func (mockCodec) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	return append([]byte("k:"), start...), append([]byte("k:"), end...)
}

func TestAffinityBuildGroupDefinitionsTable(t *testing.T) {
	tbl := &model.TableInfo{
		ID:       123,
		Affinity: &model.TableAffinityInfo{Level: ast.TableAffinityLevelTable},
	}

	groups, err := ddl.BuildAffinityGroupDefinitionsForTest(&mockCodec{}, tbl, nil)
	require.NoError(t, err)
	require.Len(t, groups, 1)

	ranges := groups[ddl.GetTableAffinityGroupID(&mockCodec{}, 123)]
	require.Len(t, ranges, 1)
	require.Equal(t, pdhttp.AffinityGroupKeyRange{
		StartKey: append([]byte("k:"), tablecodec.EncodeTablePrefix(123)...),
		EndKey:   append([]byte("k:"), tablecodec.EncodeTablePrefix(124)...),
	}, ranges[0])
}

func TestAffinityGroupIDIncludesKeyspaceInNextGen(t *testing.T) {
	codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42},
	})
	require.NoError(t, err)
	otherCodec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 43},
	})
	require.NoError(t, err)

	tableID := int64(123)
	partitionID := int64(456)
	tableGroupID := ddl.GetTableAffinityGroupID(codec, tableID)
	partitionGroupID := ddl.GetPartitionAffinityGroupID(codec, tableID, partitionID)
	if kerneltype.IsNextGen() {
		require.Equal(t, "_tidb_ks42_t_123", tableGroupID)
		require.Equal(t, "_tidb_ks42_pt_123_p456", partitionGroupID)
		require.NotEqual(t, tableGroupID, ddl.GetTableAffinityGroupID(otherCodec, tableID))
		require.NotEqual(t, partitionGroupID, ddl.GetPartitionAffinityGroupID(otherCodec, tableID, partitionID))

		// PD limits group IDs to 64 characters from this charset. Check the
		// longest format with the maximum keyspace, table, and partition IDs.
		maxCodec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
			Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 16777215},
		})
		require.NoError(t, err)
		const maxID int64 = 1<<63 - 1
		maxGroupID := ddl.GetPartitionAffinityGroupID(maxCodec, maxID, maxID)
		require.Equal(t, "_tidb_ks16777215_pt_9223372036854775807_p9223372036854775807", maxGroupID)
		require.Len(t, maxGroupID, 60)
		require.Regexp(t, `^[-A-Za-z0-9_]{1,64}$`, maxGroupID)
	} else {
		require.Equal(t, "_tidb_t_123", tableGroupID)
		require.Equal(t, "_tidb_pt_123_p456", partitionGroupID)
	}

	// Check the ID and encoded range together using real codecs for two keyspaces.
	for _, targetCodec := range []tikv.Codec{codec, otherCodec} {
		wrongCodec := codec
		if targetCodec.GetKeyspaceID() == codec.GetKeyspaceID() {
			wrongCodec = otherCodec
		}
		for _, tbl := range []*model.TableInfo{
			{ID: tableID, Affinity: &model.TableAffinityInfo{Level: ast.TableAffinityLevelTable}},
			{
				ID:       tableID,
				Affinity: &model.TableAffinityInfo{Level: ast.TableAffinityLevelPartition},
				Partition: &model.PartitionInfo{
					Enable:      true,
					Definitions: []model.PartitionDefinition{{ID: partitionID}},
				},
			},
		} {
			prefix := "_tidb_"
			if kerneltype.IsNextGen() {
				prefix = fmt.Sprintf("_tidb_ks%d_", targetCodec.GetKeyspaceID())
			}
			expectedID := prefix + "t_123"
			physicalID := tableID
			if tbl.Partition != nil {
				expectedID = prefix + "pt_123_p456"
				physicalID = partitionID
			}
			groups, err := ddl.BuildAffinityGroupDefinitionsForTest(targetCodec, tbl, nil)
			require.NoError(t, err)
			require.Len(t, groups, 1)
			require.Contains(t, groups, expectedID)
			require.Len(t, groups[expectedID], 1)
			keyRange := groups[expectedID][0]
			start, end, err := targetCodec.DecodeRegionRange(keyRange.StartKey, keyRange.EndKey)
			require.NoError(t, err)
			require.Equal(t, tablecodec.EncodeTablePrefix(physicalID), kv.Key(start))
			require.Equal(t, tablecodec.EncodeTablePrefix(physicalID+1), kv.Key(end))
			_, _, err = wrongCodec.DecodeRegionRange(keyRange.StartKey, keyRange.EndKey)
			require.Error(t, err, "the range must not belong to the other keyspace")
		}
	}
}

func TestAffinityBuildGroupDefinitionsPartition(t *testing.T) {
	tbl := &model.TableInfo{
		ID:       50,
		Affinity: &model.TableAffinityInfo{Level: ast.TableAffinityLevelPartition},
		Partition: &model.PartitionInfo{
			Enable: true,
			Definitions: []model.PartitionDefinition{
				{ID: 1},
				{ID: 3},
			},
		},
	}

	groups, err := ddl.BuildAffinityGroupDefinitionsForTest(&mockCodec{}, tbl, nil)
	require.NoError(t, err)
	require.Len(t, groups, 2)

	require.Equal(t, []pdhttp.AffinityGroupKeyRange{{
		StartKey: append([]byte("k:"), tablecodec.EncodeTablePrefix(1)...),
		EndKey:   append([]byte("k:"), tablecodec.EncodeTablePrefix(2)...),
	}}, groups[ddl.GetPartitionAffinityGroupID(&mockCodec{}, 50, 1)])
	require.Equal(t, []pdhttp.AffinityGroupKeyRange{{
		StartKey: append([]byte("k:"), tablecodec.EncodeTablePrefix(3)...),
		EndKey:   append([]byte("k:"), tablecodec.EncodeTablePrefix(4)...),
	}}, groups[ddl.GetPartitionAffinityGroupID(&mockCodec{}, 50, 3)])
}

func TestAffinityBuildGroupDefinitionsPartitionMissing(t *testing.T) {
	tbl := &model.TableInfo{
		ID:       1,
		Affinity: &model.TableAffinityInfo{Level: ast.TableAffinityLevelPartition},
	}

	_, err := ddl.BuildAffinityGroupDefinitionsForTest(nil, tbl, nil)
	require.Error(t, err)
}

// PD interaction tests

type affinityGroupCheck struct {
	codec        tikv.Codec
	tableID      int64
	partitionIDs []int64
	shouldExist  bool
	rangeCount   int
	comment      string
}

func (c *affinityGroupCheck) check(t *testing.T) {
	ctx := context.Background()

	// Collect all group IDs to check
	var groupIDs []string
	if len(c.partitionIDs) == 0 {
		// Non-partitioned table
		groupIDs = []string{ddl.GetTableAffinityGroupID(c.codec, c.tableID)}
	} else {
		// Partitioned table
		for _, partID := range c.partitionIDs {
			groupIDs = append(groupIDs, ddl.GetPartitionAffinityGroupID(c.codec, c.tableID, partID))
		}
	}

	// Get affinity groups from PD
	groups, err := affinity.GetGroups(ctx, groupIDs)
	require.NoError(t, err, c.comment)

	if c.shouldExist {
		require.Equal(t, len(groupIDs), len(groups), c.comment)
		for _, id := range groupIDs {
			group, ok := groups[id]
			require.True(t, ok, "group %s should exist, comment: %s", id, c.comment)
			require.NotNil(t, group, c.comment)
			require.Equal(t, id, group.ID, c.comment)
			if c.rangeCount > 0 {
				require.Equal(t, c.rangeCount, group.RangeCount, c.comment)
			}
		}
	} else {
		require.Empty(t, groups, "groups should not exist, comment: %s", c.comment)
	}
}

func checkAffinityGroupsInPD(t *testing.T, do *domain.Domain, dbName, tbName string, shouldExist bool) {
	tblInfo, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr(dbName), ast.NewCIStr(tbName))
	require.NoError(t, err)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	require.NoError(t, kv.RunInNewTxn(ctx, do.Store(), false, func(ctx context.Context, txn kv.Transaction) error {
		_ = meta.NewMutator(txn)

		check := &affinityGroupCheck{
			codec:       do.Store().GetCodec(),
			tableID:     tblInfo.Meta().ID,
			shouldExist: shouldExist,
			rangeCount:  1,
			comment:     fmt.Sprintf("table: %s.%s", dbName, tbName),
		}

		if tblInfo.Meta().Partition != nil {
			check.partitionIDs = make([]int64, 0, len(tblInfo.Meta().Partition.Definitions))
			for _, def := range tblInfo.Meta().Partition.Definitions {
				check.partitionIDs = append(check.partitionIDs, def.ID)
			}
		}

		check.check(t)
		return nil
	}))
}

func TestAffinityPDInteraction(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	dropTable := func() {
		tk.MustExec("drop table if exists t1, t2, t3, tp1, tp2, tp3")
	}
	dropTable()
	defer dropTable()

	// Test 1: Create table with affinity='table'
	tk.MustExec("create table t1(a int) affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "test", "t1", true)

	// Test 2: Create partitioned table with affinity='partition'
	tk.MustExec("create table tp1(a int) affinity = 'partition' partition by hash(a) partitions 4")
	checkAffinityGroupsInPD(t, dom, "test", "tp1", true)

	// Test 3: Alter table to remove affinity
	tk.MustExec("alter table t1 affinity = ''")
	checkAffinityGroupsInPD(t, dom, "test", "t1", false)

	// Test 4: Alter table to add affinity back
	tk.MustExec("alter table t1 affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "test", "t1", true)

	// Test 5: Drop table should clean up affinity groups
	// Get table ID before drop
	t1Info, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	t1ID := t1Info.Meta().ID

	tk.MustExec("drop table t1")
	// Verify affinity groups are deleted
	ctx := context.Background()
	groups, err := affinity.GetGroups(ctx, []string{ddl.GetTableAffinityGroupID(dom.Store().GetCodec(), t1ID)})
	require.NoError(t, err)
	require.Empty(t, groups, "affinity groups should be deleted after dropping table")

	// Test 6: TRUNCATE TABLE should preserve affinity
	tk.MustExec("create table t2(a int) affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "test", "t2", true)

	t2Info, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	oldTableID := t2Info.Meta().ID

	tk.MustExec("truncate table t2")
	checkAffinityGroupsInPD(t, dom, "test", "t2", true)
	// Old table ID's affinity group should be deleted
	groups, err = affinity.GetGroups(ctx, []string{ddl.GetTableAffinityGroupID(dom.Store().GetCodec(), oldTableID)})
	require.NoError(t, err)
	require.Empty(t, groups, "old table's affinity groups should be deleted after truncate")

	// Test 7: TRUNCATE PARTITION should preserve affinity
	tk.MustExec("create table tp2(a int) affinity = 'partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	checkAffinityGroupsInPD(t, dom, "test", "tp2", true)

	// Get partition ID before truncate
	tblInfo, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp2"))
	require.NoError(t, err)
	oldPartitionID := tblInfo.Meta().Partition.Definitions[0].ID

	tk.MustExec("alter table tp2 truncate partition p0")
	checkAffinityGroupsInPD(t, dom, "test", "tp2", true)
	// Old partition's affinity group should be deleted
	groups, err = affinity.GetGroups(ctx, []string{ddl.GetPartitionAffinityGroupID(dom.Store().GetCodec(), tblInfo.Meta().ID, oldPartitionID)})
	require.NoError(t, err)
	require.Empty(t, groups, "old partition's affinity group should be deleted after truncate partition")

	// Test 8: ALTER TABLE AFFINITY idempotency - partition -> partition
	tk.MustExec("create table tp3(a int) affinity = 'partition' partition by hash(a) partitions 3")
	checkAffinityGroupsInPD(t, dom, "test", "tp3", true)
	// Set affinity to partition again (idempotent operation)
	tk.MustExec("alter table tp3 affinity = 'partition'")
	checkAffinityGroupsInPD(t, dom, "test", "tp3", true)

	// Test 9: ALTER TABLE AFFINITY idempotency - table -> table
	tk.MustExec("create table t3(a int) affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "test", "t3", true)
	// Set affinity to table again (idempotent operation)
	tk.MustExec("alter table t3 affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "test", "t3", true)
}

func TestAffinityDropDatabase(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists test_affinity_db")
	tk.MustExec("create database test_affinity_db")
	tk.MustExec("use test_affinity_db")

	// Create multiple tables with affinity
	tk.MustExec("create table t1(a int) affinity = 'table'")
	tk.MustExec("create table t2(a int) affinity = 'table'")
	tk.MustExec("create table tp1(a int) affinity = 'partition' partition by hash(a) partitions 2")

	checkAffinityGroupsInPD(t, dom, "test_affinity_db", "t1", true)
	checkAffinityGroupsInPD(t, dom, "test_affinity_db", "t2", true)
	checkAffinityGroupsInPD(t, dom, "test_affinity_db", "tp1", true)

	// Get table IDs before dropping database
	t1Info, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test_affinity_db"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	t2Info, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test_affinity_db"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	tp1Info, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test_affinity_db"), ast.NewCIStr("tp1"))
	require.NoError(t, err)

	// Drop database should clean up all affinity groups
	tk.MustExec("drop database test_affinity_db")

	ctx := context.Background()
	groupIDs := make([]string, 0, 2+len(tp1Info.Meta().Partition.Definitions))
	codec := dom.Store().GetCodec()
	groupIDs = append(groupIDs, ddl.GetTableAffinityGroupID(codec, t1Info.Meta().ID))
	groupIDs = append(groupIDs, ddl.GetTableAffinityGroupID(codec, t2Info.Meta().ID))
	for _, def := range tp1Info.Meta().Partition.Definitions {
		groupIDs = append(groupIDs, ddl.GetPartitionAffinityGroupID(codec, tp1Info.Meta().ID, def.ID))
	}

	groups, err := affinity.GetGroups(ctx, groupIDs)
	require.NoError(t, err)
	require.Empty(t, groups, "all affinity groups should be deleted after dropping database")
}
