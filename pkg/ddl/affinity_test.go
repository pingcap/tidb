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
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/affinity"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
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
	manager    affinity.Manager
	failCreate atomic.Bool
	failDelete atomic.Bool
	// Cancellation tests exhaust all three lightweight retries after a partial
	// commit or lost response, so they still exercise unused-group cleanup.
	retryFailures     atomic.Int64
	retryError        atomic.Value
	fail503Once       atomic.Bool
	failOnce          atomic.Bool
	failAfterCreate   atomic.Bool
	lastCreatedGroups atomic.Value
	createCalls       atomic.Int64
	createFailures    atomic.Int64
	notFoundCreates   atomic.Int64
	legacyPD          atomic.Bool
	attemptedGroups   sync.Map
	deleteCalls       atomic.Int64
	nilDeleteContext  atomic.Bool
	deleteHasDeadline atomic.Bool
	afterCreate       func(context.Context) error
	// labelRules maps a group ID to its key ranges. PD stores them in a label
	// rule that is created and force-deleted together with the group.
	labelRules sync.Map
	// partialCreateLimit, when positive, fails a request once it commits that
	// many groups, after partialCreateSkip requests have succeeded.
	// partialCreatedIDs records the committed IDs.
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

// newlyRequestedGroups excludes groups that already belonged to the table.
func newlyRequestedGroups(
	requested, original map[string][]pdhttp.AffinityGroupKeyRange,
) map[string][]pdhttp.AffinityGroupKeyRange {
	newGroups := make(map[string][]pdhttp.AffinityGroupKeyRange)
	for id, ranges := range requested {
		if _, exists := original[id]; !exists {
			newGroups[id] = ranges
		}
	}
	return newGroups
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.retryFailures.Load() > 0 {
		c.retryFailures.Add(-1)
		return nil, c.retryError.Load().(error)
	}
	if c.notFoundCreates.Load() > 0 {
		c.notFoundCreates.Add(-1)
		return nil, fmt.Errorf("request pd http api failed with status: '404 Not Found', body: 'unsupported affinity POST'")
	}
	if c.fail503Once.Swap(false) {
		c.createFailures.Add(1)
		return nil, fmt.Errorf("request pd http api failed with status: '503 Service Unavailable', body: 'injected transient creation failure'")
	}
	if c.failCreate.Load() || c.failOnce.Swap(false) {
		c.createFailures.Add(1)
		return nil, fmt.Errorf("injected affinity creation failure")
	}
	if c.partialCreateLimit.Load() > 0 && c.partialCreateSkip.Add(-1) < 0 {
		return c.createPartially(ctx, groups, int(c.partialCreateLimit.Swap(0)))
	}
	if c.legacyPD.Load() {
		conflict := false
		for id := range groups {
			c.attemptedGroups.Store(id, struct{}{})
			_, exists := c.labelRules.Load(id)
			conflict = conflict || exists
		}
		if conflict {
			return nil, fmt.Errorf("request pd http api failed with status: '409 Conflict', body: 'affinity group already exists'")
		}
	}
	if err := c.manager.CreateAffinityGroupsIfNotExists(ctx, groups); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(groups))
	for id, ranges := range groups {
		ids = append(ids, id)
		c.labelRules.LoadOrStore(id, ranges)
		c.everCreated.Store(id, struct{}{})
	}
	c.lastCreatedGroups.Store(groups)
	if c.failAfterCreate.Swap(false) {
		return nil, c.exhaustCreationRetries(fmt.Errorf("injected response loss after affinity creation"))
	}
	if c.afterCreate != nil {
		if err := c.afterCreate(ctx); err != nil {
			return nil, err
		}
	}
	return c.manager.GetAffinityGroups(ctx, ids)
}

func (c *truncateAffinityPDClient) exhaustCreationRetries(err error) error {
	c.retryError.Store(err)
	c.retryFailures.Store(3)
	return err
}

// createPartially commits groups in ID order and fails if it reaches limit.
// PD does this when a request spans several etcd transactions and a later one
// fails after the earlier ones have committed.
func (c *truncateAffinityPDClient) createPartially(ctx context.Context, groups map[string][]pdhttp.AffinityGroupKeyRange, limit int) (map[string]*pdhttp.AffinityGroupState, error) {
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
		return nil, err
	}
	for id, ranges := range committed {
		c.labelRules.LoadOrStore(id, ranges)
		c.everCreated.Store(id, struct{}{})
	}
	c.partialCreatedIDs.Store(committedIDs)
	if len(committedIDs) == limit {
		return nil, c.exhaustCreationRetries(fmt.Errorf("injected affinity creation failure after %d of %d groups", limit, len(ids)))
	}
	return c.manager.GetAffinityGroups(ctx, committedIDs)
}

func (c *truncateAffinityPDClient) BatchDeleteAffinityGroups(ctx context.Context, ids []string, _ bool) error {
	c.deleteCalls.Add(1)
	// Match the real HTTP client's context validation; the mock manager ignores it.
	if _, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://pd/affinity-groups", nil); err != nil {
		c.nilDeleteContext.Store(ctx == nil)
		return err
	}
	_, hasDeadline := ctx.Deadline()
	c.deleteHasDeadline.Store(hasDeadline)
	if err := ctx.Err(); err != nil {
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

func (c *truncateAffinityPDClient) GetAllAffinityGroups(ctx context.Context) (map[string]*pdhttp.AffinityGroupState, error) {
	return c.manager.GetAllAffinityGroups(ctx)
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

func TestAffinityMViewOutOfPlaceCutoverCleanup(t *testing.T) {
	for _, tc := range []struct {
		name           string
		oldAffinity    bool
		shadowAffinity bool
		failDelete     bool
	}{
		{name: "without_affinity"},
		{name: "with_affinity", oldAffinity: true, shadowAffinity: true},
		{name: "shadow_only", shadowAffinity: true},
		{name: "old_only", oldAffinity: true},
		{name: "cleanup_failure", oldAffinity: true, shadowAffinity: true, failDelete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newAffinityDDLTest(t)
			s.tk.MustExec("set tidb_mview_enable = on")
			s.tk.MustExec("create table source (a int not null)")
			s.tk.MustExec("create materialized view log on source (a)")
			s.tk.MustExec("create materialized view mv (a, cnt) as select a, count(*) from source group by a")
			oldMV := s.tableInfo(t, "mv")
			if tc.oldAffinity {
				oldMV.Affinity = &model.TableAffinityInfo{Level: ast.TableAffinityLevelTable}
				require.NoError(t, kv.RunInNewTxn(context.Background(), s.store, true, func(_ context.Context, txn kv.Transaction) error {
					return meta.NewMutator(txn).UpdateTable(s.schemaID, oldMV)
				}))
				require.NoError(t, affinity.CreateGroupsIfNotExists(context.Background(), s.groupsOf(t, oldMV)))
				require.NoError(t, s.dom.Reload())
			}
			shadowName := ast.NewCIStr("__mv_shadow_affinity_cleanup")
			shadow, err := ddl.BuildTableInfoWithLike(ast.Ident{Name: shadowName}, oldMV, &ast.CreateTableStmt{})
			require.NoError(t, err)
			shadow.MaterializedViewShadow = &model.MaterializedViewShadowInfo{SourceMViewID: oldMV.ID}
			// The cleanup decision must describe the retired table, even when the
			// internal DDL API is given a shadow with different affinity metadata.
			shadow.Affinity = nil
			if tc.shadowAffinity {
				shadow.Affinity = &model.TableAffinityInfo{Level: ast.TableAffinityLevelTable}
			}
			require.NoError(t, s.dom.DDLExecutor().CreateMaterializedViewShadowTable(s.tk.Session(), s.schemaID, ast.NewCIStr("test"), shadow))
			shadow = s.tableInfo(t, shadowName.O)
			source := s.tableInfo(t, "source")
			sources := []model.MViewRefreshSource{{TableID: source.ID, MLogID: source.MaterializedViewBase.MLogID}}
			s.tk.MustExec("update mysql.tidb_mview_refresh_info set last_success_read_tso = null where mview_id = ?", oldMV.ID)
			s.tk.Session().SetValue(sessionctx.QueryString, "REFRESH MATERIALIZED VIEW mv COMPLETE OUT OF PLACE")
			s.cli.deleteCalls.Store(0)
			s.cli.failDelete.Store(tc.failDelete)
			require.NoError(t, s.dom.DDLExecutor().RefreshMaterializedViewCompleteOutOfPlaceCutover(
				s.tk.Session(), s.schemaID, ast.NewCIStr("test"), oldMV.Name, oldMV.ID, shadow.ID,
				123456789, sources, nil, 0, true, nil, false,
			))
			s.cli.failDelete.Store(false)
			require.Equal(t, shadow.ID, s.tableInfo(t, "mv").ID, "cutover must promote shadow table")
			require.Zero(t, s.cli.deleteCalls.Load(), "cutover must defer affinity group deletion to delete-range GC")
			cutoverJob := latestHistoryJobByType(t, s.tk, model.ActionMViewRefreshOutOfPlaceCutover)
			cutoverArgs, err := model.GetRefreshMaterializedViewCompleteOutOfPlaceCutoverArgs(cutoverJob)
			require.NoError(t, err)
			require.Equal(t, tc.oldAffinity, cutoverArgs.OldMViewHasAffinity)
			if tc.oldAffinity {
				expectedGroupID := ddl.GetTableAffinityGroupID(s.store.GetCodec(), oldMV.ID)
				require.Equal(t, []string{expectedGroupID}, cutoverArgs.OldAffinityGroupIDs)
				requireAffinityGroupsWithLabelRules(t, s.cli, s.groupsOf(t, oldMV), true, "retired MV's group must stay until delete-range GC")
			} else {
				require.Empty(t, cutoverArgs.OldAffinityGroupIDs)
			}
			if tc.shadowAffinity {
				requireAffinityGroupsWithLabelRules(t, s.cli, s.groupsOf(t, shadow), true, "cutover must preserve the replacement MV's group")
			}
		})
	}
}

func TestAffinityCancelledCreateOnEntryTooLarge(t *testing.T) {
	for _, action := range []model.ActionType{model.ActionCreateTable, model.ActionCreateMaterializedViewShadow} {
		t.Run(action.String(), func(t *testing.T) {
			s := newAffinityDDLTest(t)
			var created atomic.Pointer[model.TableInfo]
			var jobID atomic.Int64
			// Fail the job row update after CREATE has written table metadata and PD groups.
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable", func(job *model.Job, err *error) {
				if job.Type == action && job.IsDone() {
					created.Store(job.BinlogInfo.TableInfo.Clone())
					jobID.Store(job.ID)
					*err = kv.ErrEntryTooLarge
				}
			})

			var err error
			if action == model.ActionCreateTable {
				err = s.tk.ExecToErr("create table oversized_job (a int) affinity='table'")
			} else {
				s.tk.MustExec("set tidb_mview_enable = on")
				s.tk.MustExec("create table source (a int not null)")
				s.tk.MustExec("create materialized view log on source (a)")
				s.tk.MustExec("create materialized view mv (a, cnt) as select a, count(*) from source group by a")
				mv := s.tableInfo(t, "mv")
				name := ast.NewCIStr("__mv_shadow_oversized_job")
				shadow, buildErr := ddl.BuildTableInfoWithLike(ast.Ident{Name: name}, mv, &ast.CreateTableStmt{})
				require.NoError(t, buildErr)
				shadow.MaterializedViewShadow = &model.MaterializedViewShadowInfo{SourceMViewID: mv.ID}
				shadow.Affinity = &model.TableAffinityInfo{Level: ast.TableAffinityLevelTable}
				err = s.dom.DDLExecutor().CreateMaterializedViewShadowTable(s.tk.Session(), s.schemaID, ast.NewCIStr("test"), shadow)
			}
			require.True(t, kv.ErrEntryTooLarge.Equal(err), "unexpected CREATE error: %v", err)
			info := created.Load()
			require.NotNil(t, info, "CREATE must reach the job row update")
			require.Nil(t, s.persisted(t, info.ID))

			rows := s.tk.MustQuery("select job_meta from mysql.tidb_ddl_history where job_id = ?", jobID.Load()).Rows()
			require.Len(t, rows, 1)
			var job model.Job
			require.NoError(t, job.Decode([]byte(rows[0][0].(string))))
			require.Equal(t, model.JobStateCancelled, job.State)
			require.NotNil(t, job.BinlogInfo)
			require.Zero(t, job.LastSchemaVersion)
			require.Zero(t, job.BinlogInfo.SchemaVersion)
			require.Nil(t, job.BinlogInfo.TableInfo)
			groups := s.groupsOf(t, info)
			s.cli.requireEverCreated(t, groups)
			requireAffinityGroupsWithLabelRules(t, s.cli, groups, false, "oversized CREATE must clean up its PD groups")
		})
	}
}

func TestAffinityCancelledTruncateOnEntryTooLarge(t *testing.T) {
	cases := []struct {
		name         string
		query        string
		action       model.ActionType
		updatedState model.SchemaState
	}{
		{"table", "truncate table p", model.ActionTruncateTable, model.StatePublic},
		{"partition before replacement commit", "alter table p truncate partition p0", model.ActionTruncateTablePartition, model.StateWriteOnly},
		{"partition after replacement commit", "alter table p truncate partition p0", model.ActionTruncateTablePartition, model.StateNone},
	}
	originalVersion := model.GetJobVerInUse()
	t.Cleanup(func() { model.SetJobVerInUse(originalVersion) })
	for _, version := range []model.JobVersion{model.JobVersion1, model.JobVersion2} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("v%d/%s", version, tc.name), func(t *testing.T) {
				s := newAffinityDDLTest(t)
				model.SetJobVerInUse(version)
				s.tk.MustExec("create table p(a int) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
				s.tk.MustExec("insert into p values (1),(11)")
				before := s.tableInfo(t, "p")
				persistedBefore := s.persisted(t, before.ID)
				originalGroups := s.groupsOf(t, before)
				var replacementGroups atomic.Value
				var encodedArgs atomic.Value
				var jobID atomic.Int64
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable", func(job *model.Job, err *error) {
					if job.Type != tc.action || job.TableID != before.ID || *err != nil {
						return
					}
					if job.Type == model.ActionTruncateTable && job.IsDone() ||
						job.Type == model.ActionTruncateTablePartition && job.SchemaState == model.StateWriteOnly {
						replacementGroups.Store(s.cli.lastCreatedGroups.Load())
					}
					// Fail after encoding the job, which also replaces its raw arguments.
					if job.SchemaState == tc.updatedState {
						encodedArgs.Store(string(job.RawArgs))
						jobID.Store(job.ID)
						*err = kv.ErrEntryTooLarge
					}
				})

				err := s.tk.ExecToErr(tc.query)
				require.True(t, kv.ErrEntryTooLarge.Equal(err), "unexpected TRUNCATE error: %v", err)
				job, err := ddl.GetHistoryJobByID(s.tk.Session(), jobID.Load())
				require.NoError(t, err)
				require.Equal(t, version, job.Version)
				require.Equal(t, model.JobStateCancelled, job.State)
				groups := newlyRequestedGroups(replacementGroups.Load().(map[string][]pdhttp.AffinityGroupKeyRange), originalGroups)
				if tc.action == model.ActionTruncateTablePartition {
					require.Len(t, groups, 1)
				}
				s.cli.requireEverCreated(t, groups)

				persisted := s.persisted(t, before.ID)
				if tc.updatedState == model.StateNone {
					committedGroups := s.groupsOf(t, persisted)
					for id := range groups {
						require.Contains(t, committedGroups, id)
					}
					requireAffinityGroupsWithLabelRules(t, s.cli, committedGroups, true, "cancellation must preserve the committed replacement and untouched partition groups")
					s.tk.MustQuery("select * from p order by a").Check(testkit.Rows("11"))
				} else {
					require.Equal(t, persistedBefore, persisted)
					requireAffinityGroupsWithLabelRules(t, s.cli, originalGroups, true, "cancellation must preserve the original partition groups")
					requireAffinityGroupsWithLabelRules(t, s.cli, groups, false, "cancellation must delete the uncommitted replacement groups")
					s.tk.MustQuery("select * from p order by a").Check(testkit.Rows("1", "11"))
				}
				require.JSONEq(t, encodedArgs.Load().(string), string(job.RawArgs), "cleanup must preserve the history argument encoding")
			})
		}
	}
}

func TestAffinityCancelledCreateAfterAsyncNotify(t *testing.T) {
	var failedTableID atomic.Int64
	mgr := &recordingExternalWorkloadManager{
		role: config.RoleMaster,
		registerErrFn: func(id int64) error {
			failedTableID.Store(id)
			return context.DeadlineExceeded
		},
	}
	tk, store := createTTLExternalWorkloadTestKit(t, mgr)
	dom := domain.GetDomain(tk.Session())
	cli := &truncateAffinityPDClient{manager: affinity.NewMockManager()}
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })
	db, ok := dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
	require.True(t, ok)

	// TTL registration cancels CREATE after the notifier's internal SQL has
	// flushed the StatePublic table metadata into the worker transaction.
	require.ErrorContains(t, tk.ExecToErr(ttlTableSQL+" AFFINITY='table'"), context.DeadlineExceeded.Error())
	id := failedTableID.Load()
	require.NotZero(t, id)
	groupID := ddl.GetTableAffinityGroupID(store.GetCodec(), id)
	_, created := cli.everCreated.Load(groupID)
	require.True(t, created)
	persisted, err := meta.NewReader(store.GetSnapshot(kv.MaxVersion)).GetTable(db.ID, id)
	require.NoError(t, err)
	require.Nil(t, persisted, "cancelled CREATE must roll back the flushed table metadata")
	groups, err := affinity.GetGroups(context.Background(), []string{groupID})
	require.NoError(t, err)
	require.Empty(t, groups)
	_, labelExists := cli.labelRule(groupID)
	require.False(t, labelExists)
	require.NoError(t, dom.Reload())
	tk.MustQuery("show tables like 't'").Check(testkit.Rows())
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
			requested := s.cli.lastCreatedGroups.Load().(map[string][]pdhttp.AffinityGroupKeyRange)
			replacementGroups := newlyRequestedGroups(requested, originalGroups)
			require.Len(t, replacementGroups, 1, "the PD request must contain exactly one replacement group")
			requireAffinityGroupsWithLabelRules(t, s.cli, replacementGroups, failDelete, "failed cleanup must not prevent cancellation")
		})
	}
}

func TestAffinityTruncatePartitionWithStaleIDs(t *testing.T) {
	cases := []struct {
		name            string
		partitionNames  string
		withGlobalIndex bool
	}{
		{"p0", "p0", false},
		{"p0,p1", "p0,p1", false},
		{"p0/global_index", "p0", true},
		{"p0,p1/global_index", "p0,p1", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := make(chan *notifier.SchemaChangeEvent, 2)
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/afterDDLNotifierCreated", func(n *notifier.DDLNotifier) {
				n.RegisterHandler(notifier.TestHandlerID, func(ctx context.Context, _ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
					if event.GetType() == model.ActionTruncateTablePartition {
						select {
						case events <- event:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				})
			})
			s := newAffinityDDLTest(t)
			s.tk.MustExec("create table p(a int, b int) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
			if tc.withGlobalIndex {
				s.tk.MustExec("alter table p add unique index idx_b(b) global")
			}
			s.tk.MustExec("insert into p values (1,1),(11,11)")
			before := s.tableInfo(t, "p")
			oldIDs := []int64{before.Partition.Definitions[0].ID}
			if tc.partitionNames == "p0,p1" {
				oldIDs = append(oldIDs, before.Partition.Definitions[1].ID)
			}
			// Capture the same IDs a queued job would see before the first truncate.
			db, ok := s.dom.InfoSchema().SchemaByName(ast.NewCIStr("test"))
			require.True(t, ok)
			job, args := buildTruncatePartitionJob(db, before, oldIDs, nil)
			s.cli.legacyPD.Store(true)
			beforeCalls := s.cli.createCalls.Load()
			s.tk.MustExec("alter table p truncate partition p0")
			afterFirst := s.persisted(t, before.ID)
			firstID := afterFirst.Partition.Definitions[0].ID
			require.NotEqual(t, oldIDs[0], firstID)
			_, attempted := s.cli.attemptedGroups.Load(ddl.GetPartitionAffinityGroupID(s.store.GetCodec(), before.ID, firstID))
			require.True(t, attempted)
			_, attempted = s.cli.attemptedGroups.Load(ddl.GetPartitionAffinityGroupID(s.store.GetCodec(), before.ID, before.Partition.Definitions[1].ID))
			require.True(t, attempted, "preparation includes untouched groups so missing groups can be backfilled")
			require.Equal(t, int64(2), s.cli.createCalls.Load()-beforeCalls, "preparation retries with the replacement only after a batch conflict")
			s.tk.MustExec("insert into p values (2,2)")

			var writeOnlyTable atomic.Pointer[model.TableInfo]
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(runningJob *model.Job) {
				if runningJob.Type != model.ActionTruncateTablePartition || runningJob.SchemaState != model.StateWriteOnly {
					return
				}
				info, err := meta.NewReader(s.store.GetSnapshot(kv.MaxVersion)).GetTable(db.ID, before.ID)
				if err != nil {
					t.Error(err)
					return
				}
				writeOnlyTable.Store(info)
			})

			se := s.tk.Session()
			se.SetValue(sessionctx.QueryString, "alter table p truncate partition "+tc.partitionNames)
			exec := s.dom.DDLExecutor().(ddl.ExecutorForTest)
			beforeCalls = s.cli.createCalls.Load()
			require.NoError(t, exec.DoDDLJobWrapper(se, ddl.NewJobWrapperWithArgs(job, args, false)))
			require.Equal(t, int64(len(oldIDs)), s.cli.createCalls.Load()-beforeCalls, "after the preparation batch conflicts, only missing groups need another POST")
			require.Eventually(t, func() bool { return len(events) == 2 }, 10*time.Second, 10*time.Millisecond)
			// Discard the notification for the first truncate, which made p0's queued ID stale.
			<-events
			eventTable, added, dropped := (<-events).GetTruncatePartitionInfo()
			require.Equal(t, before.ID, eventTable.ID)
			require.Len(t, args.NewPartitionIDs, len(oldIDs))
			prepared := writeOnlyTable.Load()
			require.NotNil(t, prepared)
			require.ElementsMatch(t, args.NewPartitionIDs[1:], prepared.Partition.NewPartitionIDs)
			staleGroup := ddl.GetPartitionAffinityGroupID(s.store.GetCodec(), before.ID, args.NewPartitionIDs[0])
			_, created := s.cli.everCreated.Load(staleGroup)
			require.False(t, created, "a replacement group must not be created for stale partition ID %d", oldIDs[0])

			history, err := ddl.GetHistoryJobByID(se, job.ID)
			require.NoError(t, err)
			finishedArgs, err := model.GetFinishedTruncateTableArgs(history)
			require.NoError(t, err)
			diff, err := meta.NewReader(s.store.GetSnapshot(kv.MaxVersion)).GetSchemaDiff(history.BinlogInfo.SchemaVersion)
			require.NoError(t, err)
			require.NotNil(t, diff)
			after := s.persisted(t, before.ID)
			require.Equal(t, afterFirst.Partition.Definitions[0].ID, after.Partition.Definitions[0].ID)
			if len(oldIDs) == 2 {
				newPartitionID := after.Partition.Definitions[1].ID
				require.Equal(t, args.NewPartitionIDs[1], newPartitionID)
				require.Len(t, added.Definitions, 1)
				require.Len(t, dropped.Definitions, 1)
				require.Equal(t, oldIDs[1], dropped.Definitions[0].ID)
				require.Equal(t, newPartitionID, added.Definitions[0].ID)
				require.Equal(t, oldIDs[1:], finishedArgs.OldPartitionIDs)
				require.Equal(t, []*model.AffectedOption{{OldTableID: oldIDs[1], TableID: newPartitionID}}, diff.AffectedOpts)
				s.tk.MustQuery("select a from p order by a").Check(testkit.Rows("2"))
			} else {
				require.Empty(t, added.Definitions)
				require.Empty(t, dropped.Definitions)
				require.Empty(t, finishedArgs.OldPartitionIDs)
				require.Empty(t, diff.AffectedOpts)
				require.Equal(t, afterFirst.Partition.Definitions[1].ID, after.Partition.Definitions[1].ID)
				s.tk.MustQuery("select a from p order by a").Check(testkit.Rows("2", "11"))
			}
			if tc.withGlobalIndex {
				s.tk.MustExec("admin check table p")
			}
			requireAffinityGroupsWithLabelRules(t, s.cli, s.groupsOf(t, after), true, "the current partition groups must remain")
		})
	}
}

func TestAffinityTruncatePartitionCancelDuringCreate(t *testing.T) {
	s := newAffinityDDLTest(t)
	s.tk.MustExec("create table p(a int) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
	s.tk.MustExec("insert into p values (1),(11)")
	before := s.tableInfo(t, "p")
	persistedBefore := s.persisted(t, before.ID)
	originalGroups := s.groupsOf(t, before)
	var jobID atomic.Int64
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
		if job.Type == model.ActionTruncateTablePartition && job.SchemaState == model.StatePublic {
			jobID.Store(job.ID)
		}
	})
	var canceledDuringCreate atomic.Bool
	s.cli.afterCreate = func(ctx context.Context) error {
		cancelCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		jobErrs, err := ddl.CancelJobs(cancelCtx, s.tkCancel.Session(), []int64{jobID.Load()})
		if err != nil {
			return err
		}
		if jobErrs[0] != nil {
			return jobErrs[0]
		}
		select {
		case <-ctx.Done():
			canceledDuringCreate.Store(true)
			return ctx.Err()
		case <-cancelCtx.Done():
			return fmt.Errorf("waiting for the DDL step context to be canceled: %w", cancelCtx.Err())
		}
	}
	// The cancel command can cause a write conflict and another rollback attempt.
	// Check the first rollback before a retry gets a fresh context.
	var checkedRollback, removedDuringRollback atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRunOneJobStep", func(job *model.Job) {
		if job.ID != jobID.Load() || !job.IsCancelled() || !checkedRollback.CompareAndSwap(false, true) {
			return
		}
		for id := range s.cli.lastCreatedGroups.Load().(map[string][]pdhttp.AffinityGroupKeyRange) {
			if _, original := originalGroups[id]; !original {
				if _, exists := s.cli.labelRule(id); exists {
					return
				}
			}
		}
		removedDuringRollback.Store(true)
	})
	require.Error(t, s.tk.ExecToErr("alter table p truncate partition p0"))
	require.True(t, canceledDuringCreate.Load(), "PD must observe cancellation after storing the replacement group")
	require.True(t, removedDuringRollback.Load(), "the in-step rollback must remove the replacement group's label rule")
	require.Equal(t, before, s.tableInfo(t, "p"))
	require.Equal(t, persistedBefore, s.persisted(t, before.ID))
	s.tk.MustQuery("select * from p order by a").Check(testkit.Rows("1", "11"))
	requireAffinityGroupsWithLabelRules(t, s.cli, originalGroups, true, "cleanup must preserve the original partition groups")
	replacementGroups := make(map[string][]pdhttp.AffinityGroupKeyRange)
	for id, ranges := range s.cli.lastCreatedGroups.Load().(map[string][]pdhttp.AffinityGroupKeyRange) {
		if _, original := originalGroups[id]; !original {
			replacementGroups[id] = ranges
		}
	}
	require.Len(t, replacementGroups, 1)
	requireAffinityGroupsWithLabelRules(t, s.cli, replacementGroups, false, "rollback must remove the replacement group")
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
				requireAffinityGroupsWithLabelRules(t, s.cli, originalGroups, true, "committed truncate must keep the original groups until delete-range GC")
			})
		}
	}
}

func TestAffinityTruncateTablePartitionGrowth(t *testing.T) {
	jobVersion := model.GetJobVerInUse()
	t.Cleanup(func() { model.SetJobVerInUse(jobVersion) })
	for _, version := range []model.JobVersion{model.JobVersion1, model.JobVersion2} {
		for _, rollbackReservation := range []bool{false, true} {
			t.Run(fmt.Sprintf("version=%d/rollback_reservation=%v", version, rollbackReservation), func(t *testing.T) {
				model.SetJobVerInUse(version)
				s := newAffinityDDLTest(t)
				s.tk.MustExec("create table p(a int) partition by range(a) (partition p0 values less than (10))")
				s.tk.MustExec("insert into p values (1)")
				original := s.tableInfo(t, "p")
				readTable, resume := make(chan struct{}), make(chan struct{})
				var resumeOnce sync.Once
				release := func() { resumeOnce.Do(func() { close(resume) }) }
				var held, reservationFailed atomic.Bool
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterCheckTruncateTableMaterializedViewConstraints", func(id int64) {
					if id == original.ID && held.CompareAndSwap(false, true) {
						close(readTable)
						<-resume
					}
				})
				var jobID, initialIDs atomic.Int64
				testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
					if job.Type == model.ActionTruncateTable && job.TableID == original.ID && jobID.CompareAndSwap(0, job.ID) {
						args, err := model.GetTruncateTableArgs(job)
						require.NoError(t, err)
						initialIDs.Store(int64(len(args.NewPartitionIDs)))
						s.cli.failAfterCreate.Store(!rollbackReservation)
					}
				})
				rolledBack, retry := make(chan struct{}), make(chan struct{})
				var retryOnce, pauseOnce sync.Once
				resumeRetry := func() { retryOnce.Do(func() { close(retry) }) }
				if rollbackReservation {
					testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterUpdateJobToTable", func(job *model.Job, err *error) {
						if job.Type == model.ActionTruncateTable && job.TableID == original.ID && job.IsRunning() && reservationFailed.CompareAndSwap(false, true) {
							*err = context.DeadlineExceeded
						}
					})
					testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRefreshJob", func(job *model.Job) {
						if job.Type == model.ActionTruncateTable && job.TableID == original.ID && reservationFailed.Load() {
							pauseOnce.Do(func() { close(rolledBack); <-retry })
						}
					})
				}
				tk := testkit.NewTestKit(t, s.store)
				tk.MustExec("use test")
				done := make(chan struct{})
				var truncateErr error
				go func() {
					defer close(done)
					truncateErr = tk.ExecToErr("truncate table p")
				}()
				defer func() {
					release()
					resumeRetry()
					select {
					case <-done:
						return
					default:
					}
					cancelCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if id := jobID.Load(); id != 0 {
						jobErrs, err := ddl.CancelJobs(cancelCtx, s.tkCancel.Session(), []int64{id})
						if err == nil {
							err = jobErrs[0]
						}
						if err != nil {
							t.Logf("cancelling TRUNCATE job %d: %v", id, err)
						}
					}
					select {
					case <-done:
					case <-cancelCtx.Done():
						t.Error("TRUNCATE did not exit after releasing the hooks and attempting cancellation")
						// Stopping DDL joins the workers and cancels the submitting session's DDL wait.
						if err := s.dom.DDL().Stop(); err != nil {
							t.Errorf("stopping DDL: %v", err)
						}
						<-done
					}
				}()
				select {
				case <-readTable:
				case <-time.After(30 * time.Second):
					t.Error("TRUNCATE did not read the table")
					return
				}
				s.tk.MustExec("alter table p add partition (partition p1 values less than (20))")
				s.tk.MustExec("alter table p affinity='partition'")
				originalGroups := s.groupsOf(t, s.tableInfo(t, "p"))
				release()
				var unrelatedID int64
				if rollbackReservation {
					select {
					case <-rolledBack:
					case <-time.After(30 * time.Second):
						t.Error("ID reservation did not roll back")
						return
					}
					s.tk.MustExec("create table q(a int)")
					unrelatedID = s.tableInfo(t, "q").ID
					resumeRetry()
				}
				select {
				case <-done:
					if rollbackReservation {
						require.NoError(t, truncateErr)
					} else {
						require.ErrorContains(t, truncateErr, "injected response loss after affinity creation")
					}
				case <-time.After(30 * time.Second):
					t.Error("TRUNCATE did not finish")
					return
				}
				require.EqualValues(t, 1, initialIDs.Load())
				created := s.cli.lastCreatedGroups.Load().(map[string][]pdhttp.AffinityGroupKeyRange)
				require.Len(t, created, 2)
				after := s.tableInfo(t, "p")
				require.Len(t, after.Partition.Definitions, 2)
				if rollbackReservation {
					require.NotEqual(t, original.ID, after.ID)
					for _, partition := range after.Partition.Definitions {
						require.NotEqual(t, unrelatedID, partition.ID, "retry must not reuse IDs from the rolled-back reservation")
					}
					s.tk.MustQuery("select * from p").Check(testkit.Rows())
				} else {
					require.Equal(t, original.ID, after.ID)
					s.tk.MustQuery("select * from p").Check(testkit.Rows("1"))
				}
				requireAffinityGroupsWithLabelRules(t, s.cli, originalGroups, true, "old groups stay until delete-range GC")
				requireAffinityGroupsWithLabelRules(t, s.cli, created, rollbackReservation, "all replacement groups follow the DDL outcome")
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

	const beforeStep = "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep"
	tk.MustExec("alter table p truncate partition p0")
	require.Equal(t, int64(4), cli.createFailures.Load(), "only the exhausted attempts before replacement failed")
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
	require.Contains(t, groups, oldID, "old partition affinity group must stay until delete-range GC")
	require.Eventually(t, func() bool {
		rows := tk.MustQuery(fmt.Sprintf("select count(*) from (select end_key from mysql.gc_delete_range union all select end_key from mysql.gc_delete_range_done) ranges_to_delete where end_key='%x'", []byte(tablecodec.EncodeTablePrefix(beforeMeta.Partition.Definitions[0].ID+1)))).Rows()
		return rows[0][0] == "1"
	}, 5*time.Second, 10*time.Millisecond)
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
				readyToCancel := job.SchemaState == model.StateWriteOnly
				if scenario == "metadata not committed" {
					// Restoration keeps SchemaState at StatePublic after the failed step resets.
					readyToCancel = job.SchemaState == model.StatePublic && job.ErrorCount > 0
				}
				if job.TableID == beforeMeta.ID && job.Type == model.ActionTruncateTablePartition && readyToCancel && cancelOnce.CompareAndSwap(false, true) {
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

func TestAffinityTruncatePartitionPreparation(t *testing.T) {
	originalVersion := model.GetJobVerInUse()
	t.Cleanup(func() { model.SetJobVerInUse(originalVersion) })
	const beforeStep = "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep"
	for _, version := range []model.JobVersion{model.JobVersion1, model.JobVersion2} {
		for _, scenario := range []string{"prepared", "legacy write only", "legacy finalization"} {
			t.Run(fmt.Sprintf("v%d/%s", version, scenario), func(t *testing.T) {
				s := newAffinityDDLTest(t)
				model.SetJobVerInUse(version)
				s.tk.MustExec("create table p(a int) affinity='partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
				s.tk.MustExec("insert into p values (1),(11)")
				before := s.persisted(t, s.tableInfo(t, "p").ID)
				untouchedID := ddl.GetPartitionAffinityGroupID(s.store.GetCodec(), before.ID, before.Partition.Definitions[1].ID)
				if scenario == "prepared" {
					// Groups can be missing when the table was created before PD supported affinity.
					require.NoError(t, s.cli.BatchDeleteAffinityGroups(context.Background(), []string{untouchedID}, true))
					// Both groups fit below the injection limit, so the partial path succeeds.
					s.cli.partialCreateLimit.Store(3)
				}
				const retryWait = 60 * time.Millisecond
				if scenario == "legacy finalization" {
					errorLimit := vardef.GetDDLErrorCountLimit()
					t.Cleanup(func() { vardef.SetDDLErrorCountLimit(errorLimit) })
					s.tk.MustExec("set global tidb_ddl_error_count_limit = 1")
					ddl.SetWaitTimeWhenErrorOccurred(retryWait)
				}
				var finalAttemptsMu sync.Mutex
				var finalAttempts []time.Time
				var injected atomic.Bool
				var rollbackArgs atomic.Pointer[model.TruncateTableArgs]
				var originalArgs atomic.Pointer[model.TruncateTableArgs]
				var jobID atomic.Int64
				testfailpoint.EnableCall(t, beforeStep, func(job *model.Job) {
					if job.Type != model.ActionTruncateTablePartition {
						return
					}
					if job.IsRollingback() {
						args, err := model.GetTruncateTableArgs(job)
						if err != nil {
							t.Error(err)
							return
						}
						rollbackArgs.Store(args)
						return
					}
					state := model.StateDeleteReorganization
					if scenario == "legacy write only" {
						state = model.StateWriteOnly
					}
					if job.SchemaState != state {
						return
					}
					if scenario == "legacy finalization" {
						finalAttemptsMu.Lock()
						finalAttempts = append(finalAttempts, time.Now())
						finalAttemptsMu.Unlock()
					}
					if !injected.CompareAndSwap(false, true) {
						return
					}
					jobID.Store(job.ID)
					if scenario != "prepared" {
						args, err := model.GetTruncateTableArgs(job)
						if err != nil {
							t.Error(err)
							return
						}
						// Re-encode the job with only the fields written by older owners.
						legacyArgs := &model.TruncateTableArgs{
							OldPartitionIDs: args.OldPartitionIDs,
							NewPartitionIDs: args.NewPartitionIDs,
						}
						originalArgs.Store(legacyArgs)
						job.FillArgs(legacyArgs)
						if _, err := job.Encode(true); err != nil {
							t.Error(err)
							return
						}
						if scenario == "legacy finalization" {
							replacementID := ddl.GetPartitionAffinityGroupID(
								s.store.GetCodec(),
								before.ID,
								legacyArgs.NewPartitionIDs[0],
							)
							// Simulate missing groups on an owner upgrade.
							err = s.cli.BatchDeleteAffinityGroups(
								context.Background(),
								[]string{replacementID, untouchedID},
								true,
							)
							if err != nil {
								t.Error(err)
								return
							}
						}
					}
					if scenario == "legacy finalization" {
						s.cli.notFoundCreates.Store(3)
					} else {
						s.cli.notFoundCreates.Store(1)
					}
				})
				err := s.tk.ExecToErr("alter table p truncate partition p0")
				require.True(t, injected.Load())
				after := s.persisted(t, before.ID)
				if scenario == "legacy write only" {
					require.ErrorContains(t, err, "unsupported affinity POST")
					require.Equal(t, before.Partition, after.Partition)
					require.NotNil(t, rollbackArgs.Load())
					require.Equal(t, originalArgs.Load(), rollbackArgs.Load())
					history, err := ddl.GetHistoryJobByID(s.tk.Session(), jobID.Load())
					require.NoError(t, err)
					require.Equal(t, model.JobStateCancelled, history.State)
					require.ErrorContains(t, history.Error, "unsupported affinity POST")
					s.tk.MustQuery("select * from p order by a").Check(testkit.Rows("1", "11"))
				} else {
					require.NoError(t, err)
					require.NotEqual(t, before.Partition.Definitions[0].ID, after.Partition.Definitions[0].ID)
					require.Equal(t, before.Partition.Definitions[1].ID, after.Partition.Definitions[1].ID)
					if scenario == "prepared" {
						require.EqualValues(t, 1, s.cli.notFoundCreates.Load(), "finalization must not send a create request")
					} else {
						require.Zero(t, s.cli.notFoundCreates.Load(), "legacy finalization must retry a failed create")
						finalAttemptsMu.Lock()
						times := slices.Clone(finalAttempts)
						finalAttemptsMu.Unlock()
						require.Len(t, times, 4)
						for i := 1; i < len(times); i++ {
							require.GreaterOrEqual(t, times[i].Sub(times[i-1]), retryWait,
								"legacy finalization keeps backing off beyond the DDL error limit")
						}
						replacementID := ddl.GetPartitionAffinityGroupID(s.store.GetCodec(), before.ID, after.Partition.Definitions[0].ID)
						created := s.cli.lastCreatedGroups.Load()
						require.Contains(t, created, replacementID)
						require.Contains(t, created, untouchedID)
					}
					requireAffinityGroupsWithLabelRules(t, s.cli, s.groupsOf(t, after), true, "all current partition groups must exist")
					s.tk.MustQuery("select * from p order by a").Check(testkit.Rows("11"))
				}
				s.tk.MustExec("admin check table p")
			})
		}
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
	ddlutil.EmulatorGCDisable()
	t.Cleanup(ddlutil.EmulatorGCEnable)

	store, dom := testkit.CreateMockStoreAndDomain(t)
	cli := &truncateAffinityPDClient{manager: affinity.NewMockManager()}
	affinity.InitManager(cli)
	t.Cleanup(func() { affinity.InitManager(nil) })
	tk := testkit.NewTestKit(t, store)
	safePoint := time.Now().Add(-48 * time.Hour).Format("20060102-15:04:05 -0700 MST")
	tk.MustExec("insert into mysql.tidb values ('tikv_gc_safe_point', ?, '') on duplicate key update variable_value = values(variable_value)", safePoint)
	tk.MustExec("insert into mysql.tidb values ('tikv_gc_enable', 'true', '') on duplicate key update variable_value = values(variable_value)")
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
	if !cli.deleteHasDeadline.Load() {
		t.Error("post-commit ALTER cleanup must have a deadline")
	}
	checkAffinityGroupsInPD(t, dom, "test", "t1", false)

	// A group can still conflict after PD has removed its last key range.
	t.Run("alter rejects group with zero ranges", func(t *testing.T) {
		ctx := context.Background()
		tbl, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t1"))
		require.NoError(t, err)
		id := ddl.GetTableAffinityGroupID(store.GetCodec(), tbl.Meta().ID)
		require.NoError(t, cli.manager.CreateAffinityGroupsIfNotExists(ctx, map[string][]pdhttp.AffinityGroupKeyRange{id: nil}))
		cli.labelRules.Store(id, []pdhttp.AffinityGroupKeyRange(nil))
		cli.legacyPD.Store(true)
		t.Cleanup(func() {
			cli.legacyPD.Store(false)
			require.NoError(t, cli.BatchDeleteAffinityGroups(ctx, []string{id}, true))
		})

		require.ErrorContains(t, tk.ExecToErr("alter table t1 affinity = 'table'"), "409 Conflict")
		tbl, err = dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t1"))
		require.NoError(t, err)
		require.Nil(t, tbl.Meta().Affinity, "failed ALTER must not publish affinity")
	})

	// Test 4: Alter table to add affinity back
	tk.MustExec("alter table t1 affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "test", "t1", true)

	// Test 5: Drop table defers affinity group cleanup to GC and records OldAffinityGroupIDs.
	t1Info, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	t1ID := t1Info.Meta().ID
	t1GroupID := ddl.GetTableAffinityGroupID(dom.Store().GetCodec(), t1ID)

	tk.MustExec("drop table t1")
	ctx := context.Background()
	groups, err := affinity.GetGroups(ctx, []string{t1GroupID})
	require.NoError(t, err)
	require.Contains(t, groups, t1GroupID, "affinity groups should remain until GC after dropping table")
	dropJob := latestHistoryJobByType(t, tk, model.ActionDropTable)
	dropArgs, err := model.GetFinishedDropTableArgs(dropJob)
	require.NoError(t, err)
	require.Equal(t, []string{t1GroupID}, dropArgs.OldAffinityGroupIDs)

	// Recovering the table within the GC window keeps its original affinity group intact.
	tk.MustExec("recover table t1")
	checkAffinityGroupsInPD(t, dom, "test", "t1", true)

	// Test 6: TRUNCATE TABLE creates new affinity group and defers deleting old group to GC.
	tk.MustExec("create table t2(a int) affinity = 'table'")
	checkAffinityGroupsInPD(t, dom, "test", "t2", true)

	t2Info, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t2"))
	require.NoError(t, err)
	oldTableID := t2Info.Meta().ID
	oldTableGroupID := ddl.GetTableAffinityGroupID(dom.Store().GetCodec(), oldTableID)

	tk.MustExec("truncate table t2")
	checkAffinityGroupsInPD(t, dom, "test", "t2", true)
	groups, err = affinity.GetGroups(ctx, []string{oldTableGroupID})
	require.NoError(t, err)
	require.Contains(t, groups, oldTableGroupID, "old table's affinity groups should remain until GC after truncate")
	truncJob := latestHistoryJobByType(t, tk, model.ActionTruncateTable)
	truncArgs, err := model.GetFinishedTruncateTableArgs(truncJob)
	require.NoError(t, err)
	require.Equal(t, []string{oldTableGroupID}, truncArgs.OldAffinityGroupIDs)

	// Test 7: TRUNCATE PARTITION creates new partition group and defers deleting old group to GC.
	tk.MustExec("create table tp2(a int) affinity = 'partition' partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	checkAffinityGroupsInPD(t, dom, "test", "tp2", true)

	tblInfo, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("tp2"))
	require.NoError(t, err)
	oldPartitionID := tblInfo.Meta().Partition.Definitions[0].ID
	oldPartGroupID := ddl.GetPartitionAffinityGroupID(dom.Store().GetCodec(), tblInfo.Meta().ID, oldPartitionID)

	tk.MustExec("alter table tp2 truncate partition p0")
	checkAffinityGroupsInPD(t, dom, "test", "tp2", true)
	groups, err = affinity.GetGroups(ctx, []string{oldPartGroupID})
	require.NoError(t, err)
	require.Contains(t, groups, oldPartGroupID, "old partition's affinity group should remain until GC after truncate partition")
	truncPartJob := latestHistoryJobByType(t, tk, model.ActionTruncateTablePartition)
	truncPartArgs, err := model.GetFinishedTruncateTableArgs(truncPartJob)
	require.NoError(t, err)
	require.Equal(t, []string{oldPartGroupID}, truncPartArgs.OldAffinityGroupIDs)

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
	ddlutil.EmulatorGCDisable()
	t.Cleanup(ddlutil.EmulatorGCEnable)

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	safePoint := time.Now().Add(-48 * time.Hour).Format("20060102-15:04:05 -0700 MST")
	tk.MustExec("insert into mysql.tidb values ('tikv_gc_safe_point', ?, '') on duplicate key update variable_value = values(variable_value)", safePoint)
	tk.MustExec("insert into mysql.tidb values ('tikv_gc_enable', 'true', '') on duplicate key update variable_value = values(variable_value)")

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

	// Drop database defers affinity group cleanup to GC and records OldAffinityGroupIDs.
	tk.MustExec("drop database test_affinity_db")

	ctx := context.Background()
	groupIDs := make([]string, 0, 2+len(tp1Info.Meta().Partition.Definitions))
	codec := dom.Store().GetCodec()
	groupIDs = append(groupIDs, ddl.GetTableAffinityGroupID(codec, t1Info.Meta().ID))
	groupIDs = append(groupIDs, ddl.GetTableAffinityGroupID(codec, t2Info.Meta().ID))
	for _, def := range tp1Info.Meta().Partition.Definitions {
		groupIDs = append(groupIDs, ddl.GetPartitionAffinityGroupID(codec, tp1Info.Meta().ID, def.ID))
	}
	slices.Sort(groupIDs)

	groups, err := affinity.GetGroups(ctx, groupIDs)
	require.NoError(t, err)
	require.Len(t, groups, len(groupIDs), "all affinity groups should remain until GC after dropping database")

	dropDBJob := latestHistoryJobByType(t, tk, model.ActionDropSchema)
	dropDBArgs, err := model.GetFinishedDropSchemaArgs(dropDBJob)
	require.NoError(t, err)
	require.Equal(t, groupIDs, dropDBArgs.OldAffinityGroupIDs)

	// Recovering the database within the GC window keeps all original affinity groups intact.
	tk.MustExec("flashback database test_affinity_db")
	checkAffinityGroupsInPD(t, dom, "test_affinity_db", "t1", true)
	checkAffinityGroupsInPD(t, dom, "test_affinity_db", "t2", true)
	checkAffinityGroupsInPD(t, dom, "test_affinity_db", "tp1", true)
}

func latestHistoryJobByType(t *testing.T, tk *testkit.TestKit, action model.ActionType) *model.Job {
	t.Helper()
	rows := tk.MustQuery("select job_meta from mysql.tidb_ddl_history order by job_id desc limit 20").Rows()
	for _, row := range rows {
		var job model.Job
		require.NoError(t, job.Decode([]byte(row[0].(string))))
		if job.Type == action {
			return &job
		}
	}
	require.FailNowf(t, "history job not found", "action=%s", action)
	return nil
}

func TestParseAffinityGroupID(t *testing.T) {
	ks42, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 42},
	})
	require.NoError(t, err)
	ks43, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: 43},
	})
	require.NoError(t, err)

	var validCodec tikv.Codec = &mockCodec{}
	if kerneltype.IsNextGen() {
		validCodec = ks42
	}

	const maxID int64 = 1<<63 - 1
	tableID := int64(123)
	partID := int64(456)
	tblGroup := ddl.GetTableAffinityGroupID(validCodec, tableID)
	partGroup := ddl.GetPartitionAffinityGroupID(validCodec, tableID, partID)
	maxPartGroup := ddl.GetPartitionAffinityGroupID(validCodec, maxID, maxID)

	gotTID, gotPID, isPart, ok := ddl.ParseAffinityGroupID(validCodec, tblGroup)
	require.True(t, ok)
	require.False(t, isPart)
	require.Equal(t, tableID, gotTID)
	require.Zero(t, gotPID)

	gotTID, gotPID, isPart, ok = ddl.ParseAffinityGroupID(validCodec, partGroup)
	require.True(t, ok)
	require.True(t, isPart)
	require.Equal(t, tableID, gotTID)
	require.Equal(t, partID, gotPID)

	gotTID, gotPID, isPart, ok = ddl.ParseAffinityGroupID(validCodec, maxPartGroup)
	require.True(t, ok)
	require.True(t, isPart)
	require.Equal(t, maxID, gotTID)
	require.Equal(t, maxID, gotPID)

	invalidIDs := []string{
		"",
		"_tidb_",
		"_tidb_t_",
		"_tidb_t_0",
		"_tidb_t_-1",
		"_tidb_t_+1",
		"_tidb_t_01",
		"_tidb_t_1_extra",
		"_tidb_t_9223372036854775808",
		"_tidb_pt_1",
		"_tidb_pt_1_p0",
		"_tidb_pt_0_p1",
		"_tidb_pt_01_p2",
		"_tidb_pt_1_p02",
		"_tidb_pt_1_p2_p3",
		"_tidb_pt_1_p-2",
		"custom_group_1",
		"user_t_123",
	}
	if kerneltype.IsNextGen() {
		invalidIDs = append(invalidIDs,
			"_tidb_t_123",
			"_tidb_pt_123_p456",
			ddl.GetTableAffinityGroupID(ks43, tableID),
			ddl.GetPartitionAffinityGroupID(ks43, tableID, partID),
			"_tidb_ks042_t_123",
			"_tidb_ks42_t_0",
			"_tidb_ks42_t_01",
			"_tidb_ks42_pt_123_p0",
			"_tidb_ks42_pt_0123_p456",
		)
	} else {
		invalidIDs = append(invalidIDs,
			"_tidb_ks42_t_123",
			"_tidb_ks42_pt_123_p456",
		)
	}
	for _, id := range invalidIDs {
		tid, pid, isP, valid := ddl.ParseAffinityGroupID(validCodec, id)
		require.False(t, valid, "group ID %q must be rejected", id)
		require.Zero(t, tid)
		require.Zero(t, pid)
		require.False(t, isP)
	}
}

func TestAffinityCreateTransient503(t *testing.T) {
	s := newAffinityDDLTest(t)
	s.cli.fail503Once.Store(true)
	s.tk.MustExec("create table retry_create(a int) affinity='table'")
	require.EqualValues(t, 1, s.cli.createFailures.Load())
	require.EqualValues(t, 2, s.cli.createCalls.Load())
	checkAffinityGroupsInPD(t, s.dom, "test", "retry_create", true)
	s.tk.MustExec("create table retry_alter(a int)")
	s.cli.fail503Once.Store(true)
	s.tk.MustExec("alter table retry_alter affinity='table'")
	require.EqualValues(t, 2, s.cli.createFailures.Load())
	checkAffinityGroupsInPD(t, s.dom, "test", "retry_alter", true)
}

func TestAffinityPartitionRetryAfterAsyncNotify(t *testing.T) {
	for _, action := range []model.ActionType{model.ActionDropTablePartition, model.ActionReorganizePartition} {
		t.Run(action.String(), func(t *testing.T) {
			s := newAffinityDDLTest(t)
			s.tk.MustExec("set @@global.tidb_ddl_error_count_limit=3")
			t.Cleanup(func() { s.tk.MustExec("set @@global.tidb_ddl_error_count_limit=512") })
			s.tk.MustExec("create table p(a int primary key, b int, unique key idx(b) global) partition by range(a) (partition p0 values less than(10), partition p1 values less than(20), partition p2 values less than maxvalue)")
			s.tk.MustExec("insert into p values(1,1),(6,6),(11,11),(21,21)")
			var injected, rolledBack atomic.Bool
			var retries atomic.Int64
			var previousArgs, retryArgs string
			var previousState, retryState model.SchemaState
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeRunOneJobStep", func(job *model.Job) {
				if job.Type != action {
					return
				}
				if !injected.Load() {
					previousState = job.SchemaState
					previousArgs = string(job.RawArgs)
				} else if injected.Load() && retries.Load() == 1 {
					retryState, retryArgs = job.SchemaState, string(job.RawArgs)
				}
			})
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterAsyncNotifyEvent", func(job *model.Job, err *error) {
				if job.Type == action {
					retries.Add(1)
					if injected.CompareAndSwap(false, true) {
						*err = errors.New("transient error after notifier publication")
					}
				}
			})
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/afterRollbackTxnForMustRollbackTxnOnError", func(job *model.Job) {
				if job.Type == action {
					rolledBack.Store(true)
				}
			})
			if action == model.ActionDropTablePartition {
				s.tk.MustExec("alter table p drop partition p0")
				s.tk.MustQuery("select * from p order by a").Check(testkit.Rows("11 11", "21 21"))
				s.tk.MustQuery("select * from p use index(idx) order by a").Check(testkit.Rows("11 11", "21 21"))
			} else {
				s.tk.MustExec("alter table p reorganize partition p0 into (partition p00 values less than(5), partition p01 values less than(10))")
				s.tk.MustQuery("select * from p order by a").Check(testkit.Rows("1 1", "6 6", "11 11", "21 21"))
				s.tk.MustQuery("select * from p use index(idx) order by a").Check(testkit.Rows("1 1", "6 6", "11 11", "21 21"))
			}
			require.True(t, injected.Load())
			require.True(t, rolledBack.Load())
			require.NotEqual(t, model.StateNone, previousState)
			require.Equal(t, previousState, retryState)
			require.Equal(t, previousArgs, retryArgs, "rolled-back arguments must not be persisted")
			require.EqualValues(t, 2, retries.Load(), "the rolled-back notification must be republished")
			after := s.tableInfo(t, "p")
			require.Equal(t, after, s.persisted(t, after.ID).Clone())
			require.Equal(t, model.StateNone, after.Partition.DDLState)
			require.Equal(t, model.ActionNone, after.Partition.DDLAction)
			require.Empty(t, after.Partition.AddingDefinitions)
			require.Empty(t, after.Partition.DroppingDefinitions)
			names := make([]string, 0, len(after.Partition.Definitions))
			for _, def := range after.Partition.Definitions {
				names = append(names, def.Name.L)
			}
			if action == model.ActionDropTablePartition {
				require.Equal(t, []string{"p1", "p2"}, names)
			} else {
				require.Equal(t, []string{"p00", "p01", "p1", "p2"}, names)
			}
			s.tk.MustExec("admin check table p")
		})
	}
}

type affinity404PDClient struct {
	pdhttp.Client
}

func (*affinity404PDClient) notFoundErr() error {
	return fmt.Errorf("request pd http api failed with status: '%d %s', body: '404 page not found'", http.StatusNotFound, http.StatusText(http.StatusNotFound))
}

func (c *affinity404PDClient) CreateAffinityGroups(context.Context, map[string][]pdhttp.AffinityGroupKeyRange, ...pdhttp.CreateAffinityGroupsOption) (map[string]*pdhttp.AffinityGroupState, error) {
	return nil, c.notFoundErr()
}

func (c *affinity404PDClient) GetAffinityGroups(context.Context, []string) (map[string]*pdhttp.AffinityGroupState, error) {
	return nil, c.notFoundErr()
}

func (c *affinity404PDClient) GetAllAffinityGroups(context.Context) (map[string]*pdhttp.AffinityGroupState, error) {
	return nil, c.notFoundErr()
}

func (c *affinity404PDClient) BatchDeleteAffinityGroups(context.Context, []string, bool) error {
	return c.notFoundErr()
}

func TestShowAffinityAndDDLOnOldPDWithoutAffinityAPI(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_existing_aff(a int) affinity='table'")
	tk.MustExec("create table t_plain(a int)")

	// Switch to an old PD that does not serve the affinity HTTP API (returns 404).
	affinity.InitManager(&affinity404PDClient{})
	t.Cleanup(func() { affinity.InitManager(nil) })

	// SHOW AFFINITY preserves empty/NULL PD state instead of returning a SQL error.
	tk.MustQuery("show affinity like 't_existing_aff'").Check(
		testkit.Rows("test t_existing_aff <nil> <nil> <nil> <nil> <nil> <nil>"),
	)

	// Affinity-creating DDL fails with the PD 404 instead of succeeding without groups.
	tk.MustContainErrMsg("create table t_new_aff(a int) affinity='table'", "404 page not found")
	tk.MustQuery("show tables like 't_new_aff'").Check(testkit.Rows())
	tk.MustContainErrMsg("alter table t_plain affinity='table'", "404 page not found")

	// Reconciler recognizes unsupported PD and skips silently without error.
	require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
}

func TestAffinityDropDatabaseBoundedJobPayload(t *testing.T) {
	t.Run("700 affinity tables under 11000-byte entry limit and reconciler cleans overflow", func(t *testing.T) {
		ddlutil.EmulatorGCDisable()
		t.Cleanup(ddlutil.EmulatorGCEnable)

		store, dom := testkit.CreateMockStoreAndDomain(t)
		cli := newReconcilePDClient()
		affinity.InitManager(cli)
		t.Cleanup(func() { affinity.InitManager(nil) })

		tk := testkit.NewTestKit(t, store)
		tk.MustExec("create database db_700_aff")
		tk.MustExec("use db_700_aff")

		// Create 700 affinity tables via concurrent sessions to keep runtime fast.
		const tableCount = 700
		const workers = 10
		errCh := make(chan error, tableCount)
		var nextIdx atomic.Int32
		var wg sync.WaitGroup
		for range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				wtk := testkit.NewTestKit(t, store)
				wtk.MustExec("use db_700_aff")
				for {
					idx := int(nextIdx.Add(1) - 1)
					if idx >= tableCount {
						return
					}
					if _, err := wtk.Exec(fmt.Sprintf("create table t_%03d(a int) affinity='table'", idx)); err != nil {
						errCh <- err
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			require.NoError(t, err)
		}

		codec := store.GetCodec()
		allGroupIDs := make([]string, 0, tableCount)
		for i := range tableCount {
			tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("db_700_aff"), ast.NewCIStr(fmt.Sprintf("t_%03d", i)))
			require.NoError(t, err)
			gid := ddl.GetTableAffinityGroupID(codec, tbl.Meta().ID)
			require.True(t, cli.hasGroup(gid))
			allGroupIDs = append(allGroupIDs, gid)
		}

		// Apply a tight 11,000-byte entry-size limit before DROP DATABASE.
		origLimit := kv.TxnEntrySizeLimit.Load()
		kv.TxnEntrySizeLimit.Store(11000)
		t.Cleanup(func() { kv.TxnEntrySizeLimit.Store(origLimit) })

		tk.MustExec("drop database db_700_aff")

		dropDBJob := latestHistoryJobByType(t, tk, model.ActionDropSchema)
		encoded, err := dropDBJob.Encode(false)
		require.NoError(t, err)
		require.Less(t, len(encoded), 11000, "DROP DATABASE job_meta must stay below 11,000-byte entry limit")
		dropDBArgs, err := model.GetFinishedDropSchemaArgs(dropDBJob)
		require.NoError(t, err)
		require.Len(t, dropDBArgs.AllDroppedTableIDs, tableCount)
		require.Less(t, len(dropDBArgs.OldAffinityGroupIDs), tableCount, "OldAffinityGroupIDs must be bounded under 11,000-byte entry limit")

		// Restore entry limit and simulate GC completing delete ranges for the dropped DB.
		kv.TxnEntrySizeLimit.Store(origLimit)
		if len(dropDBArgs.OldAffinityGroupIDs) > 0 {
			require.NoError(t, affinity.DeleteGroups(context.Background(), dropDBArgs.OldAffinityGroupIDs))
		}
		tk.MustExec("delete from mysql.gc_delete_range where job_id = ?", dropDBJob.ID)

		// Reconciler must clean up all overflow groups in two consecutive rounds.
		zeroGrace := time.Duration(0)
		restore := ddl.ConfigureAffinityReconcilerForTest(dom.DDL(), ddl.AffinityReconcilerConfigForTest{
			GracePeriod: &zeroGrace,
		})
		t.Cleanup(restore)
		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		require.NoError(t, ddl.ReconcileAffinityOnceForTest(context.Background(), dom.DDL(), tk.Session()))
		for _, gid := range allGroupIDs {
			require.False(t, cli.hasGroup(gid), "overflow group %s must be cleaned up by reconciler after GC", gid)
		}
	})

	t.Run("1024 tables x 256 partitions stays well below 6 MiB in V1 and V2", func(t *testing.T) {
		const (
			tables     = 1024
			partitions = 256
			totalIDs   = tables * (partitions + 1)
			sixMiB     = 6 * 1024 * 1024
		)
		allDroppedTableIDs := make([]int64, 0, totalIDs)
		allGroupIDs := make([]string, 0, tables*partitions)
		nextID := int64(10000)
		for range tables {
			tid := nextID
			nextID++
			allDroppedTableIDs = append(allDroppedTableIDs, tid)
			for range partitions {
				pid := nextID
				nextID++
				allDroppedTableIDs = append(allDroppedTableIDs, pid)
				allGroupIDs = append(allGroupIDs, fmt.Sprintf("_tidb_pt_%d_p%d", tid, pid))
			}
		}

		for _, ver := range []model.JobVersion{model.JobVersion1, model.JobVersion2} {
			job := &model.Job{
				ID:         99999,
				Version:    ver,
				Type:       model.ActionDropSchema,
				SchemaID:   42,
				SchemaName: "db_large_aff",
				State:      model.JobStateDone,
			}
			args := &model.DropSchemaArgs{AllDroppedTableIDs: allDroppedTableIDs}
			job.FillFinishedArgs(args)
			args.OldAffinityGroupIDs = ddl.BoundOldAffinityGroupIDsForJobForTest(job, allGroupIDs)
			job.FillFinishedArgs(args)

			encoded, err := job.Encode(true)
			require.NoError(t, err)
			require.NotEmpty(t, args.OldAffinityGroupIDs)
			require.Less(t, len(args.OldAffinityGroupIDs), len(allGroupIDs))
			require.Less(t, len(encoded), 3*1024*1024, "version=%v encoded job (%d bytes) must stay well below 6 MiB (%d)", ver, len(encoded), sixMiB)

			var decoded model.Job
			require.NoError(t, decoded.Decode(encoded))
			decodedArgs, err := model.GetFinishedDropSchemaArgs(&decoded)
			require.NoError(t, err)
			require.Len(t, decodedArgs.AllDroppedTableIDs, totalIDs)
			require.Equal(t, args.OldAffinityGroupIDs, decodedArgs.OldAffinityGroupIDs)
		}
	})
}
