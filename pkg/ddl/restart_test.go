// Copyright 2020 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// this test file include some test that will cause data race, mainly because restartWorkers modify d.ctx

func getDDLSchemaVer(t *testing.T, d ddl.DDL) int64 {
	m, err := d.Stats(nil)
	require.NoError(t, err)
	v := m["ddl_schema_version"]
	return v.(int64)
}

// restartWorkers will stop the old DDL and create a new DDL and start it.
func restartWorkers(t *testing.T, store kv.Storage, d *domain.Domain) {
	err := d.DDL().Stop()
	require.NoError(t, err)
	newDDL := ddl.NewDDL(context.Background(), ddl.WithStore(d.Store()), ddl.WithInfoCache(d.InfoCache()), ddl.WithLease(d.DDL().GetLease()))
	d.SetDDL(newDDL)
	err = newDDL.Start(pools.NewResourcePool(func() (pools.Resource, error) {
		session := testkit.NewTestKit(t, store).Session()
		session.GetSessionVars().CommonGlobalLoaded = true
		return session, nil
	}, 128, 128, 5))
	require.NoError(t, err)
}

// runInterruptedJob should be called concurrently with restartWorkers
func runInterruptedJob(t *testing.T, store kv.Storage, d ddl.DDL, job *model.Job, doneCh chan error) {
	var (
		history *model.Job
		err     error
	)

	ctx := testkit.NewTestKit(t, store).Session()
	ctx.SetValue(sessionctx.QueryString, "skip")
	err = d.DoDDLJob(ctx, job)
	if errors.Is(err, context.Canceled) {
		endlessLoopTime := time.Now().Add(time.Minute)
		for history == nil {
			// imitate DoDDLJob's logic, quit only find history
			history, _ = ddl.GetHistoryJobByID(testkit.NewTestKit(t, store).Session(), job.ID)
			if history != nil {
				err = history.Error
			}
			time.Sleep(10 * testLease)
			if time.Now().After(endlessLoopTime) {
				err = errors.New("runInterruptedJob may enter endless loop")
				break
			}
		}
	}

	doneCh <- err
}

func testRunInterruptedJob(t *testing.T, store kv.Storage, d *domain.Domain, job *model.Job) {
	done := make(chan error, 1)
	go runInterruptedJob(t, store, d.DDL(), job, done)

	ticker := time.NewTicker(d.DDL().GetLease())
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			restartWorkers(t, store, d)
			time.Sleep(time.Millisecond * 20)
		case err := <-done:
			require.Nil(t, err)
			return
		}
	}
}

func TestSchemaResume(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	require.True(t, dom.DDL().OwnerManager().IsOwner())

	dbInfo, err := testSchemaInfo(store, "test_restart")
	require.NoError(t, err)
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []any{dbInfo},
	}
	testRunInterruptedJob(t, store, dom, job)
	testCheckSchemaState(t, store, dbInfo, model.StatePublic)

	job = buildDropSchemaJob(dbInfo)
	testRunInterruptedJob(t, store, dom, job)
	testCheckSchemaState(t, store, dbInfo, model.StateNone)
}

func TestStat(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	dbInfo, err := testSchemaInfo(store, "test_restart")
	require.NoError(t, err)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), dom.DDL(), dbInfo)

	job := buildDropSchemaJob(dbInfo)
	done := make(chan error, 1)
	go runInterruptedJob(t, store, dom.DDL(), job, done)

	ticker := time.NewTicker(dom.DDL().GetLease() * 1)
	defer ticker.Stop()
	ver := getDDLSchemaVer(t, dom.DDL())
LOOP:
	for {
		select {
		case <-ticker.C:
			require.GreaterOrEqual(t, getDDLSchemaVer(t, dom.DDL()), ver)
			restartWorkers(t, store, dom)
			time.Sleep(time.Millisecond * 20)
		case err := <-done:
			require.Nil(t, err)
			break LOOP
		}
	}
}

func TestTableResume(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, testLease)

	dbInfo, err := testSchemaInfo(store, "test_table")
	require.NoError(t, err)
	testCreateSchema(t, testkit.NewTestKit(t, store).Session(), dom.DDL(), dbInfo)
	defer func() {
		testDropSchema(t, testkit.NewTestKit(t, store).Session(), dom.DDL(), dbInfo)
	}()

	require.True(t, dom.DDL().OwnerManager().IsOwner())

	tblInfo, err := testTableInfo(store, "t1", 3)
	require.NoError(t, err)
	job := &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []any{tblInfo},
	}
	testRunInterruptedJob(t, store, dom, job)
	testCheckTableState(t, store, dbInfo, tblInfo, model.StatePublic)

	job = &model.Job{
		SchemaID:   dbInfo.ID,
		TableID:    tblInfo.ID,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	testRunInterruptedJob(t, store, dom, job)
	testCheckTableState(t, store, dbInfo, tblInfo, model.StateNone)
}
