// Copyright 2022 PingCAP, Inc.
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

package ddl

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_getTTLInfoInOptions(t *testing.T) {
	falseValue := false
	trueValue := true
	twentyFiveHours := "25h"

	cases := []struct {
		options            []*ast.TableOption
		ttlInfo            *model.TTLInfo
		ttlEnable          *bool
		ttlCronJobSchedule *string
		err                error
	}{
		{
			[]*ast.TableOption{},
			nil,
			nil,
			nil,
			nil,
		},
		{
			[]*ast.TableOption{
				{
					Tp:            ast.TableOptionTTL,
					ColumnName:    &ast.ColumnName{Name: ast.NewCIStr("test_column")},
					Value:         ast.NewValueExpr(5, "", ""),
					TimeUnitValue: &ast.TimeUnitExpr{Unit: ast.TimeUnitYear},
				},
			},
			&model.TTLInfo{
				ColumnName:       ast.NewCIStr("test_column"),
				IntervalExprStr:  "5",
				IntervalTimeUnit: int(ast.TimeUnitYear),
				Enable:           true,
				JobInterval:      model.DefaultTTLJobInterval,
			},
			nil,
			nil,
			nil,
		},
		{
			[]*ast.TableOption{
				{
					Tp:        ast.TableOptionTTLEnable,
					BoolValue: false,
				},
				{
					Tp:            ast.TableOptionTTL,
					ColumnName:    &ast.ColumnName{Name: ast.NewCIStr("test_column")},
					Value:         ast.NewValueExpr(5, "", ""),
					TimeUnitValue: &ast.TimeUnitExpr{Unit: ast.TimeUnitYear},
				},
			},
			&model.TTLInfo{
				ColumnName:       ast.NewCIStr("test_column"),
				IntervalExprStr:  "5",
				IntervalTimeUnit: int(ast.TimeUnitYear),
				Enable:           false,
				JobInterval:      model.DefaultTTLJobInterval,
			},
			&falseValue,
			nil,
			nil,
		},
		{
			[]*ast.TableOption{
				{
					Tp:        ast.TableOptionTTLEnable,
					BoolValue: false,
				},
				{
					Tp:            ast.TableOptionTTL,
					ColumnName:    &ast.ColumnName{Name: ast.NewCIStr("test_column")},
					Value:         ast.NewValueExpr(5, "", ""),
					TimeUnitValue: &ast.TimeUnitExpr{Unit: ast.TimeUnitYear},
				},
				{
					Tp:        ast.TableOptionTTLEnable,
					BoolValue: true,
				},
			},
			&model.TTLInfo{
				ColumnName:       ast.NewCIStr("test_column"),
				IntervalExprStr:  "5",
				IntervalTimeUnit: int(ast.TimeUnitYear),
				Enable:           true,
				JobInterval:      model.DefaultTTLJobInterval,
			},
			&trueValue,
			nil,
			nil,
		},
		{
			[]*ast.TableOption{
				{
					Tp:            ast.TableOptionTTL,
					ColumnName:    &ast.ColumnName{Name: ast.NewCIStr("test_column")},
					Value:         ast.NewValueExpr(5, "", ""),
					TimeUnitValue: &ast.TimeUnitExpr{Unit: ast.TimeUnitYear},
				},
				{
					Tp:       ast.TableOptionTTLJobInterval,
					StrValue: "25h",
				},
			},
			&model.TTLInfo{
				ColumnName:       ast.NewCIStr("test_column"),
				IntervalExprStr:  "5",
				IntervalTimeUnit: int(ast.TimeUnitYear),
				Enable:           true,
				JobInterval:      "25h",
			},
			nil,
			&twentyFiveHours,
			nil,
		},
	}

	for _, c := range cases {
		ttlInfo, ttlEnable, ttlCronJobSchedule, err := getTTLInfoInOptions(c.options)

		assert.Equal(t, c.ttlInfo, ttlInfo)
		assert.Equal(t, c.ttlEnable, ttlEnable)
		assert.Equal(t, c.ttlCronJobSchedule, ttlCronJobSchedule)
		assert.Equal(t, c.err, err)
	}
}

type fakeExternalWorkloadManager struct {
	role             config.ExternalWorkloadRole
	registeredTable  int64
	registerEnabled  bool
	deletedTable     int64
	recycledCreateTS uint64
	updatedEnable    *bool
	registerErr      error
}

func (m *fakeExternalWorkloadManager) Close() error { return nil }
func (m *fakeExternalWorkloadManager) Role() config.ExternalWorkloadRole {
	return m.role
}
func (*fakeExternalWorkloadManager) Meta() *keyspacepb.KeyspaceMeta { return nil }
func (*fakeExternalWorkloadManager) InitializeGCV2(context.Context) error {
	return nil
}
func (*fakeExternalWorkloadManager) AbortGCV2(context.Context) error { return nil }
func (*fakeExternalWorkloadManager) RegisterGCV2(context.Context, uint64, int64) error {
	return nil
}
func (*fakeExternalWorkloadManager) RecycleGCV2(context.Context, uint64) error {
	return nil
}
func (*fakeExternalWorkloadManager) UpdateGCLifeTime(context.Context, int64) error {
	return nil
}
func (m *fakeExternalWorkloadManager) RegisterTTLTask(_ context.Context, tableID int64, ttlJobEnable bool) error {
	m.registeredTable = tableID
	m.registerEnabled = ttlJobEnable
	return m.registerErr
}
func (m *fakeExternalWorkloadManager) DeleteTTLTableInfo(_ context.Context, tableID int64) error {
	m.deletedTable = tableID
	return nil
}
func (m *fakeExternalWorkloadManager) RecycleTTLTask(_ context.Context, completedJobCreateTime uint64) error {
	m.recycledCreateTS = completedJobCreateTime
	return nil
}
func (m *fakeExternalWorkloadManager) UpdateTTLJobEnable(_ context.Context, ttlJobEnable bool) error {
	m.updatedEnable = &ttlJobEnable
	return nil
}
func (*fakeExternalWorkloadManager) RegisterAutoAnalyze(context.Context, uint64) error {
	return nil
}
func (*fakeExternalWorkloadManager) RecycleAutoAnalyze(context.Context, uint64) error {
	return nil
}

func TestExternalWorkloadTTLTableReportsOnlyFromMaster(t *testing.T) {
	origEnable := vardef.EnableTTLJob.Load()
	vardef.EnableTTLJob.Store(false)
	defer vardef.EnableTTLJob.Store(origEnable)

	tblInfo := &model.TableInfo{
		ID: 123,
		TTLInfo: &model.TTLInfo{
			Enable: true,
		},
	}

	master := &fakeExternalWorkloadManager{role: config.RoleMaster}
	dc := &ddlCtx{extWorkload: master}
	require.NoError(t, dc.registerTTLTableToExternalWorkload(context.Background(), tblInfo))
	require.Equal(t, int64(123), master.registeredTable)
	require.False(t, master.registerEnabled)

	dc.deleteTTLTableFromExternalWorkload(context.Background(), tblInfo.ID)
	require.Equal(t, int64(123), master.deletedTable)

	ttlWorker := &fakeExternalWorkloadManager{role: config.RoleTTLTaskWorker}
	dc = &ddlCtx{extWorkload: ttlWorker}
	require.NoError(t, dc.registerTTLTableToExternalWorkload(context.Background(), tblInfo))
	dc.deleteTTLTableFromExternalWorkload(context.Background(), tblInfo.ID)
	require.Zero(t, ttlWorker.registeredTable)
	require.Zero(t, ttlWorker.deletedTable)
}

func TestExternalWorkloadTTLTableRegisterSkipsDisabledTTL(t *testing.T) {
	manager := &fakeExternalWorkloadManager{role: config.RoleMaster}
	dc := &ddlCtx{extWorkload: manager}
	require.NoError(t, dc.registerTTLTableToExternalWorkload(context.Background(), &model.TableInfo{
		ID:      123,
		TTLInfo: &model.TTLInfo{Enable: false},
	}))
	require.Zero(t, manager.registeredTable)
}

func TestExternalWorkloadTTLTableRegisterReturnsError(t *testing.T) {
	boom := errors.New("boom")
	manager := &fakeExternalWorkloadManager{role: config.RoleMaster, registerErr: boom}
	dc := &ddlCtx{extWorkload: manager}
	err := dc.registerTTLTableToExternalWorkload(context.Background(), &model.TableInfo{
		ID:      123,
		TTLInfo: &model.TTLInfo{Enable: true},
	})
	require.ErrorIs(t, err, boom)
}
