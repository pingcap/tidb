// Copyright 2026 PingCAP, Inc.
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

package jobsubmit_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/jobsubmit"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func newAlterTableModeSession() sessionctx.Context {
	sctx := mock.NewContext()
	sctx.GetSessionVars().CDCWriteSource = 7
	sctx.GetSessionVars().SQLMode = mysql.ModeANSIQuotes
	return sctx
}

func TestBuildAlterTableModeJob(t *testing.T) {
	sctx := newAlterTableModeSession()
	target := model.AlterTableModeTarget{
		SchemaID:    101,
		SchemaName:  ast.NewCIStr("TestDB"),
		TableID:     202,
		TableName:   ast.NewCIStr("T1"),
		CurrentMode: model.TableModeNormal,
		TargetMode:  model.TableModeImport,
	}

	job, args, noop, err := jobsubmit.BuildAlterTableModeJob(sctx, target)
	require.NoError(t, err)
	require.False(t, noop)
	require.Equal(t, &model.AlterTableModeArgs{
		TableMode: model.TableModeImport,
		SchemaID:  101,
		TableID:   202,
	}, args)
	require.Equal(t, model.JobVersion2, job.Version)
	require.EqualValues(t, 101, job.SchemaID)
	require.EqualValues(t, 202, job.TableID)
	require.Equal(t, "testdb", job.SchemaName)
	require.Equal(t, model.ActionAlterTableMode, job.Type)
	require.NotNil(t, job.BinlogInfo)
	require.EqualValues(t, 7, job.CDCWriteSource)
	require.Equal(t, mysql.ModeANSIQuotes, job.SQLMode)
	require.Equal(t, "skip", job.Query)
	require.Equal(t, []model.InvolvingSchemaInfo{{
		Database: "testdb",
		Table:    "t1",
	}}, job.InvolvingSchemaInfo)
	require.Nil(t, sctx.Value(sessionctx.QueryString))
}

func TestBuildAlterTableModeJobNoopAndInvalidMode(t *testing.T) {
	sctx := newAlterTableModeSession()
	target := model.AlterTableModeTarget{
		SchemaID:    101,
		SchemaName:  ast.NewCIStr("test"),
		TableID:     202,
		TableName:   ast.NewCIStr("t"),
		CurrentMode: model.TableModeImport,
		TargetMode:  model.TableModeImport,
	}

	job, args, noop, err := jobsubmit.BuildAlterTableModeJob(sctx, target)
	require.NoError(t, err)
	require.True(t, noop)
	require.Nil(t, job)
	require.Nil(t, args)

	target.TargetMode = model.TableModeRestore
	job, args, noop, err = jobsubmit.BuildAlterTableModeJob(sctx, target)
	require.ErrorIs(t, err, infoschema.ErrInvalidTableModeSet)
	require.False(t, noop)
	require.Nil(t, job)
	require.Nil(t, args)
}
