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
// See the License for the specific language governing permissions and
// limitations under the License.

package model_test

import (
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
)

func TestJobClone(t *testing.T) {
	job := &model.Job{
		ID:              100,
		Type:            model.ActionCreateTable,
		SchemaID:        101,
		TableID:         102,
		SchemaName:      "test",
		TableName:       "t",
		State:           model.JobStateDone,
		MultiSchemaInfo: nil,
	}
	clone := job.Clone()
	require.Equal(t, job.ID, clone.ID)
	require.Equal(t, job.Type, clone.Type)
	require.Equal(t, job.SchemaID, clone.SchemaID)
	require.Equal(t, job.TableID, clone.TableID)
	require.Equal(t, job.SchemaName, clone.SchemaName)
	require.Equal(t, job.TableName, clone.TableName)
	require.Equal(t, job.State, clone.State)
	require.Equal(t, job.MultiSchemaInfo, clone.MultiSchemaInfo)
}

func TestJobSize(t *testing.T) {
	msg := `Please make sure that the following methods work as expected:
- SubJob.FromProxyJob()
- SubJob.ToProxyJob()
`
	job := model.Job{}
	require.Equal(t, 288, int(unsafe.Sizeof(job)), msg)
}

func TestBackfillMetaCodec(t *testing.T) {
	jm := &model.JobMeta{
		SchemaID: 1,
		TableID:  2,
		Query:    "alter table t add index idx(a)",
		Priority: 1,
	}
	bm := &model.BackfillMeta{
		EndInclude: true,
		ErrMsg:     "has a err",
		JobMeta:    jm,
	}
	bmBytes, err := bm.Encode()
	require.NoError(t, err)
	bmRet := &model.BackfillMeta{}
	bmRet.Decode(bmBytes)
	require.Equal(t, bm, bmRet)
}
