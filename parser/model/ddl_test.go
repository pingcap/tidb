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
	"reflect"
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
	require.True(t, reflect.DeepEqual(job, clone))
}

func TestDDLJobSize(t *testing.T) {
	msg := `Please make sure that the following methods work as expected:
- SubJob.FromProxyJob()
- SubJob.ToProxyJob()
`
	job := model.Job{}
	require.Equal(t, 288, int(unsafe.Sizeof(job)), msg)
}
