// Copyright 2021 PingCAP, Inc.
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

package domain

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/planner/extstore"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/stretchr/testify/require"
)

func TestPlanReplayerDifferentGC(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	storage, err := extstore.CreateGlobalExtStorage("file://"+tempDir, "")
	require.NoError(t, err)
	defer func() {
		storage.Close()
	}()

	dirName := replayer.GetPlanReplayerDirName()

	time1 := time.Now().Add(-7 * 25 * time.Hour).UnixNano()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/replayer/InjectPlanReplayerFileNameTimeField", fmt.Sprintf("return(%d)", time1)))
	file1, fileName1, err := replayer.GeneratePlanReplayerFile(ctx, storage, true, false, false)
	require.NoError(t, err)
	require.NoError(t, file1.Close())
	filePath1 := filepath.Join(dirName, fileName1)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/replayer/InjectPlanReplayerFileNameTimeField"))

	time2 := time.Now().Add(-7 * 23 * time.Hour).UnixNano()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/replayer/InjectPlanReplayerFileNameTimeField", fmt.Sprintf("return(%d)", time2)))
	file2, fileName2, err := replayer.GeneratePlanReplayerFile(ctx, storage, true, false, false)
	require.NoError(t, err)
	require.NoError(t, file2.Close())
	filePath2 := filepath.Join(dirName, fileName2)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/replayer/InjectPlanReplayerFileNameTimeField"))

	time3 := time.Now().Add(-2 * time.Hour).UnixNano()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/replayer/InjectPlanReplayerFileNameTimeField", fmt.Sprintf("return(%d)", time3)))
	file3, fileName3, err := replayer.GeneratePlanReplayerFile(ctx, storage, false, false, false)
	require.NoError(t, err)
	require.NoError(t, file3.Close())
	filePath3 := filepath.Join(dirName, fileName3)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/replayer/InjectPlanReplayerFileNameTimeField"))

	time4 := time.Now().UnixNano()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/replayer/InjectPlanReplayerFileNameTimeField", fmt.Sprintf("return(%d)", time4)))
	file4, fileName4, err := replayer.GeneratePlanReplayerFile(ctx, storage, false, false, false)
	require.NoError(t, err)
	require.NoError(t, file4.Close())
	filePath4 := filepath.Join(dirName, fileName4)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/replayer/InjectPlanReplayerFileNameTimeField"))

	handler := &dumpFileGcChecker{
		paths: []string{dirName},
	}
	handler.GCDumpFiles(ctx, time.Hour, time.Hour*24*7)
	exists, err := storage.FileExists(ctx, filePath1)
	require.NoError(t, err)
	require.False(t, exists)
	exists, err = storage.FileExists(ctx, filePath2)
	require.NoError(t, err)
	require.True(t, exists)
	exists, err = storage.FileExists(ctx, filePath3)
	require.NoError(t, err)
	require.False(t, exists)
	exists, err = storage.FileExists(ctx, filePath4)
	require.NoError(t, err)
	require.True(t, exists)

	handler.GCDumpFiles(ctx, 0, 0)
	exists, err = storage.FileExists(ctx, filePath2)
	require.NoError(t, err)
	require.False(t, exists)
	exists, err = storage.FileExists(ctx, filePath4)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestDumpGCFileParseTime(t *testing.T) {
	nowTime := time.Now()
	name1 := fmt.Sprintf("replayer_single_xxxxxx_%v.zip", nowTime.UnixNano())
	pt, err := parseTime(name1)
	require.NoError(t, err)
	require.True(t, pt.Equal(nowTime))

	name2 := fmt.Sprintf("replayer_single_xxxxxx_%v1.zip", nowTime.UnixNano())
	_, err = parseTime(name2)
	require.NotNil(t, err)

	name3 := fmt.Sprintf("replayer_single_xxxxxx_%v._zip", nowTime.UnixNano())
	_, err = parseTime(name3)
	require.NotNil(t, err)

	name4 := "extract_-brq6zKMarD9ayaifkHc4A==_1678168728477502000.zip"
	_, err = parseTime(name4)
	require.NoError(t, err)

	var pName string
	pName, err = replayer.GeneratePlanReplayerFileName(false, false, false)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(true, false, false)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(false, true, false)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(true, true, false)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(false, false, true)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(true, false, true)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(false, true, true)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)

	pName, err = replayer.GeneratePlanReplayerFileName(true, true, true)
	require.NoError(t, err)
	_, err = parseTime(pName)
	require.NoError(t, err)
}

func TestSendTask(t *testing.T) {
	h := &planReplayerHandle{
		planReplayerTaskDumpHandle: &planReplayerTaskDumpHandle{
			taskCH: make(chan *PlanReplayerDumpTask, 1),
		},
		planReplayerTaskCollectorHandle: &planReplayerTaskCollectorHandle{},
	}
	task1 := &PlanReplayerDumpTask{}
	task2 := &PlanReplayerDumpTask{}
	h.SendTask(task1)
	success := h.SendTask(task2)
	require.False(t, success)
}
