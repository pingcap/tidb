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

package conflictrows

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type taskInfoGetterFunc func(context.Context, []int64) (map[int64]storage.TaskCleanupInfo, error)

func (f taskInfoGetterFunc) GetTaskCleanupInfoByIDs(
	ctx context.Context,
	taskIDs []int64,
) (map[int64]storage.TaskCleanupInfo, error) {
	return f(ctx, taskIDs)
}

type testStorage struct {
	storeapi.Storage
	walkOptions   []*storeapi.WalkOption
	failWalkErr   error
	deleteCalls   [][]string
	failDeleteAt  int
	failDeleteErr error
}

func (s *testStorage) WalkDir(
	ctx context.Context,
	opt *storeapi.WalkOption,
	fn func(string, int64) error,
) error {
	optionCopy := *opt
	s.walkOptions = append(s.walkOptions, &optionCopy)
	if s.failWalkErr != nil {
		return s.failWalkErr
	}
	return s.Storage.WalkDir(ctx, opt, fn)
}

func (s *testStorage) DeleteFiles(ctx context.Context, names []string) error {
	s.deleteCalls = append(s.deleteCalls, slices.Clone(names))
	if s.failDeleteAt > 0 && len(s.deleteCalls) == s.failDeleteAt {
		return s.failDeleteErr
	}
	return s.Storage.DeleteFiles(ctx, names)
}

func writeTestFiles(t *testing.T, store storeapi.Storage, names ...string) {
	t.Helper()
	for _, name := range names {
		require.NoError(t, store.WriteFile(context.Background(), name, []byte("row")))
	}
}

func requireTestFileExists(t *testing.T, store storeapi.Storage, name string, want bool) {
	t.Helper()
	exists, err := store.FileExists(context.Background(), name)
	require.NoError(t, err)
	require.Equal(t, want, exists, name)
}

func failedImportInfo(taskID int64) storage.TaskCleanupInfo {
	return storage.TaskCleanupInfo{ID: taskID, Type: proto.ImportInto, State: proto.TaskStateFailed}
}

func TestParseTaskID(t *testing.T) {
	testCases := []struct {
		name   string
		path   string
		wantID int64
		ok     bool
	}{
		{name: "valid", path: "conflicted-rows/42/data-0001.txt", wantID: 42, ok: true},
		{name: "nested descendant", path: "conflicted-rows/9223372036854775807/subtask/data", wantID: 9223372036854775807, ok: true},
		{name: "leading zero is decimal", path: "conflicted-rows/007/data", wantID: 7, ok: true},
		{name: "unrelated", path: "other/42/data"},
		{name: "lexical sibling", path: "conflicted-rows-old/42/data"},
		{name: "empty task", path: "conflicted-rows//data"},
		{name: "zero", path: "conflicted-rows/0/data"},
		{name: "negative", path: "conflicted-rows/-1/data"},
		{name: "explicit positive sign", path: "conflicted-rows/+1/data"},
		{name: "nondigit", path: "conflicted-rows/1a/data"},
		{name: "overflow", path: "conflicted-rows/9223372036854775808/data"},
		{name: "prefix only", path: "conflicted-rows/"},
		{name: "no descendant", path: "conflicted-rows/42"},
		{name: "empty descendant", path: "conflicted-rows/42/"},
		{name: "slash only descendant", path: "conflicted-rows/42//"},
		{name: "multiple slash only descendants", path: "conflicted-rows/42///"},
		{name: "deeper nonempty descendant", path: "conflicted-rows/42//data", wantID: 42, ok: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			gotID, ok := parseTaskID(testCase.path)
			require.Equal(t, testCase.ok, ok)
			require.Equal(t, testCase.wantID, gotID)
		})
	}
}

func TestShouldDelete(t *testing.T) {
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	justTooYoung := now.Add(-retention + time.Nanosecond)
	exactlyExpired := now.Add(-retention)
	old := now.Add(-200 * time.Hour)
	info := func(taskType proto.TaskType, state proto.TaskState, endTime *time.Time) storage.TaskCleanupInfo {
		return storage.TaskCleanupInfo{ID: 1, Type: taskType, State: state, EndTime: endTime}
	}

	testCases := []struct {
		name string
		info storage.TaskCleanupInfo
		want bool
	}{
		{name: "one nanosecond before expiry", info: info(proto.ImportInto, proto.TaskStateSucceed, &justTooYoung)},
		{name: "exact expiry", info: info(proto.ImportInto, proto.TaskStateSucceed, &exactlyExpired), want: true},
		{name: "success without end time", info: info(proto.ImportInto, proto.TaskStateSucceed, nil)},
		{name: "failed", info: info(proto.ImportInto, proto.TaskStateFailed, nil), want: true},
		{name: "reverted", info: info(proto.ImportInto, proto.TaskStateReverted, nil), want: true},
		{name: "active", info: info(proto.ImportInto, proto.TaskStateRunning, &old)},
		{name: "wrong type", info: info(proto.TaskTypeExample, proto.TaskStateFailed, &old)},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			require.Equal(t, testCase.want, shouldDelete(testCase.info, now))
		})
	}
}

func TestCleanFiles(t *testing.T) {
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

	t.Run("record task decisions", func(t *testing.T) {
		files := []string{"conflicted-rows/1/a", "conflicted-rows/1/b"}
		testCases := []struct {
			name              string
			info              *storage.TaskCleanupInfo
			shouldDeleteFiles bool
			wantStats         cleanupStats
		}{
			{
				name:              "missing task",
				shouldDeleteFiles: true,
				wantStats: cleanupStats{
					candidateTasks:        1,
					deletedTasks:          1,
					missingTasks:          1,
					missingTaskFiles:      2,
					firstMissingTaskIDs:   []int64{1},
					firstMissingTaskFiles: files,
				},
			},
			{
				name:              "non import into task",
				info:              &storage.TaskCleanupInfo{Type: proto.TaskTypeExample},
				shouldDeleteFiles: true,
				wantStats: cleanupStats{
					candidateTasks:              1,
					deletedTasks:                1,
					nonImportIntoTaskFiles:      2,
					firstNonImportIntoTaskFiles: files,
				},
			},
			{
				name:              "delete import task files",
				info:              &storage.TaskCleanupInfo{Type: proto.ImportInto},
				shouldDeleteFiles: true,
				wantStats: cleanupStats{
					candidateTasks: 1,
					deletedTasks:   1,
				},
			},
			{
				name: "retain task",
				info: &storage.TaskCleanupInfo{Type: proto.ImportInto},
				wantStats: cleanupStats{
					candidateTasks: 1,
					retainedTasks:  1,
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				stats := cleanupStats{}
				stats.recordTask(1, files, testCase.info, testCase.shouldDeleteFiles)
				require.Equal(t, testCase.wantStats, stats)
			})
		}
	})

	t.Run("mixed decisions", func(t *testing.T) {
		store := &testStorage{Storage: objstore.NewMemStorage()}
		files := []string{
			"conflicted-rows/1/a/data",
			"conflicted-rows/2/b/data",
			"conflicted-rows/3/c/data",
			"conflicted-rows/4/d/data",
			"conflicted-rows/bad/data",
			"conflicted-rows-old/1/data",
			"other/data",
		}
		writeTestFiles(t, store, files...)
		old := now.Add(-retention)
		getter := taskInfoGetterFunc(func(_ context.Context, taskIDs []int64) (map[int64]storage.TaskCleanupInfo, error) {
			require.ElementsMatch(t, []int64{1, 2, 3, 4}, taskIDs)
			return map[int64]storage.TaskCleanupInfo{
				1: failedImportInfo(1),
				2: {ID: 2, Type: proto.ImportInto, State: proto.TaskStateRunning},
				3: {ID: 3, Type: proto.TaskTypeExample, State: proto.TaskStateFailed},
				4: {ID: 4, Type: proto.ImportInto, State: proto.TaskStateSucceed, EndTime: &old},
			}, nil
		})

		stats, err := cleanFiles(context.Background(), store, getter, now)
		require.NoError(t, err)
		require.Equal(t, int64(4), stats.candidateTasks)
		require.Equal(t, int64(1), stats.retainedTasks)
		require.Equal(t, int64(3), stats.deletedTasks)
		require.Equal(t, int64(4), stats.deletedFiles)
		require.Zero(t, stats.missingTasks)
		require.Equal(t, int64(1), stats.nonImportIntoTaskFiles)
		require.Equal(t, int64(1), stats.unparsedTaskIDFiles)
		require.Zero(t, stats.failures)
		require.Equal(t, storagePrefix, store.walkOptions[0].SubDir)
		requireTestFileExists(t, store, files[0], false)
		requireTestFileExists(t, store, files[1], true)
		requireTestFileExists(t, store, files[2], false)
		requireTestFileExists(t, store, files[3], false)
		requireTestFileExists(t, store, files[4], false)
		requireTestFileExists(t, store, files[5], true)
		requireTestFileExists(t, store, files[6], true)
	})

	t.Run("task ID bound", func(t *testing.T) {
		store := &testStorage{Storage: objstore.NewMemStorage()}
		lookupSizes := make([]int, 0, 2)
		getter := taskInfoGetterFunc(func(_ context.Context, taskIDs []int64) (map[int64]storage.TaskCleanupInfo, error) {
			lookupSizes = append(lookupSizes, len(taskIDs))
			result := make(map[int64]storage.TaskCleanupInfo, len(taskIDs))
			for _, taskID := range taskIDs {
				result[taskID] = failedImportInfo(taskID)
			}
			return result, nil
		})
		for taskID := int64(1); taskID <= maxTaskIDsPerFlush+1; taskID++ {
			writeTestFiles(t, store, fmt.Sprintf("conflicted-rows/%03d/data", taskID))
		}

		stats, err := cleanFiles(context.Background(), store, getter, now)
		require.NoError(t, err)
		require.Equal(t, []int{maxTaskIDsPerFlush + 1}, lookupSizes)
		require.Equal(t, int64(maxTaskIDsPerFlush+1), stats.deletedFiles)
		require.Len(t, store.deleteCalls, 1)
		require.Len(t, store.deleteCalls[0], maxTaskIDsPerFlush+1)
	})

	t.Run("object bound keeps current callback", func(t *testing.T) {
		store := &testStorage{Storage: objstore.NewMemStorage()}
		lookupSizes := make([]int, 0, 2)
		getter := taskInfoGetterFunc(func(_ context.Context, taskIDs []int64) (map[int64]storage.TaskCleanupInfo, error) {
			lookupSizes = append(lookupSizes, len(taskIDs))
			return map[int64]storage.TaskCleanupInfo{1: failedImportInfo(1)}, nil
		})
		for i := range maxObjectsPerFlush + 1 {
			writeTestFiles(t, store, fmt.Sprintf("conflicted-rows/1/data-%04d", i))
		}

		stats, err := cleanFiles(context.Background(), store, getter, now)
		require.NoError(t, err)
		require.Equal(t, []int{1}, lookupSizes)
		require.Equal(t, int64(maxObjectsPerFlush+1), stats.deletedFiles)
		require.Len(t, store.deleteCalls, 1)
		require.Len(t, store.deleteCalls[0], maxObjectsPerFlush+1)
	})

	t.Run("later lookup failure and retry", func(t *testing.T) {
		store := &testStorage{Storage: objstore.NewMemStorage()}
		for taskID := int64(1); taskID <= maxTaskIDsPerFlush+1; taskID++ {
			writeTestFiles(t, store, fmt.Sprintf("conflicted-rows/%03d/data", taskID))
		}
		writeTestFiles(t, store, "conflicted-rows/999/retain")
		lookupErr := errors.New("lookup failed")
		lookupCount := 0
		getter := taskInfoGetterFunc(func(_ context.Context, taskIDs []int64) (map[int64]storage.TaskCleanupInfo, error) {
			lookupCount++
			if lookupCount == 2 {
				return nil, lookupErr
			}
			result := make(map[int64]storage.TaskCleanupInfo, len(taskIDs))
			for _, taskID := range taskIDs {
				if taskID != 999 {
					result[taskID] = failedImportInfo(taskID)
				} else {
					result[taskID] = storage.TaskCleanupInfo{ID: taskID, Type: proto.ImportInto, State: proto.TaskStateRunning}
				}
			}
			return result, nil
		})

		stats, err := cleanFiles(context.Background(), store, getter, now)
		require.ErrorIs(t, err, lookupErr)
		require.Equal(t, int64(maxTaskIDsPerFlush+1), stats.deletedFiles)
		require.Equal(t, int64(1), stats.failures)

		getter = func(_ context.Context, taskIDs []int64) (map[int64]storage.TaskCleanupInfo, error) {
			result := make(map[int64]storage.TaskCleanupInfo, len(taskIDs))
			for _, taskID := range taskIDs {
				if taskID == 999 {
					result[taskID] = storage.TaskCleanupInfo{ID: taskID, Type: proto.ImportInto, State: proto.TaskStateRunning}
				} else {
					result[taskID] = failedImportInfo(taskID)
				}
			}
			return result, nil
		}
		stats, err = cleanFiles(context.Background(), store, getter, now)
		require.NoError(t, err)
		require.Zero(t, stats.deletedFiles)
		requireTestFileExists(t, store, "conflicted-rows/999/retain", true)
	})

	t.Run("later delete failure and retry", func(t *testing.T) {
		deleteErr := errors.New("delete failed")
		store := &testStorage{
			Storage:       objstore.NewMemStorage(),
			failDeleteAt:  2,
			failDeleteErr: deleteErr,
		}
		for i := range maxObjectsPerFlush + 1 {
			writeTestFiles(t, store, fmt.Sprintf("conflicted-rows/1/data-%04d", i))
		}
		writeTestFiles(t, store, "conflicted-rows/2/missing-metadata")
		getter := taskInfoGetterFunc(func(_ context.Context, _ []int64) (map[int64]storage.TaskCleanupInfo, error) {
			return map[int64]storage.TaskCleanupInfo{1: failedImportInfo(1)}, nil
		})

		stats, err := cleanFiles(context.Background(), store, getter, now)
		require.ErrorIs(t, err, deleteErr)
		require.Equal(t, int64(1), stats.candidateTasks)
		require.Zero(t, stats.retainedTasks)
		require.Equal(t, int64(1), stats.deletedTasks)
		require.Equal(t, int64(maxObjectsPerFlush+1), stats.deletedFiles)
		require.Zero(t, stats.missingTasks)
		require.Empty(t, stats.firstMissingTaskIDs)
		require.Equal(t, int64(1), stats.failures)

		store.failDeleteAt = 0
		stats, err = cleanFiles(context.Background(), store, getter, now)
		require.NoError(t, err)
		require.Equal(t, int64(1), stats.deletedFiles)
		require.Equal(t, int64(1), stats.missingTasks)
		require.Equal(t, int64(1), stats.missingTaskFiles)
	})

	t.Run("missing metadata warning is bounded", func(t *testing.T) {
		core, logs := observer.New(zap.InfoLevel)
		restoreLog := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.InfoLevel)})
		t.Cleanup(restoreLog)
		store := objstore.NewMemStorage()
		for taskID := int64(1); taskID <= 20; taskID++ {
			writeTestFiles(t, store, fmt.Sprintf("conflicted-rows/%d/data", taskID))
		}

		stats, err := cleanFiles(context.Background(), store, taskInfoGetterFunc(
			func(context.Context, []int64) (map[int64]storage.TaskCleanupInfo, error) {
				return map[int64]storage.TaskCleanupInfo{}, nil
			}), now)
		require.NoError(t, err)
		require.Equal(t, int64(20), stats.candidateTasks)
		require.Zero(t, stats.retainedTasks)
		require.Equal(t, int64(20), stats.deletedTasks)
		require.Equal(t, int64(20), stats.deletedFiles)
		require.Equal(t, int64(20), stats.missingTasks)
		require.Equal(t, int64(20), stats.missingTaskFiles)
		require.Len(t, stats.firstMissingTaskIDs, maxLoggedMissingTaskIDs)
		require.Equal(t, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, stats.firstMissingTaskIDs)
		require.Len(t, stats.firstMissingTaskFiles, maxLoggedUnexpectedFiles)

		warningLogs := logs.FilterMessage("conflict-row cleanup deleted objects without task metadata").All()
		require.Len(t, warningLogs, 1)
		require.Equal(t, int64(20), warningLogs[0].ContextMap()["missing-tasks"])
		require.Len(t, warningLogs[0].ContextMap()["file-samples"], maxLoggedUnexpectedFiles)
		summaryLogs := logs.FilterMessage("finished conflict-row file cleanup").All()
		require.Len(t, summaryLogs, 1)
		require.Equal(t, int64(20), summaryLogs[0].ContextMap()["candidate-tasks"])
		require.Equal(t, int64(20), summaryLogs[0].ContextMap()["missing-tasks"])
		require.Equal(t, int64(20), summaryLogs[0].ContextMap()["missing-task-files"])
	})

	t.Run("unexpected file diagnostics are bounded", func(t *testing.T) {
		core, logs := observer.New(zap.InfoLevel)
		restoreLog := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.InfoLevel)})
		t.Cleanup(restoreLog)
		store := objstore.NewMemStorage()
		for i := range 20 {
			writeTestFiles(t, store,
				fmt.Sprintf("conflicted-rows/1/data-%02d", i),
				fmt.Sprintf("conflicted-rows/not-a-task-%02d/data", i),
			)
		}
		getter := taskInfoGetterFunc(func(_ context.Context, taskIDs []int64) (map[int64]storage.TaskCleanupInfo, error) {
			require.Equal(t, []int64{1}, taskIDs)
			return map[int64]storage.TaskCleanupInfo{
				1: {ID: 1, Type: proto.TaskTypeExample, State: proto.TaskStateFailed},
			}, nil
		})

		stats, err := cleanFiles(context.Background(), store, getter, now)
		require.NoError(t, err)
		require.Equal(t, int64(1), stats.candidateTasks)
		require.Equal(t, int64(1), stats.deletedTasks)
		require.Equal(t, int64(40), stats.deletedFiles)
		require.Equal(t, int64(20), stats.nonImportIntoTaskFiles)
		require.Equal(t, int64(20), stats.unparsedTaskIDFiles)
		require.Len(t, stats.firstNonImportIntoTaskFiles, maxLoggedUnexpectedFiles)
		require.Len(t, stats.firstUnparsedTaskIDFiles, maxLoggedUnexpectedFiles)

		nonImportLogs := logs.FilterMessage("conflict-row cleanup deleted objects for non-import-into tasks").All()
		require.Len(t, nonImportLogs, 1)
		require.Len(t, nonImportLogs[0].ContextMap()["file-samples"], maxLoggedUnexpectedFiles)
		unparsedLogs := logs.FilterMessage("conflict-row cleanup deleted objects with unparsed task IDs").All()
		require.Len(t, unparsedLogs, 1)
		require.Len(t, unparsedLogs[0].ContextMap()["file-samples"], maxLoggedUnexpectedFiles)
	})

	t.Run("unparsed files respect the object bound", func(t *testing.T) {
		store := &testStorage{Storage: objstore.NewMemStorage()}
		for i := range maxObjectsPerFlush + 1 {
			writeTestFiles(t, store, fmt.Sprintf("conflicted-rows/not-a-task/data-%04d", i))
		}

		stats, err := cleanFiles(context.Background(), store, taskInfoGetterFunc(
			func(context.Context, []int64) (map[int64]storage.TaskCleanupInfo, error) {
				require.FailNow(t, "metadata lookup should not run")
				return nil, nil
			}), now)
		require.NoError(t, err)
		require.Zero(t, stats.candidateTasks)
		require.Equal(t, int64(maxObjectsPerFlush+1), stats.unparsedTaskIDFiles)
		require.Equal(t, int64(maxObjectsPerFlush+1), stats.deletedFiles)
		require.Len(t, store.deleteCalls, 1)
		require.Len(t, store.deleteCalls[0], maxObjectsPerFlush+1)
	})

	t.Run("canceled context", func(t *testing.T) {
		store := objstore.NewMemStorage()
		writeTestFiles(t, store, "conflicted-rows/1/data")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		stats, err := cleanFiles(ctx, store, taskInfoGetterFunc(
			func(context.Context, []int64) (map[int64]storage.TaskCleanupInfo, error) {
				require.FailNow(t, "metadata lookup should not run")
				return nil, nil
			}), now)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, int64(1), stats.failures)
	})
}

func TestCleanExpiredFiles(t *testing.T) {
	t.Run("empty URI", func(t *testing.T) {
		require.NoError(t, CleanExpiredFiles(context.Background(), nil, ""))
	})

	t.Run("open error does not log URI", func(t *testing.T) {
		const credentialURI = "unsupported://access:secret@example/bucket"
		core, logs := observer.New(zap.InfoLevel)
		restoreLog := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.InfoLevel)})
		t.Cleanup(restoreLog)

		err := CleanExpiredFiles(context.Background(), nil, credentialURI)
		require.Error(t, err)
		for _, entry := range logs.All() {
			require.NotContains(t, entry.Message+fmt.Sprint(entry.ContextMap()), credentialURI)
			require.NotContains(t, entry.Message+fmt.Sprint(entry.ContextMap()), "secret")
		}
		summaryLogs := logs.FilterMessage("finished conflict-row file cleanup").All()
		require.Len(t, summaryLogs, 1)
		require.Equal(t, int64(1), summaryLogs[0].ContextMap()["failures"])
	})
}
