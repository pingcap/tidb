// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package summary

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTaskStatusPromLifecycle(t *testing.T) {
	dir := t.TempDir()
	mon := newTaskMonitor(func(string, ...zap.Field) {})

	mon.startTask(TaskStatusConfig{
		TaskType:          BackupUnit,
		Command:           "full",
		Scope:             "cluster",
		PDAddrs:           []string{"127.0.0.1:2379"},
		Storage:           "s3://bucket/path?access-key=111111&secret-access-key=222222&session-token=333333",
		TaskTS:            123,
		StatusTextfileDir: dir,
	})
	content := readOnlyPromFile(t, dir)
	assertPromGaugeValue(t, content, "br_task_running", "1")
	require.Contains(t, content, `access-key=xxxxxx`)
	require.Contains(t, content, `secret-access-key=xxxxxx`)
	require.Contains(t, content, `session-token=xxxxxx`)
	require.NotContains(t, content, "111111")
	require.NotContains(t, content, "222222")
	require.NotContains(t, content, "333333")

	mon.setTaskPhase("backup_ranges")
	content = readOnlyPromFile(t, dir)
	require.Contains(t, content, `phase="backup_ranges"`)

	mon.recordTaskSize(BackupDataSize, uint64(42))
	content = readOnlyPromFile(t, dir)
	assertPromGaugeValue(t, content, "br_task_last_size_bytes", "42")

	mon.setTaskSuccess(true)
	mon.finishTask()
	content = readOnlyPromFile(t, dir)
	assertPromGaugeValue(t, content, "br_task_running", "0")
	require.Contains(t, content, `phase="success"`)
}

func TestTaskStatusPromFailure(t *testing.T) {
	dir := t.TempDir()
	mon := newTaskMonitor(func(string, ...zap.Field) {})

	mon.startTask(TaskStatusConfig{
		TaskType:          RestoreUnit,
		Command:           "Full Restore",
		StatusTextfileDir: dir,
	})
	mon.recordTaskFailure("range-1")
	mon.setTaskSuccess(true)
	mon.finishTask()
	content := readOnlyPromFile(t, dir)
	assertPromGaugeValue(t, content, "br_task_running", "0")
	require.Contains(t, content, `phase="failed"`)
}

func TestTaskStatusPromDisabled(t *testing.T) {
	dir := t.TempDir()
	mon := newTaskMonitor(func(string, ...zap.Field) {})

	mon.recordTaskFailure("before-start")
	mon.setTaskSuccess(false)
	mon.finishTask()
	mon.startTask(TaskStatusConfig{
		TaskType:          BackupUnit,
		Command:           "Full Backup",
		StatusTextfileDir: "",
	})
	mon.setTaskPhase("backup_ranges")
	mon.recordTaskFailure("disabled")
	mon.setTaskSuccess(false)
	mon.finishTask()

	require.Empty(t, promFileNames(t, dir))

	mon.startTask(TaskStatusConfig{
		TaskType:          BackupUnit,
		Command:           "Full Backup",
		StatusTextfileDir: dir,
	})
	mon.setTaskSuccess(true)
	mon.finishTask()

	content := readOnlyPromFile(t, dir)
	assertPromGaugeValue(t, content, "br_task_running", "0")
	require.Contains(t, content, `phase="success"`)
}

func TestTaskStatusPromTerminalSnapshot(t *testing.T) {
	successDir := t.TempDir()
	successStatus := newTaskStatus(TaskStatusConfig{
		TaskType:          BackupUnit,
		Command:           "full",
		Scope:             "cluster",
		StatusTextfileDir: successDir,
	})
	require.NoError(t, writeTaskStatusSnapshot(successStatus.finish(true, time.Now())))
	successContent := readOnlyPromFile(t, successDir)
	assertPromGaugeValue(t, successContent, "br_task_running", "0")
	require.Contains(t, successContent, `phase="success"`)

	failedDir := t.TempDir()
	failedStatus := newTaskStatus(TaskStatusConfig{
		TaskType:          RestoreUnit,
		Command:           "Full Restore",
		StatusTextfileDir: failedDir,
	})
	require.NoError(t, writeTaskStatusSnapshot(failedStatus.finish(false, time.Now())))
	failedContent := readOnlyPromFile(t, failedDir)
	assertPromGaugeValue(t, failedContent, "br_task_running", "0")
	require.Contains(t, failedContent, `phase="failed"`)
}

func readOnlyPromFile(t *testing.T, dir string) string {
	t.Helper()
	promFiles := promFileNames(t, dir)
	require.Len(t, promFiles, 1)
	data, err := os.ReadFile(filepath.Join(dir, promFiles[0]))
	require.NoError(t, err)
	return string(data)
}

func promFileNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	promFiles := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".prom") {
			promFiles = append(promFiles, name)
		}
	}
	return promFiles
}

func assertPromGaugeValue(t *testing.T, content, name, value string) {
	t.Helper()
	for _, line := range strings.Split(content, "\n") {
		if strings.HasPrefix(line, name+"{") || strings.HasPrefix(line, name+" ") {
			require.True(t, strings.HasSuffix(line, " "+value), line)
			return
		}
	}
	require.Failf(t, "metric not found", "metric %s not found in content:\n%s", name, content)
}
