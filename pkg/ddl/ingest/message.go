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

package ingest

import (
	"context"

	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// Message const text
const (
	LitErrAllocMemFail      string = "allocate memory failed"
	LitErrCreateDirFail     string = "create ingest sort path error"
	LitErrStatDirFail       string = "stat ingest sort path error"
	LitErrCreateBackendFail string = "build ingest backend failed"
	LitErrGetBackendFail    string = "cannot get ingest backend"
	LitErrCreateEngineFail  string = "build ingest engine failed"
	LitErrCreateContextFail string = "build ingest writer context failed"
	LitErrGetEngineFail     string = "can not get ingest engine info"
	LitErrGetStorageQuota   string = "get storage quota error"
	LitErrCloseEngineErr    string = "close engine error"
	LitErrCleanEngineErr    string = "clean engine error"
	LitErrFlushEngineErr    string = "flush engine data err"
	LitErrIngestDataErr     string = "ingest data into storage error"
	LitErrRemoteDupExistErr string = "remote duplicate index key exist"
	LitErrExceedConcurrency string = "the concurrency is greater than ingest limit"
	LitErrCloseWriterErr    string = "close writer error"
	LitErrReadSortPath      string = "cannot read sort path"
	LitErrCleanSortPath     string = "cannot cleanup sort path"
	LitErrResetEngineFail   string = "reset engine failed"
	LitWarnEnvInitFail      string = "initialize environment failed"
	LitWarnConfigError      string = "build config for backend failed"
	LitInfoEnvInitSucc      string = "init global ingest backend environment finished"
	LitInfoSortDir          string = "the ingest sorted directory"
	LitInfoCreateBackend    string = "create one backend for an DDL job"
	LitInfoCloseBackend     string = "close one backend for DDL job"
	LitInfoOpenEngine       string = "open an engine for index reorg task"
	LitInfoAddWriter        string = "reuse engine and add a writer for index reorg task"
	LitInfoCreateWrite      string = "create one local writer for index reorg task"
	LitInfoCloseEngine      string = "flush all writer and get closed engine"
	LitInfoRemoteDupCheck   string = "start remote duplicate checking"
	LitInfoStartImport      string = "start to import data"
	LitInfoChgMemSetting    string = "change memory setting for ingest"
	LitInfoInitMemSetting   string = "initial memory setting for ingest"
	LitInfoUnsafeImport     string = "do a partial import data into the storage"
)

func genBackendAllocMemFailedErr(ctx context.Context, memRoot MemRoot, jobID int64) error {
	logutil.Logger(ctx).Warn(LitErrAllocMemFail, zap.Int64("job ID", jobID),
		zap.Int64("current memory usage", memRoot.CurrentUsage()),
		zap.Int64("max memory quota", memRoot.MaxMemoryQuota()))
	return dbterror.ErrIngestFailed.FastGenByArgs("memory used up")
}

func genEngineAllocMemFailedErr(ctx context.Context, memRoot MemRoot, jobID int64, idxIDs []int64) error {
	logutil.Logger(ctx).Warn(LitErrAllocMemFail,
		zap.Int64("job ID", jobID),
		zap.Int64s("index IDs", idxIDs),
		zap.Int64("current memory usage", memRoot.CurrentUsage()),
		zap.Int64("max memory quota", memRoot.MaxMemoryQuota()))
	return dbterror.ErrIngestFailed.FastGenByArgs("memory used up")
}

func genWriterAllocMemFailedErr(ctx context.Context, memRoot MemRoot, jobID int64, idxID int64) error {
	logutil.Logger(ctx).Warn(LitErrAllocMemFail,
		zap.Int64("job ID", jobID),
		zap.Int64("index ID", idxID),
		zap.Int64("current memory usage", memRoot.CurrentUsage()),
		zap.Int64("max memory quota", memRoot.MaxMemoryQuota()))
	return dbterror.ErrIngestFailed.FastGenByArgs("memory used up")
}
