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
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Message const text
const (
	LitErrAllocMemFail      string = "[ddl-ingest] allocate memory failed"
	LitErrCreateDirFail     string = "[ddl-ingest] create ingest sort path error"
	LitErrStatDirFail       string = "[ddl-ingest] stat ingest sort path error"
	LitErrDeleteDirFail     string = "[ddl-ingest] delete ingest sort path error"
	LitErrCreateBackendFail string = "[ddl-ingest] build ingest backend failed"
	LitErrGetBackendFail    string = "[ddl-ingest] cannot get ingest backend"
	LitErrCreateEngineFail  string = "[ddl-ingest] build ingest engine failed"
	LitErrCreateContextFail string = "[ddl-ingest] build ingest writer context failed"
	LitErrGetEngineFail     string = "[ddl-ingest] can not get ingest engine info"
	LitErrGetStorageQuota   string = "[ddl-ingest] get storage quota error"
	LitErrCloseEngineErr    string = "[ddl-ingest] close engine error"
	LitErrCleanEngineErr    string = "[ddl-ingest] clean engine error"
	LitErrFlushEngineErr    string = "[ddl-ingest] flush engine data err"
	LitErrIngestDataErr     string = "[ddl-ingest] ingest data into storage error"
	LitErrRemoteDupExistErr string = "[ddl-ingest] remote duplicate index key exist"
	LitErrExceedConcurrency string = "[ddl-ingest] the concurrency is greater than ingest limit"
	LitErrUpdateDiskStats   string = "[ddl-ingest] update disk usage error"
	LitWarnEnvInitFail      string = "[ddl-ingest] initialize environment failed"
	LitWarnConfigError      string = "[ddl-ingest] build config for backend failed"
	LitInfoEnvInitSucc      string = "[ddl-ingest] init global ingest backend environment finished"
	LitInfoSortDir          string = "[ddl-ingest] the ingest sorted directory"
	LitInfoCreateBackend    string = "[ddl-ingest] create one backend for an DDL job"
	LitInfoCloseBackend     string = "[ddl-ingest] close one backend for DDL job"
	LitInfoOpenEngine       string = "[ddl-ingest] open an engine for index reorg task"
	LitInfoAddWriter        string = "[ddl-ingest] reuse engine and add a writer for index reorg task"
	LitInfoCreateWrite      string = "[ddl-ingest] create one local writer for index reorg task"
	LitInfoCloseEngine      string = "[ddl-ingest] flush all writer and get closed engine"
	LitInfoRemoteDupCheck   string = "[ddl-ingest] start remote duplicate checking"
	LitInfoStartImport      string = "[ddl-ingest] start to import data"
	LitInfoChgMemSetting    string = "[ddl-ingest] change memory setting for ingest"
	LitInfoInitMemSetting   string = "[ddl-ingest] initial memory setting for ingest"
	LitInfoUnsafeImport     string = "[ddl-ingest] do a partial import data into the storage"
	LitErrCloseWriterErr    string = "[ddl-ingest] close writer error"
)

func genBackendAllocMemFailedErr(memRoot MemRoot, jobID int64) error {
	logutil.BgLogger().Warn(LitErrAllocMemFail, zap.Int64("job ID", jobID),
		zap.Int64("current memory usage", memRoot.CurrentUsage()),
		zap.Int64("max memory quota", memRoot.MaxMemoryQuota()))
	return dbterror.ErrIngestFailed.FastGenByArgs("memory used up")
}

func genEngineAllocMemFailedErr(memRoot MemRoot, jobID, idxID int64) error {
	logutil.BgLogger().Warn(LitErrAllocMemFail, zap.Int64("job ID", jobID),
		zap.Int64("index ID", idxID),
		zap.Int64("current memory usage", memRoot.CurrentUsage()),
		zap.Int64("max memory quota", memRoot.MaxMemoryQuota()))
	return dbterror.ErrIngestFailed.FastGenByArgs("memory used up")
}
