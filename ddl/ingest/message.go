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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Message const text
const (
	LitErrAllocMemFail      string = "[ddl-ingest] allocate memory failed"
	LitErrOutMaxMem         string = "[ddl-ingest] memory used up for lightning add index"
	LitErrCreateDirFail     string = "[ddl-ingest] create lightning sort path error"
	LitErrStatDirFail       string = "[ddl-ingest] stat lightning sort path error"
	LitErrDeleteDirFail     string = "[ddl-ingest] delete lightning sort path error"
	LitErrCreateBackendFail string = "[ddl-ingest] build lightning backend failed, will use kernel index reorg method to backfill the index"
	LitErrGetBackendFail    string = "[ddl-ingest]: Can not get cached backend"
	LitErrCreateEngineFail  string = "[ddl-ingest] build lightning engine failed, will use kernel index reorg method to backfill the index"
	LitErrCreateContextFail string = "[ddl-ingest] build lightning worker context failed, will use kernel index reorg method to backfill the index"
	LitErrGetEngineFail     string = "[ddl-ingest] can not get cached engine info"
	LitErrGetStorageQuota   string = "[ddl-ingest] get storage quota error"
	LitErrGetSysLimitErr    string = "[ddl-ingest] get system open file limit error"
	LitErrCloseEngineErr    string = "[ddl-ingest] close engine error"
	LitErrCleanEngineErr    string = "[ddl-ingest] clean engine error"
	LitErrFlushEngineErr    string = "[ddl-ingest] flush engine data err"
	LitErrIngestDataErr     string = "[ddl-ingest] ingest data into storage error"
	LitErrRemoteDupExistErr string = "[ddl-ingest] remote duplicate index key exist"
	LitErrExceedConcurrency string = "[ddl-ingest] the concurrency is greater than lightning limit(tikv-importer.range-concurrency)"
	LitErrUpdateDiskStats   string = "[ddl-ingest] update disk usage error"
	LitWarnEnvInitFail      string = "[ddl-ingest] initialize environment failed"
	LitWarnConfigError      string = "[ddl-ingest] build config for backend failed"
	LitWarnGenMemLimit      string = "[ddl-ingest] generate memory max limitation"
	LitInfoEnvInitSucc      string = "[ddl-ingest] init global lightning backend environment finished"
	LitInfoSortDir          string = "[ddl-ingest] the lightning sorted dir"
	LitInfoCreateBackend    string = "[ddl-ingest] create one backend for an DDL job"
	LitInfoCloseBackend     string = "[ddl-ingest] close one backend for DDL job"
	LitInfoOpenEngine       string = "[ddl-ingest] open an engine for index reorg task"
	LitInfoAddWriter        string = "[ddl-ingest] reuse engine and add a writer for index reorg task"
	LitInfoCreateWrite      string = "[ddl-ingest] create one local writer for index reorg task"
	LitInfoCloseEngine      string = "[ddl-ingest] flush all writer and get closed engine"
	LitInfoRemoteDupCheck   string = "[ddl-ingest] start remote duplicate checking"
	LitInfoStartImport      string = "[ddl-ingest] start to import data"
	LitInfoSetMemLimit      string = "[ddl-ingest] set max memory limitation"
	LitInfoChgMemSetting    string = "[ddl-ingest] change memory setting for lightning"
	LitInfoInitMemSetting   string = "[ddl-ingest] initial memory setting for lightning"
	LitInfoUnsafeImport     string = "[ddl-ingest] do a partial import data into the storage"
)

func genBackendAllocMemFailedErr(memRoot MemRoot, jobID int64) error {
	logutil.BgLogger().Warn(LitErrAllocMemFail, zap.Int64("job ID", jobID),
		zap.Int64("current memory usage", memRoot.CurrentUsage()),
		zap.Int64("max memory quota", memRoot.MaxMemoryQuota()))
	return errors.New(LitErrOutMaxMem)
}

func genEngineAllocMemFailedErr(memRoot MemRoot, jobID, idxID int64) error {
	logutil.BgLogger().Warn(LitErrAllocMemFail, zap.Int64("job ID", jobID),
		zap.Int64("index ID", idxID),
		zap.Int64("current memory usage", memRoot.CurrentUsage()),
		zap.Int64("max memory quota", memRoot.MaxMemoryQuota()))
	return errors.New(LitErrOutMaxMem)
}
