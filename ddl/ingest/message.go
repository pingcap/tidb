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
	LitErrAllocMemFail      string = "[ddl-lightning] allocate memory failed"
	LitErrOutMaxMem         string = "[ddl-lightning] memory used up for lightning add index"
	LitErrCreateDirFail     string = "[ddl-lightning] create lightning sort path error"
	LitErrStatDirFail       string = "[ddl-lightning] stat lightning sort path error"
	LitErrDeleteDirFail     string = "[ddl-lightning] delete lightning sort path error"
	LitErrCreateBackendFail string = "[ddl-lightning] build lightning backend failed, will use kernel index reorg method to backfill the index"
	LitErrCreateEngineFail  string = "[ddl-lightning] build lightning engine failed, will use kernel index reorg method to backfill the index"
	LitErrCreateContextFail string = "[ddl-lightning] build lightning worker context failed, will use kernel index reorg method to backfill the index"
	LitErrGetEngineFail     string = "[ddl-lightning] can not get cached engine info"
	LitErrGetStorageQuota   string = "[ddl-lightning] get storage quota error"
	LitErrGetSysLimitErr    string = "[ddl-lightning] get system open file limit error"
	LitErrCloseEngineErr    string = "[ddl-lightning] close engine error"
	LitErrCleanEngineErr    string = "[ddl-lightning] clean engine error"
	LitErrFlushEngineErr    string = "[ddl-lightning] flush engine data err"
	LitErrIngestDataErr     string = "[ddl-lightning] ingest data into storage error"
	LitErrRemoteDupExistErr string = "[ddl-lightning] remote duplicate index key exist"
	LitErrExceedConcurrency string = "[ddl-lightning] the concurrency is greater than lightning limit(tikv-importer.range-concurrency)"
	LitErrUpdateDiskStats   string = "[ddl-lightning] update disk usage error"
	LitWarnEnvInitFail      string = "[ddl-lightning] initialize environment failed"
	LitWarnConfigError      string = "[ddl-lightning] build config for backend failed"
	LitWarnGenMemLimit      string = "[ddl-lightning] generate memory max limitation"
	LitInfoEnvInitSucc      string = "[ddl-lightning] init global lightning backend environment finished"
	LitInfoSortDir          string = "[ddl-lightning] the lightning sorted dir"
	LitInfoCreateBackend    string = "[ddl-lightning] create one backend for an DDL job"
	LitInfoCloseBackend     string = "[ddl-lightning] close one backend for DDL job"
	LitInfoOpenEngine       string = "[ddl-lightning] open an engine for index reorg task"
	LitInfoCreateWrite      string = "[ddl-lightning] create one local Writer for Index reorg task"
	LitInfoCloseEngine      string = "[ddl-lightning] flush all writer and get closed engine"
	LitInfoRemoteDupCheck   string = "[ddl-lightning] start remote duplicate checking"
	LitInfoStartImport      string = "[ddl-lightning] start to import data"
	LitInfoSetMemLimit      string = "[ddl-lightning] set max memory limitation"
	LitInfoChgMemSetting    string = "[ddl-lightning] change memory setting for lightning"
	LitInfoInitMemSetting   string = "[ddl-lightning] initial memory setting for lightning"
	LitInfoUnsafeImport     string = "[ddl-lightning] do a partial import data into the storage"
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
