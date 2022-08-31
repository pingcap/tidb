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

package lightning

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Message const text
const (
	LitErrAllocMemFail      string = "lightning: allocate memory failed"
	LitErrOutMaxMem         string = "lightning: memory used up for lightning add index"
	LitErrUnknownMemType    string = "lightning: unknown struct mem required for lightning add index"
	LitErrCreateDirFail     string = "lightning: create lightning sort path error"
	LitErrStatDirFail       string = "lightning: stat lightning sort path error"
	LitErrDeleteDirFail     string = "lightning: delete lightning sort path error"
	LitErrCreateBackendFail string = "lightning: build lightning backend failed, will use kernel index reorg method to backfill the index"
	LitErrGetBackendFail    string = "lightning: can not get cached backend"
	LitErrCreateEngineFail  string = "lightning: build lightning engine failed, will use kernel index reorg method to backfill the index"
	LitErrCreateContextFail string = "lightning: build lightning worker context failed, will use kernel index reorg method to backfill the index"
	LitErrGetEngineFail     string = "lightning: can not get cached engine info"
	LitErrGetStorageQuota   string = "lightning: get storage quota error"
	LitErrGetSysLimitErr    string = "lightning: get system open file limit error"
	LitErrCloseEngineErr    string = "lightning: close engine error"
	LitErrCleanEngineErr    string = "lightning: clean engine error"
	LitErrFlushEngineErr    string = "lightning: flush engine data err"
	LitErrIngestDataErr     string = "lightning: ingest data into storage error"
	LitErrRemoteDupCheckErr string = "lightning: remote duplicate check error"
	LitErrRemoteDupExistErr string = "lightning: remote duplicate index key exist"
	LitErrDiskQuotaLess     string = "lightning: specified disk quota is less than 100 GB disable the lightning"
	LitErrIncompatiblePiTR  string = "lightning: the storage config log-backup.enable should be false when the lightning backfill is enabled"
	LitErrExceedConcurrency string = "lightning: the concurrency is greater than lightning limit(tikv-importer.range-concurrency)"
	LitErrUpdateDiskStats   string = "lightning: update disk usage error"
	LitWarnEnvInitFail      string = "lightning: initialize environment failed"
	LitWarnBackendNotExist  string = "lightning: backend not exist"
	LitWarnConfigError      string = "lightning: build config for backend failed"
	LitWarnGenMemLimit      string = "lightning: generate memory max limitation"
	LitWarnExtentWorker     string = "lightning: extend worker failed will use worker count number worker to keep doing backfill task"
	LitWarnDiskShortage     string = "lightning: local disk storage shortage"
	LitInfoEnvInitSucc      string = "lightning: init global lightning backend environment finished"
	LitInfoSortDir          string = "lightning: the lightning sorted dir"
	LitInfoCreateBackend    string = "lightning: create one backend for an DDL job"
	LitInfoCloseBackend     string = "lightning: close one backend for DDL job"
	LitInfoOpenEngine       string = "lightning: open an engine for index reorg task"
	LitInfoCleanUpEngine    string = "lightning: cleanUp one engine for index reorg task"
	LitInfoCreateWrite      string = "lightning: create one local Writer for Index reorg task"
	LitInfoCloseEngine      string = "lightning: flush all writer and get closed engine"
	LitInfoDelEngine        string = "lightning: delete one engine"
	LitInfoRemoteDupCheck   string = "lightning: start remote duplicate checking"
	LitInfoStartImport      string = "lightning: start to import data"
	LitInfoSetMemLimit      string = "lightning: set max memory limitation"
	LitInfoChgMemSetting    string = "lightning: change memory setting for lightning"
	LitInfoInitMemSetting   string = "lightning: initial memory setting for lightning"
	LitInfoEngineDelete     string = "lightning: delete one engine from engine manager cache"
	LitInfoUnsafeImport     string = "lightning: do a partial import data into the storage"
	LitInfoDiskMaxLimit     string = "lightning: local disk storage usage arrive up limited"
	LitInfoDiskQuotaChg     string = "lightning: local disk storage quota changed"
)

func logAllocMemFailedBackend(memRoot MemRoot, jobID int64) error {
	logutil.BgLogger().Warn(LitErrAllocMemFail, zap.Int64("job ID", jobID),
		zap.Int64("current memory usage", memRoot.CurrentUsage()),
		zap.Int64("max memory quota", memRoot.MaxMemoryQuota()))
	return errors.New(LitErrOutMaxMem)
}

func logAllocMemFailedEngine(memRoot MemRoot, jobID, idxID int64) error {
	logutil.BgLogger().Warn(LitErrAllocMemFail, zap.Int64("job ID", jobID),
		zap.Int64("index ID", idxID),
		zap.Int64("current memory usage", memRoot.CurrentUsage()),
		zap.Int64("max memory quota", memRoot.MaxMemoryQuota()))
	return errors.New(LitErrOutMaxMem)
}
