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

// Message const text
const (
	LitErrAllocMemFail      string = "lightning: Allocate memory failed"
	LitErrOutMaxMem         string = "lightning: Memory used up for lightning add index"
	LitErrUnknownMemType    string = "lightning: Unknown struct mem required for lightning add index"
	LitErrCreateDirFail     string = "lightning: Create lightning sort path error"
	LitErrStatDirFail       string = "lightning: Stat lightning sort path error"
	LitErrDeleteDirFail     string = "lightning: Delete lightning sort path error"
	LitErrCreateBackendFail string = "lightning: Build lightning backend failed, will use kernel index reorg method to backfill the index"
	LitErrGetBackendFail    string = "lightning: Can not get cached backend"
	LitErrCreateEngineFail  string = "lightning: Build lightning engine failed, will use kernel index reorg method to backfill the index"
	LitErrCreateContextFail string = "lightning: Build lightning worker context failed, will use kernel index reorg method to backfill the index"
	LitErrGetEngineFail     string = "lightning: Can not get cached engine info"
	LitErrGetStorageQuota   string = "lightning: Get storage quota error"
	LitErrGetSysLimitErr    string = "lightning: Get system open file limit error"
	LitErrCloseEngineErr    string = "lightning: Close engine error"
	LitErrCleanEngineErr    string = "lightning: Clean engine error"
	LitErrFlushEngineErr    string = "lightning: Flush engine data err"
	LitErrIngestDataErr     string = "lightning: Ingest data into TiKV error"
	LitErrRemoteDupCheckrr  string = "lightning: Remote duplicate check error"
	LitErrRemoteDupExistErr string = "lightning: Remote duplicate index key exist"
	LitErrDiskQuotaLess     string = "lightning: Specified disk quota is less than 100 GB disable the lightning"
	LitErrIncompatiblePiTR  string = "lightning: The storage config log-backup.enable should be false when the lightning backfill is enabled"
	LitWarnEnvInitFail      string = "lightning: Initialize environment failed"
	LitWarnBackendNOTExist  string = "lightning: Backend not exist"
	LitWarnConfigError      string = "lightning: Build config for backend failed"
	LitWarnGenMemLimit      string = "lightning: Generate memory max limitation"
	LitWarnExtentWorker     string = "lightning: Extend worker failed will use worker count number worker to keep doing backfill task"
	LitWarnDiskShortage     string = "lightning: Local disk storage shortage"
	LitInfoEnvInitSucc      string = "lightning: Init global lightning backend environment finished"
	LitInfoSortDir          string = "lightning: The lightning sorted dir"
	LitInfoCreateBackend    string = "lightning: Create one backend for an DDL job"
	LitInfoCloseBackend     string = "lightning: Close one backend for DDL job"
	LitInfoOpenEngine       string = "lightning: Open an engine for index reorg task"
	LitInfoCleanUpEngine    string = "lightning: CleanUp one engine for index reorg task"
	LitInfoCreateWrite      string = "lightning: Create one local Writer for Index reorg task"
	LitInfoCloseEngine      string = "lightning: Flush all writer and get closed engine"
	LitInfoDelEngine        string = "lightning: Delete one engine"
	LitInfoRemoteDuplCheck  string = "lightning: Start remote duplicate checking"
	LitInfoStartImport      string = "lightning: Start to import data"
	LitInfoSetMemLimit      string = "lightning: Set max memory limitation"
	LitInfoChgMemSetting    string = "lightning: Change memory setting for lightning"
	LitInfoInitMemSetting   string = "lightning: Initial memory setting for lightning"
	LitInfoEngineDelete     string = "lightning: Delete one engine from engine manager cache"
	LitInfoUnsafeImport     string = "lightning: Do a partial import data into TiKV"
	LitInfoDiskMaxLimit     string = "lightning: Local disk storage usage arrive up limited"
	LitInfoDiskQuotaChg     string = "lightning: Local disk storage quota changed"
)
