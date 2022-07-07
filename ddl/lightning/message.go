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
	LitErrAllocMemFail      string = "Lightning: Allocate memory failed"
	LitErrOutMaxMem         string = "Lightning: Memory used up for Lightning add index"
	LitErrUnknownMemType    string = "Lightning: Unknown struct mem required for Lightning add index"
	LitErrCreateDirFail     string = "Lightning: Create lightning sort path error"
	LitErrStatDirFail       string = "Lightning: Stat lightning sort path error"
	LitErrDeleteDirFail     string = "Lightning: Delete lightning sort path error"
	LitErrCreateBackendFail string = "Lightning: Build lightning backend failed, will use kernel index reorg method to backfill the index"
	LitErrGetBackendFail    string = "Lightning: Can not get cached backend"
	LitErrCreateEngineFail  string = "Lightning: Build lightning engine failed, will use kernel index reorg method to backfill the index"
	LitErrCreateContextFail string = "Lightning: Build lightning worker context failed, will use kernel index reorg method to backfill the index"
	LitErrGetEngineFail     string = "Lightning: Can not get catched engininfo"
	LitErrGetStorageQuota   string = "Lightning: Get storage quota error"
	LitErrGetSysLimitErr    string = "Lightning: Get system open file limit error"
	LitErrCloseEngineErr    string = "Lightning: Close engine error"
	LitErrCleanEngineErr    string = "Lightning: Clean engine error"
	LitErrFlushEngineErr    string = "Lightning: Flush engine data err"
	LitErrIngestDataErr     string = "Lightning: Ingest data into TiKV error"
	LitErrRemoteDupCheckrr  string = "Lightning: Remote duplicate check error"
	LitErrRemoteDupExistErr string = "Lightning: Remote duplicate index key exist"
	LitErrDiskQuotaLess     string = "Lightning: Specified disk quota is less than 100 GB disable the Lightning"
	LitWarnEnvInitFail      string = "Lightning: Initialize environment failed"
	LitWarnBackendNOTExist  string = "Lightning: Backend not exist"
	LitWarnConfigError      string = "Lightning: Build config for backend failed"
	LitWarnGenMemLimit      string = "Lightning: Generate memory max limitation"
	LitWarnExtentWorker     string = "Lightning: Extend worker failed will use worker count number worker to keep doing backfill task"
	LitWarnDiskShortage     string = "Lightning: Local disk storage shortage"
	LitInfoEnvInitSucc      string = "Lightning: Init global lightning backend environment finished"
	LitInfoSortDir          string = "Lightning: The lightning sorted dir"
	LitInfoCreateBackend    string = "Lightning: Create one backend for an DDL job"
	LitInfoCloseBackend     string = "Lightning: Close one backend for DDL job"
	LitInfoOpenEngine       string = "Lightning: Open an engine for index reorg task"
	LitInfoCleanUpEngine    string = "Lightning: CleanUp one engine for index reorg task"
	LitInfoCreateWrite      string = "Lightning: Create one local Writer for Index reorg task"
	LitInfoCloseEngine      string = "Lightning: Flush all writer and get closed engine"
	LitInfoDelEngine        string = "Lightning: Delete one engine"
	LitInfoRemoteDuplCheck  string = "Lightning: Start remote duplicate checking"
	LitInfoStartImport      string = "Lightning: Start to import data"
	LitInfoSetMemLimit      string = "Lightning: Set max memory limitation"
	LitInfoChgMemSetting    string = "Lightning: Change memory setting for lightning"
	LitInfoInitMemSetting   string = "Lightning: Initial memory setting for lightning,"
	LitInfoEngineDelete     string = "Lightning: Delete one engine from engine manager cache"
	LitInfoUnsafeImport     string = "Lightning: Do a partial import data into TiKV"
	LitInfoDiskMaxLimit     string = "Lightning: Local disk storage usage arrive up limited"
	LitInfoDiskQuotaChg     string = "Lightning: Local disk storage quota changed"
)
