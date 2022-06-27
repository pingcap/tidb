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

const (
	// Error messages
	LERR_ALLOC_MEM_FAILED      string = "Lightning: Allocate memory failed"
	LERR_OUT_OF_MAX_MEM        string = "Lightning: Memory used up for Lightning add index"
	LERR_UNKNOW_MEM_TYPE       string = "Lightning: Unknown struct mem required for Lightning add index"
	LERR_CREATE_DIR_FAILED     string = "Lightning: Create lightning sort path error"
	LERR_CREATE_BACKEND_FAILED string = "Lightning: Build lightning backend failed, will use kernel index reorg method to backfill the index"
	LERR_GET_BACKEND_FAILED    string = "Lightning: Can not get cached backend"
	LERR_CREATE_ENGINE_FAILED  string = "Lightning: Build lightning engine failed, will use kernel index reorg method to backfill the index"
	LERR_CREATE_CONTEX_FAILED  string = "Lightning: Build lightning worker context failed, will use kernel index reorg method to backfill the index"
	LERR_GET_ENGINE_FAILED     string = "Lightning: Can not get catched engininfo"
	LERR_GET_STORAGE_QUOTA     string = "Lightning: Get storage quota error"
	LERR_GET_SYS_LIMIT_ERR     string = "Lightning: Get system open file limit error"
	LERR_CLOSE_ENGINE_ERR      string = "Lightning: Close engine error"
	LERR_FLUSH_ENGINE_ERR      string = "Lightning: Flush engine data err"
	LERR_INGEST_DATA_ERR       string = "Lightning: Ingest data into TiKV error"
	LERR_LOCAL_DUP_CHECK_ERR   string = "Lightning: Locale duplicate check error"
	LERR_LOCAL_DUP_EXIST_ERR   string = "Lightning: Locale duplicate index key exist"
	LERR_REMOTE_DUP_CHECK_ERR  string = "Lightning: Remote duplicate check error"
	LERR_REMOTE_DUP_EXIST_ERR  string = "Lightning: Remote duplicate index key exist"
	LERR_DISK_QUOTA_SMALL      string = "Lightning: Specified disk quota is less than 100 GB disable the Lightning"
	// Warning messages
	LWAR_ENV_INIT_FAILD        string = "Lightning: Initialize environment failed"
	LWAR_BACKEND_NOT_EXIST     string = "Lightning: Backend not exist"
	LWAR_CONFIG_ERROR          string = "Lightning: Build config for backend failed"
	LWAR_GEN_MEM_LIMIT         string = "Lightning: Generate memory max limitation"
	LWAR_EXTENT_WORKER         string = "Lightning: Extend worker failed will use worker count number worker to keep doing backfill task "
	// Infomation messages
	LINFO_ENV_INIT_SUCC        string = "Lightning: Init global lightning backend environment finished"
	LINFO_SORTED_DIR           string = "Lightning: The lightning sorted dir"
	LINFO_CREATE_BACKEND       string = "Lightning: Create one backend for an DDL job"
	LINFO_CLOSE_BACKEND        string = "Lightning: Close one backend for DDL job"
	LINFO_OPEN_ENGINE          string = "Lightning: Open an engine for index reorg task"
	LINFO_CLEANUP_ENGINE       string = "Lightning: CleanUp one engine for index reorg task"
	LINFO_CREATE_WRITER        string = "Lightning: Create one local Writer for Index reorg task"
	LINFO_CLOSE_ENGINE         string = "Lightning: Flush all writer and get closed engine"
	LINFO_DEL_ENGINE       	   string = "Lightning: Delete one engine"
	LINFO_LOCAL_DUPL_CHECK     string = "Lightning: Start Local duplicate checking"
	LINFO_REMOTE_DUPL_CHECK    string = "Lightning: Start remote duplicate checking"
	LINFO_START_TO_IMPORT      string = "Lightning: Start to import data"
	LINFO_SET_MEM_LIMIT        string = "Lightning: Set max memory limitation"
	LINFO_CHG_MEM_SETTING      string = "Lightning: Change memory setting for lightning"
	LINFO_INIT_MEM_SETTING     string = "Lightning: Initial memory setting for lightning,"
	LINFO_ENGINE_DELETE        string = "Lightning: Delete one engine from engine manager cache,"
	LINFO_UNSAFE_IMPORT        string = "Lightning: Do a partial import data into TiKV,"

)
