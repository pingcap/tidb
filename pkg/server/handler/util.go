// Copyright 2023 PingCAP, Inc.
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

package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

//revive:disable
const (
	DBName             = "db"
	HexKey             = "hexKey"
	IndexName          = "index"
	Handle             = "handle"
	RegionID           = "regionID"
	StartTS            = "startTS"
	TableName          = "table"
	TableID            = "tableID"
	ColumnID           = "colID"
	ColumnTp           = "colTp"
	ColumnFlag         = "colFlag"
	ColumnLen          = "colLen"
	RowBin             = "rowBin"
	Snapshot           = "snapshot"
	FileName           = "filename"
	DumpPartitionStats = "dumpPartitionStats"
	Begin              = "begin"
	End                = "end"
)

// For extract task handler
const (
	Type   = "type"
	IsDump = "isDump"

	// For extract plan task handler
	IsSkipStats   = "isSkipStats"
	IsHistoryView = "isHistoryView"
)

// For query string
const (
	TableIDQuery = "table_id"
	Limit        = "limit"
	JobID        = "start_job_id"
	Operation    = "op"
	Seconds      = "seconds"
)

const (
	HeaderContentType = "Content-Type"
	ContentTypeJSON   = "application/json"
)

//revive:enable

// WriteError writes error to response.
func WriteError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	_, err = w.Write([]byte(err.Error()))
	terror.Log(errors.Trace(err))
}

// WriteData writes data to response.
func WriteData(w http.ResponseWriter, data any) {
	js, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		WriteError(w, err)
		return
	}
	// write response
	w.Header().Set(HeaderContentType, ContentTypeJSON)
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	terror.Log(errors.Trace(err))
}

// ExtractTableAndPartitionName extracts table name and partition name from this "table(partition)":
func ExtractTableAndPartitionName(str string) (table, partition string) {
	// extract table name and partition name from this "table(partition)":
	// A sane person would not let the the table name or partition name contain '('.
	start := strings.IndexByte(str, '(')
	if start == -1 {
		return str, ""
	}
	end := strings.IndexByte(str, ')')
	if end == -1 {
		return str, ""
	}
	return str[:start], str[start+1 : end]
}
