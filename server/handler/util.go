package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/terror"
)

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

const (
	HeaderContentType = "Content-Type"
	ContentTypeJSON   = "application/json"
)

func WriteError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	_, err = w.Write([]byte(err.Error()))
	terror.Log(errors.Trace(err))
}

func WriteData(w http.ResponseWriter, data interface{}) {
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

// ExtractTableAndPartitionName extracts table name and partition name from a string.
func ExtractTableAndPartitionName(str string) (string, string) {
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
