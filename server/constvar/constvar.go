package constvar

const (
	DBNameParam             = "db"
	HexKeyParam             = "hexKey"
	IndexNameParam          = "index"
	HandleParam             = "handle"
	RegionIDParam           = "regionID"
	StartTSParam            = "startTS"
	TableNameParam          = "table"
	TableIDParam            = "tableID"
	ColumnIDParam           = "colID"
	ColumnTpParam           = "colTp"
	ColumnFlagParam         = "colFlag"
	ColumnLenParam          = "colLen"
	RowBinParam             = "rowBin"
	SnapshotParam           = "snapshot"
	FileNameParam           = "filename"
	DumpPartitionStatsParam = "dumpPartitionStats"
	BeginParam              = "begin"
	EndParam                = "end"
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
	TableIDQuery   = "table_id"
	LimitQuery     = "limit"
	JobIDQuery     = "start_job_id"
	OperationQuery = "op"
	SecondsQuery   = "seconds"
)

const (
	HeaderContentType = "Content-Type"
	ContentTypeJSON   = "application/json"
)
