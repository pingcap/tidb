package model

// MaterializedViewInfo is stored in TableInfo for a materialized view table.
type MaterializedViewInfo struct {
	// BaseTableID is the table ID of the base table.
	BaseTableID int64 `json:"base_table_id"`

	// MLogID is the table ID of the materialized view log used by this MV.
	MLogID int64 `json:"mlog_id"`

	// SQLContent is the SELECT statement in CREATE MATERIALIZED VIEW.
	SQLContent string `json:"sql_content"`
}

// MaterializedViewLogInfo is stored in TableInfo for a materialized view log table.
type MaterializedViewLogInfo struct {
	// BaseTableID is the table ID of the base table.
	BaseTableID int64 `json:"base_table_id"`

	// Columns is the base table column list recorded in the log (user-specified columns).
	Columns []string `json:"columns"`
}
