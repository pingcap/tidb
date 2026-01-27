package model

// MaterializedViewRefreshMode is the refresh mode for a materialized view (TiDB demo).
type MaterializedViewRefreshMode string

const (
	MaterializedViewRefreshModeFast     MaterializedViewRefreshMode = "FAST"
	MaterializedViewRefreshModeComplete MaterializedViewRefreshMode = "COMPLETE"
)

// MaterializedViewInfo is the persisted metadata for a materialized view table (TiDB demo).
type MaterializedViewInfo struct {
	BaseTableID int64 `json:"base_table_id"`
	LogTableID  int64 `json:"log_table_id"`

	DefinitionSQL string                      `json:"definition_sql,omitempty"`
	RefreshMode   MaterializedViewRefreshMode `json:"refresh_mode,omitempty"`

	RefreshIntervalSeconds int64 `json:"refresh_interval_seconds"`

	GroupByColumnIDs []int64 `json:"group_by_column_ids,omitempty"`
}

func (i *MaterializedViewInfo) Clone() *MaterializedViewInfo {
	if i == nil {
		return nil
	}
	ni := *i
	if i.GroupByColumnIDs != nil {
		ni.GroupByColumnIDs = make([]int64, len(i.GroupByColumnIDs))
		copy(ni.GroupByColumnIDs, i.GroupByColumnIDs)
	}
	return &ni
}

// MaterializedViewLogInfo is the persisted metadata for a materialized view log table (TiDB demo).
type MaterializedViewLogInfo struct {
	BaseTableID int64   `json:"base_table_id"`
	ColumnIDs   []int64 `json:"column_ids"`
}

func (i *MaterializedViewLogInfo) Clone() *MaterializedViewLogInfo {
	if i == nil {
		return nil
	}
	ni := *i
	if i.ColumnIDs != nil {
		ni.ColumnIDs = make([]int64, len(i.ColumnIDs))
		copy(ni.ColumnIDs, i.ColumnIDs)
	}
	return &ni
}
