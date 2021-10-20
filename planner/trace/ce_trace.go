package trace

type CETraceRecord struct {
	TableID  int64
	Type     string
	Expr     string
	RowCount uint64
}

