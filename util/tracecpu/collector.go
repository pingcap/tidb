package tracecpu

// SQLStatsCollector uses to collect SQL stats.
// TODO: add a collector.
type SQLStatsCollector interface {
	Collect(ts int64, stats []SQLStats)
	RegisterSQL(sqlDigest, normalizedSQL string)
	RegisterPlan(planDigest string, normalizedPlan string)
}

// SQLStats indicate the SQL stats.
type SQLStats struct {
	SQLDigest  string
	PlanDigest string
	CPUTimeMs  uint32
}
