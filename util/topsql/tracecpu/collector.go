// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tracecpu

// TopSQLCollector uses to collect SQL stats.
// TODO: add a collector to collect and store the SQL stats.
type TopSQLCollector interface {
	// Collect uses to collect the SQL execution information.
	// ts is a Unix time, unit is second.
	Collect(ts int64, stats []TopSQLRecord)
	RegisterSQL(sqlDigest, normalizedSQL string)
	RegisterPlan(planDigest string, normalizedPlan string)
}

// TopSQLRecord contains the SQL meta and execution information.
type TopSQLRecord struct {
	SQLDigest  string
	PlanDigest string
	CPUTimeMs  uint32
}
