// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"errors"
	"github.com/pingcap/tidb/pkg/resourcegroup/ruv3"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/metrics"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func requireStatementRUReportConservation(t *testing.T, finalized statementRUFinalizedSnapshot) {
	t.Helper()
	require.NotNil(t, finalized.report)
	var total ruv3.StmtUnits
	var engineRU [statementRUEngineCount]float64
	for engine, operators := range finalized.report.units {
		for _, units := range operators {
			total = total.Add(units)
			result, ok := ruv3.Calculate(units, currentStatementRUWeights())
			require.True(t, ok)
			engineRU[engine] += result.TotalRU
		}
	}
	require.Equal(t, finalized.units, total)
	require.InDelta(t, finalized.engineRU.TiDB, engineRU[statementRUTiDB], 1e-9)
	require.InDelta(t, finalized.engineRU.TiKV, engineRU[statementRUTiKV], 1e-9)
	require.InDelta(t, finalized.result.TotalRU, finalized.engineRU.TiDB+finalized.engineRU.TiKV, 1e-9)
}

func TestStatementRUReportingModes(t *testing.T) {
	defer config.RestoreFunc()()
	for _, mode := range []string{config.RUReportModeResult, config.RUReportModeFull} {
		t.Run(mode, func(t *testing.T) {
			config.UpdateGlobal(func(c *config.Config) { c.RUV2.ReportMode = mode })
			fixture := newStatementRUSimpleSelectFixture(t)
			sc := fixture.stmt.Ctx.GetSessionVars().StmtCtx
			flat := sc.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
			sc.SetFlatPlan(nil)
			installStatementRUOwner(fixture.stmt)
			fixture.owner = fixture.stmt.statementRUOwner
			sc.SetFlatPlan(flat)
			require.Equal(t, mode == config.RUReportModeFull, fixture.owner.calculationSetup.fullReport)
			// Changing the global mode cannot split the installed owner's lifecycle.
			config.UpdateGlobal(func(c *config.Config) {
				if mode == config.RUReportModeFull {
					c.RUV2.ReportMode = config.RUReportModeResult
				} else {
					c.RUV2.ReportMode = config.RUReportModeFull
				}
			})
			finalized, ok := calculateStatementRU(flat, sc.RuntimeStatsColl, fixture.stmt.Ctx.GetSessionVars().RUV2Metrics,
				statementRUWriteSnapshot{}, fixture.owner.calculationSetup, true)
			require.True(t, ok)
			require.Equal(t, statementRUEngineResult{TiDB: 1 + float64(len(statementRUSimpleSelectSQLForTest)), TiKV: 31}, finalized.engineRU)
			if mode == config.RUReportModeFull {
				requireStatementRUReportConservation(t, finalized)
				require.Equal(t, float64(1), finalized.report.units[statementRUTiKV][statementRURangeScan].OperatorNum)
				require.Equal(t, float64(10), finalized.report.units[statementRUTiKV][statementRUReader].ScanBytes)
			} else {
				require.Nil(t, finalized.report)
			}

			diagnostics := prometheus.NewRegistry()
			diagnostics.MustRegister(metrics.RUV3Unit, metrics.RUV3Statements)
			totalBefore := testutil.ToFloat64(metrics.RUV3Total)
			sqlTypeBefore := testutil.ToFloat64(metrics.RUV3BySQLType.WithLabelValues("select"))
			tidbBefore := testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues("tidb"))
			tikvBefore := testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues("tikv"))
			success := metrics.RUV3Statements.WithLabelValues("success", "incomplete")
			successBefore := testutil.ToFloat64(success)
			before, err := diagnostics.Gather()
			require.NoError(t, err)
			reporter := &statementRUReporterForTest{}
			fixture.stmt.Ctx = &statementRUReportingContextForTest{Context: fixture.stmt.Ctx.(*mock.Context), reporter: reporter}
			sc.ResourceGroupName = "ru-test"
			observed := 0
			observeStatementRUCalibrationForTest(t, func(statementRUCalibrationSnapshot) { observed++ })
			fixture.stmt.recordStatementRURootEOF()
			fixture.stmt.RecordStatementRUFinalOutcome(true)
			fixture.stmt.finishStatementRU(nil)
			fixture.stmt.finishStatementRU(nil)
			require.InDelta(t, finalized.engineRU.TiDB, testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues("tidb"))-tidbBefore, 1e-9)
			require.InDelta(t, finalized.engineRU.TiKV, testutil.ToFloat64(metrics.RUV3ByEngine.WithLabelValues("tikv"))-tikvBefore, 1e-9)
			require.InDelta(t, finalized.result.TotalRU, testutil.ToFloat64(metrics.RUV3Total)-totalBefore, 1e-9)
			require.InDelta(t, finalized.result.TotalRU, testutil.ToFloat64(metrics.RUV3BySQLType.WithLabelValues("select"))-sqlTypeBefore, 1e-9)
			require.Equal(t, "ru-test", reporter.group)
			require.Equal(t, [3]float64{finalized.engineRU.TiKV, finalized.engineRU.TiDB, 0}, reporter.ru)
			require.Equal(t, 1, reporter.calls)
			if mode == config.RUReportModeFull {
				require.Equal(t, 1, observed)
				require.Equal(t, successBefore+1, testutil.ToFloat64(success))
			} else {
				require.Zero(t, observed)
				after, err := diagnostics.Gather()
				require.NoError(t, err)
				require.Equal(t, before, after)
			}
		})
	}
}

func TestStatementRUFullReportFreeze(t *testing.T) {
	t.Run("skip zero-valued series", func(t *testing.T) {
		original := metrics.RUV3Unit
		metrics.RUV3Unit = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tidb_ruv3_unit_total", Help: "RUv3 units under test.",
		}, []string{"engine", "opclass", "unit"})
		t.Cleanup(func() { metrics.RUV3Unit = original })
		registry := prometheus.NewRegistry()
		registry.MustRegister(metrics.RUV3Unit)
		report := &statementRUFullReport{}
		report.add(statementRUTiDB, statementRUProjection, ruv3.StmtUnits{CPUWork: 3})
		report.add(statementRUTiKV, statementRUReader, ruv3.StmtUnits{})
		finalized := statementRUFinalizedSnapshot{report: report, calibrationState: statementRUCalibrationIncomplete}
		publishStatementRUFullMetrics(finalized)
		families, err := registry.Gather()
		require.NoError(t, err)
		require.Len(t, families, 1)
		require.Len(t, families[0].Metric, 1)
		metric := families[0].Metric[0]
		require.Equal(t, float64(3), metric.GetCounter().GetValue())
		labels := make(map[string]string)
		for _, label := range metric.Label {
			labels[label.GetName()] = label.GetValue()
		}
		require.Equal(t, map[string]string{"engine": "tidb", "opclass": "projection", "unit": "cpu_work"}, labels)
		// A later zero contribution neither creates series nor removes accumulated work.
		report.units[statementRUTiDB][statementRUProjection] = ruv3.StmtUnits{}
		publishStatementRUFullMetrics(finalized)
		after, err := registry.Gather()
		require.NoError(t, err)
		require.Equal(t, families, after)
	})
	calculator := newStatementRUCalculator(statementRUCalculationSetup{fullReport: true, frontendCompileBytes: 11})
	local := ruv3.StmtUnits{CPUWork: 2, HashStateRows: 3, JoinOutputRows: 5, OperatorNum: 7}
	remote := ruv3.StmtUnits{CPUWork: 13, HashStateRows: 17, OperatorNum: 19, ScanBytes: 23, NetBytes: 29}
	for engine, units := range []ruv3.StmtUnits{local, remote} {
		calculator.units = calculator.units.Add(units)
		calculator.recordOperatorUnits(statementRUEngine(engine), units)
		calculator.report.addOperator(statementRUEngine(engine), statementRUHashAgg, units)
	}
	calculator.units.WriteStatement, calculator.units.WriteKeys, calculator.units.WriteBytes = 1, 31, 37
	first, ok := calculator.finalize()
	require.True(t, ok)
	second, ok := calculator.finalize()
	require.True(t, ok)
	require.Equal(t, first, second)
	requireStatementRUReportConservation(t, first)
	require.Equal(t, float64(1), first.report.units[statementRUTiDB][statementRUWrite].WriteStatement)
	require.Equal(t, float64(31), first.report.units[statementRUTiKV][statementRUKVWrite].WriteKeys)
	require.Equal(t, float64(37), first.report.units[statementRUTiKV][statementRUKVWrite].WriteBytes)
	calculator.report.add(statementRUTiDB, statementRUHashAgg, local)
	require.Equal(t, first, second)
	requireStatementRUReportConservation(t, first)
	checks := []struct {
		engine, operator, unit string
		want, before           float64
	}{
		{"tidb", "hash_agg", metrics.LblRUV3UnitCPUWork, 2, 0},
		{"tikv", "hash_agg", metrics.LblRUV3UnitScanBytes, 23, 0},
		{"tikv", "hash_agg", metrics.LblRUV3UnitNetBytes, 29, 0},
		{"tidb", "sql_frontend", metrics.LblRUV3UnitFrontendCompileBytes, 11, 0},
		{"tidb", "hash_agg", metrics.LblRUV3UnitHashStateRows, 3, 0},
		{"tidb", "hash_agg", metrics.LblRUV3UnitJoinOutputRows, 5, 0},
		{"tidb", "write", metrics.LblRUV3UnitWriteStatement, 1, 0},
		{"tidb", "hash_agg", metrics.LblRUV3UnitOperatorNum, 7, 0},
		{"tikv", "kv_write", metrics.LblRUV3UnitWriteKeys, 31, 0},
		{"tikv", "kv_write", metrics.LblRUV3UnitWriteBytes, 37, 0},
	}
	for i := range checks {
		c := &checks[i]
		c.before = testutil.ToFloat64(metrics.RUV3Unit.WithLabelValues(c.engine, c.operator, c.unit))
	}
	publishStatementRUMetricsSafely(first)
	for _, c := range checks {
		require.InDelta(t, c.want, testutil.ToFloat64(metrics.RUV3Unit.WithLabelValues(c.engine, c.operator, c.unit))-c.before, 1e-9, c.unit)
	}
	// The same input has exactly the same result without the full report.
	calculator.report = nil
	resultOnly, ok := calculator.finalize()
	require.True(t, ok)
	require.Equal(t, first.result, resultOnly.result)
	require.Equal(t, first.engineRU, resultOnly.engineRU)
	require.Nil(t, resultOnly.report)
}

func TestStatementRUReportingFailures(t *testing.T) {
	for _, full := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			status  string
			reason  statementRUFailureReason
			prepare func(statementRUSimpleSelectFixture)
			abort   bool
			err     error
		}{
			{name: "statement error", status: "failed", reason: statementRUStatementError, err: errors.New("statement failed")},
			{name: "abort", status: "failed", reason: statementRUStatementError, abort: true},
			{name: "early close", status: "failed", reason: statementRUNotFinished, prepare: func(f statementRUSimpleSelectFixture) { f.owner.rootEOF.Store(false) }},
			{name: "invalid plan", status: "failed", reason: statementRUInvalid, prepare: func(f statementRUSimpleSelectFixture) { f.stmt.Plan = nil }},
			{name: "unsupported plan", status: "skipped", reason: statementRUUnsupported, prepare: func(f statementRUSimpleSelectFixture) {
				flat := f.stmt.Ctx.GetSessionVars().StmtCtx.GetFlatPlan().(*plannercore.FlatPhysicalPlan)
				flat.Main[0].Origin = &plannercore.Simple{}
				f.stmt.Plan = flat.Main[0].Origin
			}},
			{name: "ineligible", status: "skipped", reason: statementRUIneligible, prepare: func(f statementRUSimpleSelectFixture) { f.stmt.Ctx.GetSessionVars().InRestrictedSQL = true }},
		} {
			t.Run(tc.name+map[bool]string{false: "/result", true: "/full"}[full], func(t *testing.T) {
				fixture := newStatementRUSimpleSelectFixture(t)
				fixture.owner.calculationSetup.fullReport = full
				if tc.prepare != nil {
					tc.prepare(fixture)
				}
				counter := metrics.RUV3Statements.WithLabelValues(tc.status, string(tc.reason))
				before := testutil.ToFloat64(counter)
				billable := prometheus.NewRegistry()
				billable.MustRegister(metrics.RUV3ByEngine, metrics.RUV3Total, metrics.RUV3Unit)
				beforeUnits, err := billable.Gather()
				require.NoError(t, err)
				fixture.stmt.RecordStatementRUFinalOutcome(true)
				if tc.abort {
					fixture.stmt.abortStatementRU()
				}
				fixture.stmt.finishStatementRU(tc.err)
				fixture.stmt.finishStatementRU(nil)
				want := before
				if full {
					want++
				}
				require.Equal(t, want, testutil.ToFloat64(counter))
				afterUnits, err := billable.Gather()
				require.NoError(t, err)
				require.Equal(t, beforeUnits, afterUnits)
			})
		}
	}
}

type statementRUReporterForTest struct {
	resourcegroup.ConsumptionReporter
	group string
	ru    [3]float64
	calls int
}

func (r *statementRUReporterForTest) ReportRUV2Consumption(group string, tikv, tidb, tiflash float64) {
	r.group, r.ru = group, [3]float64{tikv, tidb, tiflash}
	r.calls++
}

type statementRUReportingContextForTest struct {
	*mock.Context
	reporter *statementRUReporterForTest
}

func (c *statementRUReportingContextForTest) GetDistSQLCtx() *distsqlctx.DistSQLContext {
	ctx := c.Context.GetDistSQLCtx()
	ctx.RUConsumptionReporter = c.reporter
	ctx.ResourceGroupName = c.GetSessionVars().StmtCtx.ResourceGroupName
	return ctx
}

func (calculator *statementRUCalculator) recordOperatorUnits(engine statementRUEngine, units ruv3.StmtUnits) {
	compute := &calculator.compute[engine]
	compute.cpuWork += units.CPUWork
	compute.hashStateRows += units.HashStateRows
	compute.operatorNum += units.OperatorNum
}
