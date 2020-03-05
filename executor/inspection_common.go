// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	"context"
	"sort"

	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

type inspectionRuleRetriever struct {
	dummyCloser
	retrieved bool
	extractor *plannercore.InspectionRuleTableExtractor
}

const (
	inspectionRuleTypeInspection string = "inspection"
	inspectionRuleTypeSummary    string = "summary"
)

func (e *inspectionRuleRetriever) retrieve(ctx context.Context, sctx sessionctx.Context) ([][]types.Datum, error) {
	if e.retrieved || e.extractor.SkipRequest {
		return nil, nil
	}
	e.retrieved = true

	tps := inspectionFilter{set: e.extractor.Types}
	var finalRows [][]types.Datum

	// Select inspection rules
	if tps.enable(inspectionRuleTypeInspection) {
		for _, r := range inspectionRules {
			finalRows = append(finalRows, types.MakeDatums(
				r.name(),
				inspectionRuleTypeInspection,
				// TODO: add rule explanation
				"",
			))
		}
	}
	// Select summary rules
	if tps.enable(inspectionRuleTypeSummary) {
		// Get ordered key of map inspectionSummaryRules
		summaryRules := make([]string, 0)
		for rule := range inspectionSummaryRules {
			summaryRules = append(summaryRules, rule)
		}
		sort.Strings(summaryRules)

		for _, rule := range summaryRules {
			finalRows = append(finalRows, types.MakeDatums(
				rule,
				inspectionRuleTypeSummary,
				// TODO: add rule explanation
				"",
			))
		}
	}
	return finalRows, nil
}
