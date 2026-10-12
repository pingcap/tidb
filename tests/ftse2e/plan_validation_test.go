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

// Package ftse2e tests the real TiDB -> TiFlash Boolean MATCH execution path.
// It is opt-in because it requires a running TiUP cluster with a TiFlash node.
package ftse2e

import (
	"fmt"
	"strings"
	"testing"
)

// Both brief EXPLAIN and statement-summary plans put the operator and task
// before the operator info. Read those columns rather than searching predicates
// for words such as "root" or "selection". Unrelated residual filters are valid.
func matchPlanOperator(line string) (operator, task string) {
	fields := strings.Fields(strings.ToLower(line))
	if len(fields) == 0 {
		return "", ""
	}
	operator = strings.TrimLeft(fields[0], "│├└─")
	for _, field := range fields[1:min(len(fields), 4)] {
		if field == "root" || strings.HasPrefix(field, "cop[") || strings.HasPrefix(field, "batchcop[") || strings.HasPrefix(field, "mpp[") {
			task = field
			break
		}
	}
	return operator, task
}

func validateMatchPlan(text string, native, decodedSummary bool) error {
	text = strings.ToLower(text)
	// The legacy statement-summary codec records only root/store type and
	// decodes MPP tasks as cop[tiflash]. Require the MPP TableReader/ExchangeSender
	// envelope before accepting these labels; ordinary EXPLAIN remains MPP-only.
	summaryMPP := false
	if decodedSummary {
		reader, sender := false, false
		for _, line := range strings.Split(text, "\n") {
			operator, task := matchPlanOperator(line)
			if strings.HasPrefix(operator, "tablereader") && task == "root" && strings.Contains(line, "mppversion:") && strings.Contains(line, "data:exchangesender") {
				reader = true
			}
			if strings.HasPrefix(operator, "exchangesender") && task == "cop[tiflash]" {
				sender = true
			}
		}
		summaryMPP = reader && sender
	}
	isTiFlash := func(task string) bool {
		return task == "mpp[tiflash]" || (summaryMPP && task == "cop[tiflash]")
	}
	scan, match := false, false
	for _, line := range strings.Split(text, "\n") {
		operator, task := matchPlanOperator(line)
		if task == "cop[tici]" || task == "batchcop[tici]" || task == "mpp[tici]" {
			return fmt.Errorf("unexpected TiCI plan")
		}
		if isTiFlash(task) && (strings.HasPrefix(operator, "tablefullscan") || strings.HasPrefix(operator, "tablerangescan")) {
			scan = true
		}
		if !strings.Contains(line, "match_against(") {
			continue
		}
		if !strings.HasPrefix(operator, "selection") {
			return fmt.Errorf("expected MATCH in a Selection, got %s", operator)
		}
		if native && !isTiFlash(task) {
			return fmt.Errorf("MATCH is not in a TiFlash Selection (task %s)", task)
		}
		if !native && task != "root" {
			return fmt.Errorf("expected TiDB root MATCH fallback, got task %s", task)
		}
		match = true
	}
	if !match {
		return fmt.Errorf("missing MATCH Selection")
	}
	if native && !scan {
		return fmt.Errorf("expected TiFlash TableFullScan or TableRangeScan")
	}
	return nil
}

func TestMatchPlanValidation(t *testing.T) {
	for _, tc := range []struct {
		name, plan             string
		native, decodedSummary bool
		wantErr                bool
	}{
		{"full_scan", "Selection_1 1 mpp[tiflash] match_against(\"+foo\",body)\n└─TableFullScan_2 10 mpp[tiflash]", true, false, false},
		{"range_scan_with_root_filter", "Selection_1 1 root unrelated(body)\n└─Selection_2 1 mpp[tiflash] match_against(\"+foo\",body)\n  └─TableRangeScan_3 10 mpp[tiflash] range:[-1,10)", true, false, false},
		{"executed_cop_range_without_mpp", "Selection_1\tcop[tiflash]\t1\tmatch_against(\"+foo\",body)\n└─TableRangeScan_2\tcop[tiflash]\t10\trange:[-1,10)", true, true, true},
		{"decoded_mpp_range", "TableReader_1\troot\t1\tMppVersion: 2, data:ExchangeSender_2\n└─ExchangeSender_2\tcop[tiflash]\t1\tExchangeType: PassThrough\n  └─Selection_3\tcop[tiflash]\t1\tmatch_against(\"+foo\",body)\n    └─TableRangeScan_4\tcop[tiflash]\t10\trange:[-1,10)", true, true, false},
		{"summary_missing_sender", "TableReader_1 root 1 MppVersion: 2, data:ExchangeSender_2\n└─Selection_3 cop[tiflash] 1 match_against(\"+foo\",body)\n  └─TableRangeScan_4 cop[tiflash] 10 range:[-1,10)", true, true, true},
		{"summary_missing_reader", "ExchangeSender_2 cop[tiflash] 1 ExchangeType: PassThrough\n└─Selection_3 cop[tiflash] 1 match_against(\"+foo\",body)\n  └─TableRangeScan_4 cop[tiflash] 10 range:[-1,10)", true, true, true},
		{"batch_cop_rejected", "Selection_1 1 batchCop[tiflash] match_against(\"+foo\",body)\n└─TableFullScan_2 10 batchCop[tiflash]", true, false, true},
		{"cop_not_allowed", "Selection_1 1 cop[tiflash] match_against(\"+foo\",body)\n└─TableRangeScan_2 10 cop[tiflash]", true, false, true},
		{"no_match", "Selection_1 1 mpp[tiflash] gt(id,0)\n└─TableFullScan_2 10 mpp[tiflash]", true, false, true},
		{"root_match", "Selection_1 1 root match_against(\"+foo\",body)\n└─Selection_2 1 mpp[tiflash] gt(id,0)\n  └─TableRangeScan_3 10 mpp[tiflash]", true, false, true},
		{"mixed_match_placement", "Selection_1 1 root match_against(\"+bar\",body)\n└─Selection_2 1 mpp[tiflash] match_against(\"+foo\",body)\n  └─TableFullScan_3 10 mpp[tiflash]", true, false, true},
		{"projection_match", "Projection_1 1 mpp[tiflash] match_against(\"+foo\",body)\n└─TableFullScan_2 10 mpp[tiflash]", true, false, true},
		{"wrong_scan_task", "Selection_1 1 mpp[tiflash] match_against(\"+foo\",body)\n└─TableRangeScan_2 10 cop[tikv]", true, false, true},
		{"dual_not_pushdown_evidence", "TableDual_1 0 root rows:0", true, false, true},
		{"tici", "Selection_1 1 root match_against(\"+foo\",body)\n└─IndexRangeScan_2 10 cop[tici]", false, false, true},
		{"fallback", "Selection_1 1 root match_against(\"+foo\",body)\n└─TableRangeScan_2 10 cop[tikv]", false, false, false},
		{"fallback_with_replica_scan", "Selection_1 1 root match_against(\"+foo\",body)\n└─TableRangeScan_2 10 mpp[tiflash]", false, false, false},
		{"fallback_match_pushed", "Selection_1 1 cop[tiflash] match_against(\"+foo\",body)\n└─TableRangeScan_2 10 cop[tiflash]", false, true, true},
		{"task_word_in_search", "Selection_1 1 root match_against(\"+mpp[tiflash] selection\",body)\n└─TableFullScan_2 10 mpp[tiflash]", true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateMatchPlan(tc.plan, tc.native, tc.decodedSummary); (err != nil) != tc.wantErr {
				t.Fatalf("want error=%v, got %v for:\n%s", tc.wantErr, err, tc.plan)
			}
		})
	}
}
