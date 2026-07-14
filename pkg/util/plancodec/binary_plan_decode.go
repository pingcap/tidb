// Copyright 2022 PingCAP, Inc.
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

package plancodec

import (
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/texttree"
	"github.com/pingcap/tipb/go-tipb"
)

// DecodeBinaryPlan decode the binary plan and display it similar to EXPLAIN ANALYZE statement.
func DecodeBinaryPlan(binaryPlan string) (string, error) {
	protoBytes, err := Decompress(binaryPlan)
	if err != nil {
		return "", err
	}
	pb := &tipb.ExplainData{}
	err = pb.Unmarshal(protoBytes)
	if err != nil {
		return "", err
	}
	if pb.DiscardedDueToTooLong {
		return planDiscardedDecoded, nil
	}
	// 1. decode the protobuf into strings
	rows := decodeBinaryOperator(pb.Main, "", true, pb.WithRuntimeStats, nil, false)
	for _, cte := range pb.Ctes {
		rows = decodeBinaryOperator(cte, "", true, pb.WithRuntimeStats, rows, false)
	}
	for _, subQ := range pb.Subqueries {
		rows = decodeBinaryOperator(subQ, "", true, pb.WithRuntimeStats, rows, false)
	}
	if len(rows) == 0 {
		return "", nil
	}

	// 2. calculate the max length of each column and the total length
	// Because the text tree part of the "id" column contains characters that consist of multiple bytes, we need the
	// lengths calculated in bytes and runes both. Length in bytes is for preallocating memory. Length in runes is
	// for padding space to align the content.
	runeMaxLens, byteMaxLens := calculateMaxFieldLens(rows, pb.WithRuntimeStats)
	singleRowLen := 0
	for _, fieldLen := range byteMaxLens {
		singleRowLen += fieldLen
		// every field begins with "| " and ends with " "
		singleRowLen += 3
	}
	// every row ends with " |\n"
	singleRowLen += 3
	// length for a row * (row count + 1(for title row))
	totalBytes := singleRowLen * (len(rows) + 1)
	// there is a "\n" at the beginning
	totalBytes++

	// 3. format the strings and get the final result
	var b strings.Builder
	b.Grow(totalBytes)
	var titleFields []string
	if pb.WithRuntimeStats {
		titleFields = fullTitleFields
	} else {
		titleFields = noRuntimeStatsTitleFields
	}
	b.WriteString("\n")
	for i, str := range titleFields {
		b.WriteString("| ")
		b.WriteString(str)
		if len([]rune(str)) < runeMaxLens[i] {
			// append spaces to align the content
			b.WriteString(strings.Repeat(" ", runeMaxLens[i]-len([]rune(str))))
		}
		b.WriteString(" ")
		if i == len(titleFields)-1 {
			b.WriteString(" |\n")
		}
	}
	for _, row := range rows {
		for i, str := range row {
			b.WriteString("| ")
			b.WriteString(str)
			if len([]rune(str)) < runeMaxLens[i] {
				// append spaces to align the content
				b.WriteString(strings.Repeat(" ", runeMaxLens[i]-len([]rune(str))))
			}
			b.WriteString(" ")
			if i == len(titleFields)-1 {
				b.WriteString(" |\n")
			}
		}
	}
	return b.String(), nil
}

// DecodeBinaryPlan4Connection decodes a binary execution plan for the `EXPLAIN FOR CONNECTION` statement.
// This function is also used by TopSQL for plan analysis.
func DecodeBinaryPlan4Connection(binaryPlan string, format string, forTopsql bool) ([][]string, error) {
	protoBytes, err := Decompress(binaryPlan)
	if err != nil {
		return nil, err
	}
	pb := &tipb.ExplainData{}
	err = pb.Unmarshal(protoBytes)
	if err != nil {
		return nil, err
	}
	if pb.DiscardedDueToTooLong {
		return nil, nil
	}
	isBrief := format == types.ExplainFormatBrief
	// decode the protobuf into strings
	rows := decodeBinaryOperator(pb.Main, "", true, pb.WithRuntimeStats, nil, isBrief)
	for _, cte := range pb.Ctes {
		rows = decodeBinaryOperator(cte, "", true, pb.WithRuntimeStats, rows, isBrief)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	// Define column indices for each format
	var columnIndices []int
	if pb.WithRuntimeStats && !forTopsql {
		switch format {
		case types.ExplainFormatBrief, types.ExplainFormatROW:
			columnIndices = []int{0, 1, 3, 4, 5, 6, 7, 8, 9}
		case types.ExplainFormatPlanTree:
			columnIndices = []int{0, 2, 3, 4, 5, 6, 7, 8}
		case types.ExplainFormatVerbose:
			columnIndices = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		}
	} else {
		switch format {
		case types.ExplainFormatBrief, types.ExplainFormatROW:
			columnIndices = []int{0, 1, 3, 4, 5}
		case types.ExplainFormatPlanTree:
			columnIndices = []int{0, 2, 3, 4, 5}
		case types.ExplainFormatVerbose:
			columnIndices = []int{0, 1, 2, 3, 4, 5}
		}
	}
	// Extract specified columns
	result := make([][]string, len(rows))
	for i, row := range rows {
		result[i] = make([]string, len(columnIndices))
		for j, idx := range columnIndices {
			result[i][j] = row[idx]
		}
	}

	return result, nil
}

var (
	noRuntimeStatsTitleFields = []string{"id", "estRows", "estCost", "task", "access object", "operator info"}
	fullTitleFields           = []string{"id", "estRows", "estCost", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
)

func calculateMaxFieldLens(rows [][]string, hasRuntimeStats bool) (runeLens, byteLens []int) {
	runeLens = make([]int, len(rows[0]))
	byteLens = make([]int, len(rows[0]))
	for _, row := range rows {
		for i, field := range row {
			if runeLens[i] < len([]rune(field)) {
				runeLens[i] = len([]rune(field))
			}
			if byteLens[i] < len(field) {
				byteLens[i] = len(field)
			}
		}
	}
	var titleFields []string
	if hasRuntimeStats {
		titleFields = fullTitleFields
	} else {
		titleFields = noRuntimeStatsTitleFields
	}
	for i := range byteLens {
		if runeLens[i] < len([]rune(titleFields[i])) {
			runeLens[i] = len([]rune(titleFields[i]))
		}
		if byteLens[i] < len(titleFields[i]) {
			byteLens[i] = len(titleFields[i])
		}
	}
	return
}

func decodeBinaryOperator(op *tipb.ExplainOperator, indent string, isLastChild, hasRuntimeStats bool, out [][]string, isBrief bool) [][]string {
	row := make([]string, 0, 10)
	// 1. extract the information and turn them into strings for display
	var explainID string
	if isBrief {
		explainID = texttree.PrettyIdentifier(op.BriefName+printDriverSide(op.Labels), indent, isLastChild)
	} else {
		explainID = texttree.PrettyIdentifier(op.Name+printDriverSide(op.Labels), indent, isLastChild)
	}
	estRows := strconv.FormatFloat(op.EstRows, 'f', 2, 64)
	cost := strconv.FormatFloat(op.Cost, 'f', 2, 64)
	var actRows, execInfo, memInfo, diskInfo string
	if hasRuntimeStats {
		actRows = strconv.FormatInt(int64(op.ActRows), 10)
		execInfo = op.RootBasicExecInfo
		groupExecInfo := strings.Join(op.RootGroupExecInfo, ", ")
		if len(groupExecInfo) > 0 {
			if len(execInfo) > 0 {
				execInfo += ", "
			}
			execInfo += groupExecInfo
		}
		if len(op.CopExecInfo) > 0 {
			if len(execInfo) > 0 {
				execInfo += ", "
			}
			execInfo += op.CopExecInfo
		}
		if op.MemoryBytes < 0 {
			memInfo = "N/A"
		} else {
			memInfo = memory.FormatBytes(op.MemoryBytes)
		}
		if op.DiskBytes < 0 {
			diskInfo = "N/A"
		} else {
			diskInfo = memory.FormatBytes(op.DiskBytes)
		}
	}
	task := op.TaskType.String()
	if op.TaskType != tipb.TaskType_unknown && op.TaskType != tipb.TaskType_root {
		task = task + "[" + op.StoreType.String() + "]"
	}
	accessObject := printAccessObject(op.AccessObjects)

	// 2. append the strings to the slice
	row = append(row, explainID, estRows, cost)
	if hasRuntimeStats {
		row = append(row, actRows)
	}
	row = append(row, task, accessObject)
	if hasRuntimeStats {
		row = append(row, execInfo)
	}
	// Choose the appropriate operator info based on isBrief flag
	operatorInfo := op.OperatorInfo
	if isBrief && op.BriefOperatorInfo != "" {
		operatorInfo = op.BriefOperatorInfo
	}
	row = append(row, operatorInfo)

	if hasRuntimeStats {
		row = append(row, memInfo, diskInfo)
	}
	out = append(out, row)

	// 3. recursively process the children
	children := make([]*tipb.ExplainOperator, len(op.Children))
	copy(children, op.Children)
	if len(children) == 2 &&
		len(children[0].Labels) >= 1 &&
		children[0].Labels[0] == tipb.OperatorLabel_probeSide &&
		len(children[1].Labels) >= 1 &&
		children[1].Labels[0] == tipb.OperatorLabel_buildSide {
		children[0], children[1] = children[1], children[0]
	}
	childIndent := texttree.Indent4Child(indent, isLastChild)
	for i, child := range children {
		out = decodeBinaryOperator(child, childIndent, i == len(children)-1, hasRuntimeStats, out, isBrief)
	}
	return out
}

func printDriverSide(labels []tipb.OperatorLabel) string {
	strs := make([]string, 0, len(labels))
	for _, label := range labels {
		switch label {
		case tipb.OperatorLabel_empty:
			strs = append(strs, "")
		case tipb.OperatorLabel_buildSide:
			strs = append(strs, "(Build)")
		case tipb.OperatorLabel_probeSide:
			strs = append(strs, "(Probe)")
		case tipb.OperatorLabel_seedPart:
			strs = append(strs, "(Seed Part)")
		case tipb.OperatorLabel_recursivePart:
			strs = append(strs, "(Recursive Part)")
		}
	}
	return strings.Join(strs, "")
}

func printDynamicPartitionObject(ao *tipb.DynamicPartitionAccessObject) string {
	if ao == nil {
		return ""
	}
	if ao.AllPartitions {
		return "partition:all"
	} else if len(ao.Partitions) == 0 {
		return "partition:dual"
	}
	return "partition:" + strings.Join(ao.Partitions, ",")
}

func printAccessObject(pbAccessObjs []*tipb.AccessObject) string {
	strs := make([]string, 0, len(pbAccessObjs))
	for _, pbAccessObj := range pbAccessObjs {
		switch ao := pbAccessObj.AccessObject.(type) {
		case *tipb.AccessObject_DynamicPartitionObjects:
			if ao == nil || ao.DynamicPartitionObjects == nil {
				return ""
			}
			aos := ao.DynamicPartitionObjects.Objects
			if len(aos) == 0 {
				return ""
			}
			// If it only involves one table, just print the partitions.
			if len(aos) == 1 {
				return printDynamicPartitionObject(aos[0])
			}
			var b strings.Builder
			// If it involves multiple tables, we also need to print the table name.
			for i, access := range aos {
				if access == nil {
					continue
				}
				if i != 0 {
					b.WriteString(", ")
				}
				b.WriteString(printDynamicPartitionObject(access))
				b.WriteString(" of " + access.Table)
			}
			strs = append(strs, b.String())
		case *tipb.AccessObject_ScanObject:
			if ao == nil || ao.ScanObject == nil {
				return ""
			}
			scanAO := ao.ScanObject
			var b strings.Builder
			if len(scanAO.Table) > 0 {
				b.WriteString("table:" + scanAO.Table)
			}
			if len(scanAO.Partitions) > 0 {
				b.WriteString(", partition:" + strings.Join(scanAO.Partitions, ","))
			}
			for _, index := range scanAO.Indexes {
				if index.IsClusteredIndex {
					b.WriteString(", clustered index:")
				} else {
					b.WriteString(", index:")
				}
				b.WriteString(index.Name + "(" + strings.Join(index.Cols, ", ") + ")")
			}
			strs = append(strs, b.String())
		case *tipb.AccessObject_OtherObject:
			strs = append(strs, ao.OtherObject)
		}
	}
	return strings.Join(strs, "")
}
