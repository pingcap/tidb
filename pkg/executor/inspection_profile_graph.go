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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

func (pb *profileBuilder) addNodeEdge(parent, child *metricNode, childValue *metricValue) {
	if pb.ignoreFraction(childValue, pb.totalValue) {
		return
	}
	style := ""
	if child.isPartOfParent {
		style = "dotted"
	}
	if len(parent.label) == 0 {
		label := ""
		if !child.isPartOfParent {
			label = pb.formatValueByTp(childValue.getValue(pb.valueTP))
		}
		pb.addEdge(parent.getName(""), child.getName(""), label, style, childValue.getValue(pb.valueTP))
	} else {
		for label, value := range parent.labelValue {
			if pb.ignoreFraction(value, pb.totalValue) {
				continue
			}
			pb.addEdge(parent.getName(label), child.getName(""), "", style, childValue.getValue(pb.valueTP))
		}
	}
}

func (pb *profileBuilder) addNode(n *metricNode, selfCost, nodeTotal float64) error {
	name := n.getName("")
	weight := selfCost
	if len(n.label) > 0 {
		for label, value := range n.labelValue {
			if pb.ignoreFraction(value, pb.totalValue) {
				continue
			}
			v := value.getValue(pb.valueTP)
			vStr := pb.formatValueByTp(v)
			labelValue := fmt.Sprintf(" %s", vStr)
			pb.addEdge(n.getName(""), n.getName(label), labelValue, "", v)
			labelValue = fmt.Sprintf("%s\n %s (%.2f%%)", n.getName(label), vStr, v*100/pb.totalValue)
			pb.addNodeDef(n.getName(label), labelValue, value.getComment(), v, v)
		}
		weight = selfCost / 2
		// Since this node has labels, all cost was consume on the children, so the selfCost is 0.
		selfCost = 0
	}

	label := fmt.Sprintf("%s\n %s (%.2f%%)\nof %s (%.2f%%)",
		name,
		pb.formatValueByTp(selfCost), selfCost*100/pb.totalValue,
		pb.formatValueByTp(nodeTotal), nodeTotal*100/pb.totalValue)
	pb.addNodeDef(n.getName(""), label, n.value.getComment(), weight, selfCost)
	return nil
}

func (pb *profileBuilder) addNodeDef(name, labelValue, comment string, fontWeight, colorWeight float64) {
	baseFontSize, maxFontSize, maxFontGrowth := 5, 64, 18.0
	fontSize := baseFontSize
	fontSize += int(math.Ceil(maxFontGrowth * math.Sqrt(math.Abs(fontWeight)/pb.totalValue)))
	if fontSize > maxFontSize {
		fontSize = maxFontSize
	}

	fmt.Fprintf(pb.buf, `N%d [label="%s" tooltip="%s" fontsize=%d shape=box color="%s" fillcolor="%s"]`,
		pb.getNameID(name), labelValue, comment, fontSize,
		pb.dotColor(colorWeight/pb.totalValue, false),
		pb.dotColor(colorWeight/pb.totalValue, true))
	pb.buf.WriteByte('\n')
}

func (pb *profileBuilder) addEdge(from, to, label, style string, value float64) {
	weight := 1 + int(math.Min(value*100/pb.totalValue, 100))
	color := pb.dotColor(value/pb.totalValue, false)
	fmt.Fprintf(pb.buf, `N%d -> N%d [`, pb.getNameID(from), pb.getNameID(to))
	if label != "" {
		fmt.Fprintf(pb.buf, ` label="%s" `, label)
	}
	if style != "" {
		fmt.Fprintf(pb.buf, ` style="%s" `, style)
	}
	fmt.Fprintf(pb.buf, ` weight=%d color="%s"]`, weight, color)
	pb.buf.WriteByte('\n')
}

func (pb *profileBuilder) ignoreFraction(v *metricValue, total float64) bool {
	value := v.getValue(pb.valueTP)
	return value*100/total < 0.01
}

func (pb *profileBuilder) formatValueByTp(value float64) string {
	switch pb.valueTP {
	case metricValueCnt:
		return strconv.Itoa(int(value))
	case metricValueSum, metricValueAvg:
		if math.IsNaN(value) {
			return ""
		}
		if math.Abs(value) > 1 {
			// second unit
			return fmt.Sprintf("%.2fs", value)
		} else if math.Abs(value*1000) > 1 {
			// millisecond unit
			return fmt.Sprintf("%.2f ms", value*1000)
		} else if math.Abs(value*1000*1000) > 1 {
			// microsecond unit
			return fmt.Sprintf("%.2f ms", value*1000*1000)
		}
		return time.Duration(int64(value * float64(time.Second))).String()
	}
	panic("should never happen")
}

// dotColor function is copy from https://github.com/google/pprof.
func (*profileBuilder) dotColor(score float64, isBackground bool) string {
	// A float between 0.0 and 1.0, indicating the extent to which
	// colors should be shifted away from grey (to make positive and
	// negative values easier to distinguish, and to make more use of
	// the color range.)
	const shift = 0.7
	// Saturation and value (in hsv colorspace) for background colors.
	const bgSaturation = 0.1
	const bgValue = 0.93
	// Saturation and value (in hsv colorspace) for foreground colors.
	const fgSaturation = 1.0
	const fgValue = 0.7
	// Choose saturation and value based on isBackground.
	var saturation float64
	var value float64
	if isBackground {
		saturation = bgSaturation
		value = bgValue
	} else {
		saturation = fgSaturation
		value = fgValue
	}

	// Limit the score values to the range [-1.0, 1.0].
	score = math.Max(-1.0, math.Min(1.0, score))

	// Reduce saturation near score=0 (so it is colored grey, rather than yellow).
	if math.Abs(score) < 0.2 {
		saturation *= math.Abs(score) / 0.2
	}

	// Apply 'shift' to move scores away from 0.0 (grey).
	if score > 0.0 {
		score = math.Pow(score, (1.0 - shift))
	}
	if score < 0.0 {
		score = -math.Pow(-score, (1.0 - shift))
	}

	var r, g, b float64 // red, green, blue
	if score < 0.0 {
		g = value
		r = value * (1 + saturation*score)
	} else {
		r = value
		g = value * (1 - saturation*score)
	}
	b = value * (1 - saturation)
	return fmt.Sprintf("#%02x%02x%02x", uint8(r*255.0), uint8(g*255.0), uint8(b*255.0))
}

func (*profileBuilder) genTiDBGCTree() *metricNode {
	tidbGC := &metricNode{
		table:          "tidb_gc",
		isPartOfParent: true,
		label:          []string{"stage"},
		children: []*metricNode{
			{
				table:          "tidb_kv_request",
				isPartOfParent: true,
			},
		},
	}
	return tidbGC
}

func (*profileBuilder) genTiDBQueryTree() *metricNode {
	tidbKVRequest := &metricNode{
		table:          "tidb_kv_request",
		isPartOfParent: true,
		label:          []string{"type"},
		children: []*metricNode{
			{
				table: "tidb_batch_client_wait",
				unit:  int64(10e8),
			},
			{
				table: "tidb_batch_client_wait_conn",
			},
			{
				table: "tidb_batch_client_unavailable",
			},
			{
				table:     "pd_client_cmd",
				condition: "type not in ('tso','wait','tso_async_wait')",
			},
			{
				table: "tikv_grpc_message",
				children: []*metricNode{
					{
						table: "tikv_cop_request",
						children: []*metricNode{
							{
								table:     "tikv_cop_wait",
								label:     []string{"type"},
								condition: "type != 'all'",
							},
							{table: "tikv_cop_handle"},
						},
					},
					{
						table: "tikv_scheduler_command",
						children: []*metricNode{
							{table: "tikv_scheduler_latch_wait"},
							{table: "tikv_scheduler_processing_read"},
							{
								table: "tikv_storage_async_request",
								children: []*metricNode{
									{
										table:     "tikv_storage_async_request",
										name:      "tikv_storage_async_request.snapshot",
										condition: "type='snapshot'",
									},
									{
										table:     "tikv_storage_async_request",
										name:      "tikv_storage_async_request.write",
										condition: "type='write'",
										children: []*metricNode{
											{table: "tikv_raftstore_propose_wait"},
											{
												table: "tikv_raftstore_process",
												children: []*metricNode{
													{table: "tikv_raftstore_append_log"},
												},
											},
											{table: "tikv_raftstore_commit_log"},
											{table: "tikv_raftstore_apply_wait"},
											{table: "tikv_raftstore_apply_log"},
										},
									},
								},
							},
						},
					},
					{
						table: "tikv_gc_tasks",
						label: []string{"task"},
					},
				},
			},
		},
	}
	ddlTime := &metricNode{
		table: "tidb_ddl",
		label: []string{"type"},
		children: []*metricNode{
			{
				table: "tidb_ddl_worker",
				label: []string{"type"},
				children: []*metricNode{
					{
						table: "tidb_ddl_batch_add_index",
					},
					{
						table: "tidb_load_schema",
					},
					{
						table: "tidb_ddl_update_self_version",
					},
					{
						table: "tidb_owner_handle_syncer",
					},
					{
						table: "tidb_meta_operation",
					},
				},
			},
		},
	}
	tidbExecute := &metricNode{
		table: "tidb_execute",
		children: []*metricNode{
			{
				table: "pd_start_tso_wait",
			},
			{
				table: "tidb_auto_id_request",
			},
			{
				table:          "tidb_cop",
				isPartOfParent: true,
				children: []*metricNode{
					{
						table:          "tidb_kv_backoff",
						label:          []string{"type"},
						isPartOfParent: true,
					},
					tidbKVRequest,
				},
			},
			{
				table: "tidb_txn_cmd",
				label: []string{"type"},
				children: []*metricNode{
					{
						table:          "tidb_kv_backoff",
						label:          []string{"type"},
						isPartOfParent: true,
					},
					tidbKVRequest,
				},
			},
			ddlTime,
		},
	}
	queryTime := &metricNode{
		table: "tidb_query",
		label: []string{"sql_type"},
		children: []*metricNode{
			{
				table: "tidb_get_token",
				unit:  int64(10e5),
			},
			{
				table: "tidb_parse",
			},
			{
				table: "tidb_compile",
			},
			tidbExecute,
		},
	}

	return queryTime
}
