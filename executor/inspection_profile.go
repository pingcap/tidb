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
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
)

type profileBuilder struct {
	sctx        sessionctx.Context
	idMap       map[string]uint64
	idAllocator uint64
	totalValue  float64
	uniqueMap   map[string]struct{}
	buf         *bytes.Buffer
	start       time.Time
	end         time.Time
}

type metricNode struct {
	table      string
	name       string
	label      []string
	condition  string
	labelValue map[string]float64
	value      float64
	unit       int64
	children   []*metricNode
	// isPartOfParent indicates the parent of this node not fully contain this node.
	isPartOfParent bool
	initialized    bool
}

func (n *metricNode) getName(label string) string {
	name := n.table
	if n.name != "" {
		name = n.name
	}
	if len(label) != 0 {
		name = name + "." + label
	}
	return name
}

func (n *metricNode) getValue(pb *profileBuilder) (float64, error) {
	if n.initialized {
		return n.value, nil
	}
	n.initialized = true
	v, err := pb.getMetricValue(n)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(v) {
		n.value = 0
	} else {
		n.value = v
	}
	return n.value, nil
}

func (pb *profileBuilder) getMetricValue(n *metricNode) (float64, error) {
	if n.labelValue != nil {
		return n.value, nil
	}
	n.labelValue = make(map[string]float64)
	var query string
	format := "2006-01-02 15:04:05"
	queryCondition := fmt.Sprintf("where time >= '%v' and time <= '%v'", pb.start.Format(format), pb.end.Format(format))
	if n.condition != "" {
		queryCondition += (" and " + n.condition)
	}
	if len(n.label) == 0 {
		query = fmt.Sprintf("select sum(value), '' from `metrics_schema`.`%v_total_time` %v", n.table, queryCondition)
	} else {
		query = fmt.Sprintf("select sum(value), `%[3]s` from `metrics_schema`.`%[1]s_total_time` %[2]s group by `%[3]s` having sum(value) > 0",
			n.table, queryCondition, strings.Join(n.label, "`,`"))
	}
	rows, _, err := pb.sctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQLWithContext(context.Background(), query)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 || rows[0].Len() == 0 {
		return 0, nil
	}

	for _, row := range rows {
		v := row.GetFloat64(0)
		if n.unit != 0 {
			v = v / float64(n.unit)
		}
		label := ""
		for i := 1; i < row.Len(); i++ {
			if i > 1 {
				label += ","
			}
			label += row.GetString(i)
		}
		if label == "" && len(n.label) > 0 {
			continue
		}
		n.labelValue[label] = v
		n.value += v
	}
	return n.value, nil
}

// NewProfileBuilder returns a new profileBuilder.
func NewProfileBuilder(sctx sessionctx.Context, start, end time.Time) *profileBuilder {
	return &profileBuilder{
		sctx:        sctx,
		idMap:       make(map[string]uint64),
		idAllocator: uint64(1),
		buf:         bytes.NewBuffer(make([]byte, 0, 1024)),
		uniqueMap:   make(map[string]struct{}),
		start:       start,
		end:         end,
	}
}

// Collect uses to collect the related metric information.
func (pb *profileBuilder) Collect() error {
	pb.buf.WriteString(fmt.Sprintf(`digraph "%s" {`, "tidb_profile"))
	pb.buf.WriteByte('\n')
	pb.buf.WriteString(`node [style=filled fillcolor="#f8f8f8"]`)
	pb.buf.WriteByte('\n')
	err := pb.addMetricTree(pb.genTiDBQueryTree(), "tidb_query_total_time")
	if err != nil {
		return err
	}
	return nil
}

// Build returns the metric profile dot.
func (pb *profileBuilder) Build() []byte {
	pb.buf.WriteByte('}')
	return pb.buf.Bytes()
}

func (pb *profileBuilder) getNameID(name string) uint64 {
	if id, ok := pb.idMap[name]; ok {
		return id
	}
	id := pb.idAllocator
	pb.idAllocator++
	pb.idMap[name] = id
	return id
}

func (pb *profileBuilder) addMetricTree(root *metricNode, name string) error {
	if root == nil {
		return nil
	}
	pb.buf.WriteString(fmt.Sprintf(`subgraph %[1]s { "%[1]s" [shape=box fontsize=16 label="Type: %[1]s\lTime: %s\lDuration: %s\l"] }`, name, pb.start.String(), pb.end.Sub(pb.start).String()))
	pb.buf.WriteByte('\n')
	v, err := root.getValue(pb)
	if err != nil {
		return err
	}
	if v != 0 {
		pb.totalValue = v
	} else {
		pb.totalValue = 1
	}
	return pb.traversal(root)
}

func (pb *profileBuilder) traversal(n *metricNode) error {
	if n == nil {
		return nil
	}
	nodeName := n.getName("")
	if _, ok := pb.uniqueMap[nodeName]; ok {
		return nil
	}
	pb.uniqueMap[nodeName] = struct{}{}
	selfValue, err := n.getValue(pb)
	if err != nil {
		return err
	}

	if pb.ignoreFraction(selfValue, pb.totalValue) {
		return nil
	}
	totalChildrenValue := float64(0)
	for _, child := range n.children {
		childValue, err := child.getValue(pb)
		if err != nil {
			return err
		}
		pb.addNodeEdge(n, child, childValue)
		if !child.isPartOfParent {
			totalChildrenValue += childValue
		}
	}
	selfCost := selfValue - totalChildrenValue
	pb.addNode(n, selfCost, selfValue)
	for _, child := range n.children {
		err := pb.traversal(child)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pb *profileBuilder) addNodeEdge(parent, child *metricNode, childValue float64) {
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
			label = fmt.Sprintf(" %.2fs", childValue)
		}
		pb.addEdge(parent.getName(""), child.getName(""), label, style, childValue)
	} else {
		for label, v := range parent.labelValue {
			if pb.ignoreFraction(v, pb.totalValue) {
				continue
			}
			pb.addEdge(parent.getName(label), child.getName(""), "", style, childValue)
		}
	}
}

func (pb *profileBuilder) addNode(n *metricNode, selfCost, nodeTotal float64) {
	name := n.getName("")
	weight := selfCost
	if len(n.label) > 0 {
		for label, v := range n.labelValue {
			if pb.ignoreFraction(v, pb.totalValue) {
				continue
			}
			labelValue := fmt.Sprintf(" %.2fs", v)
			pb.addEdge(n.getName(""), n.getName(label), labelValue, "", v)
			labelValue = fmt.Sprintf("%s\n %.2fs (%.2f%%)", n.getName(label), v, v*100/pb.totalValue)
			pb.addNodeDef(n.getName(label), labelValue, v, v)
		}
		weight = selfCost / 2
		// Since this node has labels, all cost was consume on the children, so the selfCost is 0.
		selfCost = 0
	}

	label := fmt.Sprintf("%s\n %.2fs (%.2f%%)\nof %.2fs (%.2f%%)",
		name, selfCost, selfCost*100/pb.totalValue, nodeTotal, nodeTotal*100/pb.totalValue)
	pb.addNodeDef(n.getName(""), label, weight, selfCost)
}

func (pb *profileBuilder) addNodeDef(name, labelValue string, fontWeight, colorWeight float64) {
	baseFontSize, maxFontGrowth := 5, 18.0
	fontSize := baseFontSize
	fontSize += int(math.Ceil(maxFontGrowth * math.Sqrt(math.Abs(fontWeight)/pb.totalValue)))

	pb.buf.WriteString(fmt.Sprintf(`N%d [label="%s" fontsize=%d shape=box color="%s" fillcolor="%s"]`,
		pb.getNameID(name), labelValue, fontSize,
		pb.dotColor(colorWeight/pb.totalValue, false),
		pb.dotColor(colorWeight/pb.totalValue, true)))
	pb.buf.WriteByte('\n')
}

func (pb *profileBuilder) addEdge(from, to, label, style string, value float64) {
	weight := 1 + int(math.Min(value*100/pb.totalValue, 100))
	color := pb.dotColor(value/pb.totalValue, false)
	pb.buf.WriteString(fmt.Sprintf(`N%d -> N%d [`, pb.getNameID(from), pb.getNameID(to)))
	if label != "" {
		pb.buf.WriteString(fmt.Sprintf(` label="%s" `, label))
	}
	if style != "" {
		pb.buf.WriteString(fmt.Sprintf(` style="%s" `, style))
	}
	pb.buf.WriteString(fmt.Sprintf(` weight=%d color="%s"]`, weight, color))
	pb.buf.WriteByte('\n')
}

func (pb *profileBuilder) ignoreFraction(value, total float64) bool {
	return value*100/total < 0.01
}

// dotColor function is copy from https://github.com/google/pprof.
func (pb *profileBuilder) dotColor(score float64, isBackground bool) string {
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

func (pb *profileBuilder) genTiDBQueryTree() *metricNode {
	tidbKVRequest := &metricNode{
		table:          "tidb_kv_request",
		isPartOfParent: true,
		label:          []string{"type"},
		children: []*metricNode{
			{
				table: "tidb_batch_client_wait",
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
								table: "tikv_cop_wait",
								label: []string{"type"},
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
