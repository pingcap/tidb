// Copyright 2023 PingCAP, Inc.
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

package pcc

import (
	"strconv"
	"strings"

	"github.com/pingcap/errors"
)

// ParseV6 parse sql into Plan with v6
func ParseV6(SQL string, rows [][]string) (Plan, error) {
	p := Plan{SQL: SQL, Ver: V6}
	root, err := parseV6Op(rows, 0)
	p.Root = root
	return p, err
}

func parseV6Op(rows [][]string, rowNo int) (Operator, error) {
	children := make([]Operator, 0, 2)
	childRowNo := findChildRowNo(rows, rowNo, 0)
	for _, no := range childRowNo {
		child, err := parseV6Op(rows, no)
		if err != nil {
			return nil, err
		}
		children = append(children, child)
	}

	op, err := parseRowV4(rows[rowNo], children)
	if err != nil {
		return nil, err
	}
	return op, nil
}

func parseRowV4(cols []string, children []Operator) (Operator, error) {
	estRows, err := strconv.ParseFloat(strings.TrimSpace(cols[1]), 64)
	if err != nil {
		return nil, err
	}
	opID := extractOperatorID(cols[0])
	opType := MatchOpType(opID)
	if OpTypeIsJoin(opType) {
		adjustJoinChildrenV4(children)
	}
	base := BaseOp{
		id:       opID,
		opType:   opType,
		estRow:   estRows,
		task:     parseTaskType(cols[2]),
		children: children,
	}

	switch opType {
	case OpTypeHashJoin:
		return HashJoinOp{base, JoinTypeUnknown}, nil
	case OpTypeMergeJoin:
		return MergeJoinOp{base, JoinTypeUnknown}, nil
	case OpTypeIndexJoin:
		return IndexJoinOp{base, JoinTypeUnknown}, nil
	case OpTypeTableReader:
		return TableReaderOp{base}, nil
	case OpTypeTableScan:
		kvs := splitKVs(cols[3])
		return TableScanOp{base, kvs["table"]}, nil
	case OpTypeIndexReader:
		return IndexReaderOp{base}, nil
	case OpTypeIndexScan:
		tbl, idx := extractTableIndexV4(cols[3])
		return IndexScanOp{base, tbl, idx}, nil
	case OpTypeIndexLookup:
		return IndexLookupOp{base}, nil
	case OpTypeSelection:
		return SelectionOp{base}, nil
	case OpTypeProjection:
		return ProjectionOp{base}, nil
	case OpTypePointGet:
		kvs := splitKVs(cols[3])
		return PointGetOp{base, false, kvs["table"]}, nil
	case OpTypeHashAgg:
		return HashAggOp{base}, nil
	case OpTypeStreamAgg:
		return StreamAggOp{base}, nil
	case OpTypeMaxOneRow:
		return MaxOneRowOp{base}, nil
	case OpTypeApply:
		return ApplyOp{base}, nil
	case OpTypeLimit:
		return LimitOp{base}, nil
	case OpTypeSort:
		return SortOp{base}, nil
	case OpTypeTopN:
		return TopNOp{base}, nil
	case OpTypeTableDual:
		return TableDual{base}, nil
	case OpTypeSelectLock:
		return SelectLock{base}, nil
	}
	return nil, errors.Errorf("unknown operator type %v", opID)
}

func extractTableIndexV4(info string) (tbl string, idx string) {
	// table:a, index:idx_notice_config_id_type(user_id, notice_config_id, notice_type)
	st, si := "table:", "index:"
	if begin := strings.Index(info, st); begin != -1 {
		if end := strings.Index(info[begin+len(st):], ","); end != -1 {
			tbl = info[begin+len(st) : begin+len(st)+end] // tbl = a
		}
	}
	if begin := strings.Index(info, si); begin != -1 {
		tmp := info[begin+len(si):]
		if begin := strings.Index(tmp, "("); begin != -1 {
			if end := strings.Index(tmp[begin+1:], ")"); end != -1 {
				idx = tmp[begin+1 : begin+1+end] // user_id, notice_config_id, notice_type
			}
		}
	}
	return
}

func adjustJoinChildrenV4(children []Operator) {
	// make children[0] is the outer side
	if strings.Contains(strings.ToLower(children[0].ID()), "probe") {
		children[0], children[1] = children[1], children[0]
	}
}
