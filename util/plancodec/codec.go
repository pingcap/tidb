// Copyright 2019 PingCAP, Inc.
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

package plancodec

import (
	"bytes"
	"encoding/base64"
	"strconv"
	"strings"
	"sync"

	"github.com/golang/snappy"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/v4/util/texttree"
)

const (
	rootTaskType = "0"
	copTaskType  = "1"
)

const (
	idSeparator    = "_"
	lineBreaker    = '\n'
	lineBreakerStr = "\n"
	separator      = '\t'
	separatorStr   = "\t"
)

var decoderPool = sync.Pool{
	New: func() interface{} {
		return &planDecoder{}
	},
}

// DecodePlan use to decode the string to plan tree.
func DecodePlan(planString string) (string, error) {
	if len(planString) == 0 {
		return "", nil
	}
	pd := decoderPool.Get().(*planDecoder)
	defer decoderPool.Put(pd)
	pd.buf.Reset()
	return pd.decode(planString)
}

// DecodeNormalizedPlan decodes the string to plan tree.
func DecodeNormalizedPlan(planString string) (string, error) {
	if len(planString) == 0 {
		return "", nil
	}
	pd := decoderPool.Get().(*planDecoder)
	defer decoderPool.Put(pd)
	pd.buf.Reset()
	return pd.buildPlanTree(planString)
}

type planDecoder struct {
	buf       bytes.Buffer
	depths    []int
	indents   [][]rune
	planInfos []*planInfo
}

type planInfo struct {
	depth  int
	fields []string
}

func (pd *planDecoder) decode(planString string) (string, error) {
	str, err := decompress(planString)
	if err != nil {
		return "", err
	}
	return pd.buildPlanTree(str)
}

func (pd *planDecoder) buildPlanTree(planString string) (string, error) {
	nodes := strings.Split(planString, lineBreakerStr)
	if len(pd.depths) < len(nodes) {
		pd.depths = make([]int, 0, len(nodes))
		pd.planInfos = make([]*planInfo, 0, len(nodes))
		pd.indents = make([][]rune, 0, len(nodes))
	}
	pd.depths = pd.depths[:0]
	pd.planInfos = pd.planInfos[:0]
	planInfos := pd.planInfos
	for _, node := range nodes {
		p, err := decodePlanInfo(node)
		if err != nil {
			return "", err
		}
		if p == nil {
			continue
		}
		planInfos = append(planInfos, p)
		pd.depths = append(pd.depths, p.depth)
	}

	// Calculated indentation of plans.
	pd.initPlanTreeIndents()
	for i := 1; i < len(pd.depths); i++ {
		parentIndex := pd.findParentIndex(i)
		pd.fillIndent(parentIndex, i)
	}
	// Align the value of plan fields.
	pd.alignFields(planInfos)

	for i, p := range planInfos {
		if i > 0 {
			pd.buf.WriteByte(lineBreaker)
		}
		// This is for alignment.
		pd.buf.WriteByte(separator)
		pd.buf.WriteString(string(pd.indents[i]))
		for j := 0; j < len(p.fields); j++ {
			if j > 0 {
				pd.buf.WriteByte(separator)
			}
			pd.buf.WriteString(p.fields[j])
		}
	}
	return pd.buf.String(), nil
}

func (pd *planDecoder) initPlanTreeIndents() {
	pd.indents = pd.indents[:0]
	for i := 0; i < len(pd.depths); i++ {
		indent := make([]rune, 2*pd.depths[i])
		pd.indents = append(pd.indents, indent)
		if len(indent) == 0 {
			continue
		}
		for i := 0; i < len(indent)-2; i++ {
			indent[i] = ' '
		}
		indent[len(indent)-2] = texttree.TreeLastNode
		indent[len(indent)-1] = texttree.TreeNodeIdentifier
	}
}

func (pd *planDecoder) findParentIndex(childIndex int) int {
	for i := childIndex - 1; i > 0; i-- {
		if pd.depths[i]+1 == pd.depths[childIndex] {
			return i
		}
	}
	return 0
}
func (pd *planDecoder) fillIndent(parentIndex, childIndex int) {
	depth := pd.depths[childIndex]
	if depth == 0 {
		return
	}
	idx := depth*2 - 2
	for i := childIndex - 1; i > parentIndex; i-- {
		if pd.indents[i][idx] == texttree.TreeLastNode {
			pd.indents[i][idx] = texttree.TreeMiddleNode
			break
		}
		pd.indents[i][idx] = texttree.TreeBody
	}
}

func (pd *planDecoder) alignFields(planInfos []*planInfo) {
	if len(planInfos) == 0 {
		return
	}
	fieldsLen := len(planInfos[0].fields)
	// Last field no need to align.
	fieldsLen--
	for colIdx := 0; colIdx < fieldsLen; colIdx++ {
		maxFieldLen := pd.getMaxFieldLength(colIdx, planInfos)
		for rowIdx, p := range planInfos {
			fillLen := maxFieldLen - pd.getPlanFieldLen(rowIdx, colIdx, p)
			for i := 0; i < fillLen; i++ {
				p.fields[colIdx] += " "
			}
		}
	}
}

func (pd *planDecoder) getMaxFieldLength(idx int, planInfos []*planInfo) int {
	maxLength := -1
	for rowIdx, p := range planInfos {
		l := pd.getPlanFieldLen(rowIdx, idx, p)
		if l > maxLength {
			maxLength = l
		}
	}
	return maxLength
}

func (pd *planDecoder) getPlanFieldLen(rowIdx, colIdx int, p *planInfo) int {
	if colIdx == 0 {
		return len(p.fields[0]) + len(pd.indents[rowIdx])
	}
	return len(p.fields[colIdx])
}

func decodePlanInfo(str string) (*planInfo, error) {
	values := strings.Split(str, separatorStr)
	if len(values) < 2 {
		return nil, nil
	}

	p := &planInfo{
		fields: make([]string, 0, len(values)-1),
	}
	for i, v := range values {
		switch i {
		// depth
		case 0:
			depth, err := strconv.Atoi(v)
			if err != nil {
				return nil, errors.Errorf("decode plan: %v, depth: %v, error: %v", str, v, err)
			}
			p.depth = depth
		// plan ID
		case 1:
			ids := strings.Split(v, idSeparator)
			if len(ids) != 2 {
				return nil, errors.Errorf("decode plan: %v error, invalid plan id: %v", str, v)
			}
			planID, err := strconv.Atoi(ids[0])
			if err != nil {
				return nil, errors.Errorf("decode plan: %v, plan id: %v, error: %v", str, v, err)
			}
			p.fields = append(p.fields, PhysicalIDToTypeString(planID)+idSeparator+ids[1])
		// task type
		case 2:
			if v == rootTaskType {
				p.fields = append(p.fields, "root")
			} else {
				p.fields = append(p.fields, "cop")
			}
		default:
			p.fields = append(p.fields, v)
		}
	}
	return p, nil
}

// EncodePlanNode is used to encode the plan to a string.
func EncodePlanNode(depth, pid int, planType string, isRoot bool, rowCount float64, explainInfo string, buf *bytes.Buffer) {
	buf.WriteString(strconv.Itoa(depth))
	buf.WriteByte(separator)
	buf.WriteString(encodeID(planType, pid))
	buf.WriteByte(separator)
	if isRoot {
		buf.WriteString(rootTaskType)
	} else {
		buf.WriteString(copTaskType)
	}
	buf.WriteByte(separator)
	buf.WriteString(strconv.FormatFloat(rowCount, 'f', -1, 64))
	buf.WriteByte(separator)
	buf.WriteString(explainInfo)
	buf.WriteByte(lineBreaker)
}

// NormalizePlanNode is used to normalize the plan to a string.
func NormalizePlanNode(depth, pid int, planType string, isRoot bool, explainInfo string, buf *bytes.Buffer) {
	buf.WriteString(strconv.Itoa(depth))
	buf.WriteByte(separator)
	buf.WriteString(encodeID(planType, pid))
	buf.WriteByte(separator)
	if isRoot {
		buf.WriteString(rootTaskType)
	} else {
		buf.WriteString(copTaskType)
	}
	buf.WriteByte(separator)
	buf.WriteString(explainInfo)
	buf.WriteByte(lineBreaker)
}

func encodeID(planType string, id int) string {
	planID := TypeStringToPhysicalID(planType)
	return strconv.Itoa(planID) + idSeparator + strconv.Itoa(id)
}

// Compress is used to compress the input with zlib.
func Compress(input []byte) string {
	compressBytes := snappy.Encode(nil, input)
	return base64.StdEncoding.EncodeToString(compressBytes)
}

func decompress(str string) (string, error) {
	decodeBytes, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", err
	}

	bs, err := snappy.Decode(nil, decodeBytes)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}
