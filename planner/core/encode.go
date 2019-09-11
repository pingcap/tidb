package core

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
)

type PlanEncoder struct {
	buf          bytes.Buffer
	encodedPlans map[int]bool
}

const (
	rootTaskType = "0"
	copTaskType  = "1"
)

const (
	lineBreaker    = '\n'
	lineBreakerStr = "\n"
	separator      = '\t'
	separatorStr   = "\t"
)

func (pn *PlanEncoder) Encode(p PhysicalPlan) string {
	pn.encodedPlans = make(map[int]bool)
	pn.encode(p, rootTaskType, 0)
	str, err := compress(pn.buf.Bytes())
	if err != nil {
		fmt.Println(err)
	}
	return str
}

func (pn *PlanEncoder) encode(p PhysicalPlan, taskType string, depth int) {
	encodePlanNode(p, taskType, depth, &pn.buf)
	pn.encodedPlans[p.ID()] = true

	depth++
	for _, child := range p.Children() {
		if pn.encodedPlans[child.ID()] {
			continue
		}
		pn.encode(child.(PhysicalPlan), taskType, depth)
	}
	switch copPlan := p.(type) {
	case *PhysicalTableReader:
		pn.encode(copPlan.tablePlan, copTaskType, depth)
	case *PhysicalIndexReader:
		pn.encode(copPlan.indexPlan, copTaskType, depth)
	case *PhysicalIndexLookUpReader:
		pn.encode(copPlan.indexPlan, copTaskType, depth)
		pn.encode(copPlan.tablePlan, copTaskType, depth)
	}
}

type PlanDecoder struct {
	buf     bytes.Buffer
	depths  []int
	indents [][]rune
}

type planInfo struct {
	depth  int
	fields []string
}

func (pn *PlanDecoder) Decode(planString string) (string, error) {
	str, err := decompress(planString)
	if err != nil {
		return "", err
	}

	nodes := strings.Split(str, lineBreakerStr)
	pn.depths = make([]int, 0, len(nodes))
	planInfos := make([]*planInfo, 0, len(nodes))
	for _, node := range nodes {
		p, err := decodePlanInfo(node)
		if err != nil {
			return "", err
		}
		if p == nil {
			continue
		}
		planInfos = append(planInfos, p)
		pn.depths = append(pn.depths, p.depth)
	}

	pn.initPlanTreeIndents()

	for i := 1; i < len(pn.depths); i++ {
		parentIndex := pn.findParentIndex(i)
		pn.fillIndent(parentIndex, i)
	}

	pn.buf.Reset()
	for i, p := range planInfos {
		pn.buf.WriteString(string(pn.indents[i]))
		for i := 0; i < len(p.fields); i++ {
			pn.buf.WriteString(p.fields[i])
			pn.buf.WriteByte(separator)
		}
		pn.buf.WriteByte(lineBreaker)
	}

	return pn.buf.String(), nil
}

func (pn *PlanDecoder) initPlanTreeIndents() {
	indents := make([][]rune, len(pn.depths))
	for i := 0; i < len(pn.depths); i++ {
		indent := make([]rune, 2*pn.depths[i])
		indents[i] = indent
		if len(indent) == 0 {
			continue
		}
		for i := 0; i < len(indent)-2; i++ {
			indent[i] = ' '
		}
		indent[len(indent)-2] = treeLastNode
		indent[len(indent)-1] = treeNodeIdentifier
	}
	pn.indents = indents
}

func encodePlanNode(p PhysicalPlan, taskType string, depth int, buf *bytes.Buffer) {
	buf.WriteString(strconv.Itoa(depth))
	buf.WriteByte(separator)
	buf.WriteString(p.EncodeID())
	buf.WriteByte(separator)
	buf.WriteString(taskType)
	buf.WriteByte(separator)
	buf.WriteString(strconv.FormatFloat(p.statsInfo().RowCount, 'f', -1, 64))
	buf.WriteByte(separator)
	buf.WriteString(p.ExplainInfo())
	buf.WriteByte(lineBreaker)
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
			ids := strings.Split(v, "_")
			if len(ids) != 2 {
				return nil, errors.Errorf("decode plan: %v error, invalid plan id: %v", str, v)
			}
			planID, err := strconv.Atoi(ids[0])
			if err != err {
				return nil, errors.Errorf("decode plan: %v, plan id: %v, error: %v", str, v, err)
			}
			p.fields = append(p.fields, PhysicalIDToTypeString(planID)+"_"+ids[1])
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

func (pn *PlanDecoder) findParentIndex(childIndex int) int {
	for i := childIndex - 1; i > 0; i-- {
		if pn.depths[i]+1 == pn.depths[childIndex] {
			return i
		}
	}
	return 0
}
func (pn *PlanDecoder) fillIndent(parentIndex, childIndex int) {
	depth := pn.depths[childIndex]
	if depth == 0 {
		return
	}
	idx := depth*2 - 2
	for i := childIndex - 1; i > parentIndex; i-- {
		if pn.indents[i][idx] == treeLastNode {
			pn.indents[i][idx] = treeMiddleNode
			break
		}
		pn.indents[i][idx] = treeBody
	}
}

func compress(input []byte) (string, error) {
	var in bytes.Buffer
	w, err := zlib.NewWriterLevel(&in, zlib.BestCompression)
	if err != nil {
		return "", err
	}
	_, err = w.Write(input)
	if err != nil {
		return "", err
	}
	err = w.Close()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(in.Bytes()), nil
}

func decompress(str string) (string, error) {
	decodeBytes, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return "", err
	}
	reader := bytes.NewReader(decodeBytes)
	out, err := zlib.NewReader(reader)
	if err != nil {
		return "", err
	}
	var outbuf bytes.Buffer
	_, err = io.Copy(&outbuf, out)
	if err != nil {
		return "", err
	}
	return outbuf.String(), nil
}
