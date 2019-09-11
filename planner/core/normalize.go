package core

import (
	"bytes"
	"github.com/pingcap/errors"
	"strconv"
	"strings"
)

type PlanNormalizeEncoder struct {
	buf            bytes.Buffer
	explainedPlans map[int]bool
}

const (
	rootTaskType = "0"
	copTaskType  = "1"
)

func (pn *PlanNormalizeEncoder) NormalizePlanTreeString(p PhysicalPlan) string {
	pn.explainedPlans = make(map[int]bool)
	pn.normalizePlanTree(p, rootTaskType, 0)
	str := pn.buf.String()
	if len(str) > 0 && str[len(str)-1] == '\n' {
		str = str[:len(str)-1]
	}
	return str
}

type NormalizedInfoPlan interface {
	ExplainNormalizeInfo() string
}

func (pn *PlanNormalizeEncoder) normalizePlanTree(p PhysicalPlan, taskType string, depth int) {
	pn.buf.WriteString(strconv.Itoa(depth))
	pn.buf.WriteString("\t")
	pn.buf.WriteString(p.EncodeID())
	pn.buf.WriteString("\t")
	pn.buf.WriteString(taskType)
	if normalizeInfoPlan , ok := p.(NormalizedInfoPlan); ok {
		pn.buf.WriteString("\t")
		pn.buf.WriteString(normalizeInfoPlan.ExplainNormalizeInfo())
	}
	pn.buf.WriteString("\n")

	pn.explainedPlans[p.ID()] = true

	depth++
	for _, child := range p.Children() {
		if pn.explainedPlans[child.ID()] {
			continue
		}
		pn.normalizePlanTree(child.(PhysicalPlan), taskType, depth)
	}

	switch copPlan := p.(type) {
	case *PhysicalTableReader:
		pn.normalizePlanTree(copPlan.tablePlan, copTaskType, depth)
	case *PhysicalIndexReader:
		pn.normalizePlanTree(copPlan.indexPlan, copTaskType, depth)
	case *PhysicalIndexLookUpReader:
		pn.normalizePlanTree(copPlan.indexPlan, copTaskType, depth)
		pn.normalizePlanTree(copPlan.tablePlan, copTaskType, depth)
	}
}

type PlanNormalizeDecoder struct {
	buf     bytes.Buffer
	depths  []int
	rows    []string
	indents [][]rune
}

func (pn *PlanNormalizeDecoder) DecodeNormalizePlanTreeString(str string) (string, error) {
	nodes := strings.Split(str, "\n")
	pn.rows = make([]string, 0, len(nodes))
	pn.depths = make([]int, 0, len(nodes))
	for _, node := range nodes {
		err := pn.decodePlanTreeNode(node)
		if err != nil {
			return "", err
		}
	}
	pn.initPlanTreeIndents()

	for i := 1; i < len(pn.depths); i++ {
		parentIndex := pn.findParentIndex(i)
		pn.fillIndent(parentIndex, i)
	}

	pn.buf.Reset()
	for i := 0; i < len(pn.rows); i++ {
		pn.buf.WriteString(string(pn.indents[i]))
		pn.buf.WriteString(pn.rows[i])
		pn.buf.WriteByte('\n')
	}

	return pn.buf.String(), nil
}

func (pn *PlanNormalizeDecoder) initPlanTreeIndents() {
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

func (pn *PlanNormalizeDecoder) decodePlanTreeNode(node string) error {
	pn.buf.Reset()
	values := strings.Split(node, "\t")
	// TODO: check len(values)
	for i, v := range values {
		switch i {
		// depth
		case 0:
			depth, err := strconv.Atoi(v)
			if err != nil {
				return errors.Errorf("decode normalize plan error: invalid depth: %v, node: %v, error: %v", v, node, err.Error())
			}
			pn.depths = append(pn.depths, depth)
		// plan ID
		case 1:
			ids := strings.Split(v, "_")
			if len(ids) != 2 {
				return errors.Errorf("decode normalize plan error: invalid plan value: %v, node: %v", v, node)
			}
			planID, err := strconv.Atoi(ids[0])
			if err != err {
				return errors.Errorf("decode normalize plan error: invalid plan id: %v, node: %v, error: %v", v, node, err.Error())
			}

			pn.buf.WriteString(PhysicalIDToTypeString(planID) + "_" + ids[1])
		// task type
		case 2:
			pn.buf.WriteByte('\t')
			if v == rootTaskType {
				pn.buf.WriteString("root")
			} else {
				pn.buf.WriteString("cop")
			}
		default:
			pn.buf.WriteByte('\t')
			pn.buf.WriteString(v)
		}
	}
	pn.rows = append(pn.rows, pn.buf.String())
	return nil
}

func (pn *PlanNormalizeDecoder) findParentIndex(childIndex int) int {
	for i := childIndex - 1; i > 0; i-- {
		if pn.depths[i]+1 == pn.depths[childIndex] {
			return i
		}
	}
	return 0
}
func (pn *PlanNormalizeDecoder) fillIndent(parentIndex, childIndex int) {
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
