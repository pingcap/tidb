package plan

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
)

const (
	// TreeBody indicates the current operator sub-tree is not finished, still
	// has child operators to be attached on.
	TreeBody = '│'
	// TreeMiddleNode indicates this operator is not the last child of the
	// current sub-tree rooted by its parent.
	TreeMiddleNode = '├'
	// TreeLastNode indicates this operator is the last child of the current
	// sub-tree rooted by its parent.
	TreeLastNode = '└'
	// TreeGap is used to represent the gap between the branches of the tree.
	TreeGap = ' '
	// TreeNodeIdentifier is used to replace the TreeGap once we need to attach
	// a node to a sub-tree.
	TreeNodeIdentifier = '─'
)

const (
	RootTaskType = "0"
	CopTaskType  = "1"
)

const (
	LineBreaker    = '\n'
	LineBreakerStr = "\n"
	Separator      = '\t'
	SeparatorStr   = "\t"
)

func encodeID(planType string, id int) string {
	planID := TypeStringToPhysicalID(planType)
	return strconv.Itoa(planID) + "_" + strconv.Itoa(id)
}

func EncodePlanNode(depth, pid int, planType string, taskType string, rowCount float64, explainInfo string, buf *bytes.Buffer) {
	buf.WriteString(strconv.Itoa(depth))
	buf.WriteByte(Separator)
	buf.WriteString(encodeID(planType, pid))
	buf.WriteByte(Separator)
	buf.WriteString(taskType)
	buf.WriteByte(Separator)
	buf.WriteString(strconv.FormatFloat(rowCount, 'f', -1, 64))
	buf.WriteByte(Separator)
	buf.WriteString(explainInfo)
	buf.WriteByte(LineBreaker)
}

var decoderPool = sync.Pool{
	New: func() interface{} {
		return &PlanDecoder{}
	},
}

func DecodePlan(planString string) string {
	pd := decoderPool.Get().(*PlanDecoder)
	defer decoderPool.Put(pd)
	str, err := pd.Decode(planString)
	if err != nil {
		terror.Log(err)
	}
	return str
}

type PlanDecoder struct {
	buf       bytes.Buffer
	depths    []int
	indents   [][]rune
	planInfos []*planInfo
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

	nodes := strings.Split(str, LineBreakerStr)
	if len(pn.depths) < len(nodes) {
		pn.depths = make([]int, 0, len(nodes))
		pn.planInfos = make([]*planInfo, 0, len(nodes))
		pn.indents = make([][]rune, 0, len(nodes))
	}
	pn.depths = pn.depths[:0]
	pn.planInfos = pn.planInfos[:0]
	planInfos := pn.planInfos
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
		if i > 0 {
			pn.buf.WriteByte(LineBreaker)
		}
		pn.buf.WriteString(string(pn.indents[i]))
		for j := 0; j < len(p.fields); j++ {
			if j > 0 {
				pn.buf.WriteByte(Separator)
			}
			pn.buf.WriteString(p.fields[j])
		}
	}

	return pn.buf.String(), nil
}

func (pn *PlanDecoder) initPlanTreeIndents() {
	pn.indents = pn.indents[:0]
	for i := 0; i < len(pn.depths); i++ {
		indent := make([]rune, 2*pn.depths[i])
		pn.indents = append(pn.indents, indent)
		if len(indent) == 0 {
			continue
		}
		for i := 0; i < len(indent)-2; i++ {
			indent[i] = ' '
		}
		indent[len(indent)-2] = TreeLastNode
		indent[len(indent)-1] = TreeNodeIdentifier
	}
}

func decodePlanInfo(str string) (*planInfo, error) {
	values := strings.Split(str, SeparatorStr)
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
			if v == RootTaskType {
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
		if pn.indents[i][idx] == TreeLastNode {
			pn.indents[i][idx] = TreeMiddleNode
			break
		}
		pn.indents[i][idx] = TreeBody
	}
}

func Compress(input []byte) (string, error) {
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
