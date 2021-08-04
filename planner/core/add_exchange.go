package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
)

var MaxThrNum int

var (
	_ PhysicalPlan = &PhysicalXchg{}
)

type XchgType int

// Three places to update when add new xchg operator:
// 1. XchgNames
// 2. isSender
// 3. initXchg
const (
	TypeXchgReceiverRandom        = 0
	TypeXchgReceiverPassThrough   = 1
	TypeXchgReceiverPassThroughHT = 2

	TypeXchgSenderPassThrough = 3
	TypeXchgSenderBroadcastHT = 4
	TypeXchgSenderHash        = 5
	TypeXchgSenderRandom      = 6
)

var XchgNames = []string{
	"XchgReceiverRandom",
	"XchgReceiverPassThrough",
	"XchgReceiverPassThroughHT",
	"XchgSenderPassThrough",
	"XchgSenderBroadcastHT",
	"XchgSenderHash",
	"XchgSenderRandom",
}

type PhysicalXchg struct {
	basePhysicalPlan
	// TODO: remove, just use basePhysicalPlan.stats.
	childStat *property.StatsInfo
	tp        XchgType
	// outStreamCnt is used to compute pipeline's cost.
	outStreamCnt int
	// inStreamCnt is used to record child count.
	inStreamCnt int
}

func (p *PhysicalXchg) isSender() bool {
	switch p.tp {
	case TypeXchgSenderPassThrough, TypeXchgSenderBroadcastHT, TypeXchgSenderHash, TypeXchgSenderRandom:
		return true
	case TypeXchgReceiverRandom, TypeXchgReceiverPassThrough, TypeXchgReceiverPassThroughHT:
		return false
	default:
		panic("unexpected kind of xchg")
	}
}

type XchgProperty struct {
	output              int
	available           int
	isBroadcastHT       bool
	nonOrderedPartition []*expression.Column
	// TODO: not implemented yet.
	// orderedPartition []*expression.Column
	// sorting []*property.SortItem
	// grouping []*expression.Column
}

func newXchgProperty() *XchgProperty {
	return &XchgProperty{output: 1}
}

func (prop *XchgProperty) Clone() *XchgProperty {
	res := &XchgProperty{
		output:              prop.output,
		available:           prop.available,
		isBroadcastHT:       prop.isBroadcastHT,
		nonOrderedPartition: make([]*expression.Column, len(prop.nonOrderedPartition)),
	}
	copy(res.nonOrderedPartition, prop.nonOrderedPartition)
	return res
}

func (p *basePhysicalPlan) SetChildXchgProps(props []*XchgProperty) {
	p.childrenXchgProps = props
}

func (p *PointGetPlan) SetChildXchgProps(props []*XchgProperty) {
	return
}

func (p *BatchPointGetPlan) SetChildXchgProps(props []*XchgProperty) {
	return
}

func (p *basePhysicalPlan) GetChildXchgProps() []*XchgProperty {
	return p.childrenXchgProps
}

func (p *PointGetPlan) GetChildXchgProps() []*XchgProperty {
	return nil
}

func (p *BatchPointGetPlan) GetChildXchgProps() []*XchgProperty {
	return nil
}

func FindBestXchgTask(ctx sessionctx.Context, root PhysicalPlan) (task, error) {
	initProp := newXchgProperty()
	// 1 means the main thread.
	initProp.available = MaxThrNum - 1
	return findBestXchgTask(ctx, root, initProp)
}

func findBestXchgTask(ctx sessionctx.Context, node PhysicalPlan, reqProp *XchgProperty) (task, error) {
	possiblePlans, err := node.TryAddXchg(ctx, reqProp)
	if err != nil {
		return nil, err
	}

	var bestTask task = invalidTask
	for _, p := range possiblePlans {
		planToFind := p
		if xchgReceiver, isXchg := planToFind.(*PhysicalXchg); isXchg {
			planToFind = xchgReceiver.Children()[0].Children()[0]
		}

		// Stop find best task recursively when got xxxReader.
		if isReaderNode(planToFind) {
			bestTask = &xchgTask{
				rootTask: rootTask{
					cst: planToFind.Cost(),
					p:   planToFind,
				},
				dop: 1,
			}
			if p != planToFind {
				bestTask = p.attach2Task(bestTask)
			}
			// Here we assume there is only one possible plan for xxxReader.
			// See xxxReader.TryAddXchg().
			continue
		}

		childTasks := make([]task, 0, len(planToFind.Children()))
		childrenXchgProps := planToFind.GetChildXchgProps()
		bestChildrenTasksFound := true
		for i, child := range planToFind.Children() {
			bestChildTask, err := findBestXchgTask(ctx, child, childrenXchgProps[i])
			if err != nil {
				return invalidTask, err
			}
			if bestChildTask.invalid() {
				bestChildrenTasksFound = false
			}
			childTasks = append(childTasks, bestChildTask)
		}
		if bestChildrenTasksFound {
			curTask := planToFind.attach2Task(childTasks...)
			if p != planToFind {
				curTask = p.attach2Task(curTask)
			}
			if curTask.cost() < bestTask.cost() || (bestTask.invalid() && !curTask.invalid()) {
				bestTask = curTask
			}
		}
	}
	return bestTask, nil
}

func (p *basePhysicalPlan) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	return nil, errors.Errorf("TryAddXchg not implemented yet: %v", p.TP())
}

func (p *PointGetPlan) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) ([]PhysicalPlan, error) {
	return nil, errors.Errorf("TryAddXchg not implemented yet for PointGet")
}

func (p *BatchPointGetPlan) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) ([]PhysicalPlan, error) {
	return nil, errors.Errorf("TryAddXchg not implemented yet for PointGet")
}

func (p *PhysicalSelection) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	return tryAddXchgForBasicPlan(ctx, p, reqProp)
}

func (p *PhysicalProjection) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	return tryAddXchgForBasicPlan(ctx, p, reqProp)
}

func (p *PhysicalTableReader) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	if !hasAvailableThr(reqProp.available, 0) {
		return nil, nil
	}
	if res, err = tryAddXchgPreWork(p, reqProp, res); err != nil {
		return nil, err
	}
	// TODO: for now we add random merge for TableReader, we can also partition by range.
	if reqProp.output != 1 {
		// Clear res, because cost of xchg+xxxReader is greater than xxxReader.
		res = res[:0]
		newNode, err := p.Clone()
		if err != nil {
			return nil, err
		}
		updateChildrenProp(newNode, newXchgProperty())

		var sender *PhysicalXchg
		var receiver *PhysicalXchg
		if reqProp.isBroadcastHT {
			sender = &PhysicalXchg{tp: TypeXchgSenderBroadcastHT}
			receiver = &PhysicalXchg{tp: TypeXchgReceiverPassThroughHT}
		} else {
			sender = &PhysicalXchg{tp: TypeXchgSenderRandom}
			receiver = &PhysicalXchg{tp: TypeXchgReceiverPassThrough}
		}
		if err := initXchg(ctx, sender, newNode, newNode.Stats(), reqProp.output); err != nil {
			return nil, err
		}
		if err := initXchg(ctx, receiver, sender, newNode.Stats(), reqProp.output); err != nil {
			return nil, err
		}

		res = append(res, receiver)
	}
	return res, nil
}

func (p *PhysicalHashJoin) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	// TODO: useOuterToBuild
	if p.UseOuterToBuild {
		return nil, errors.New("TryAddXchg not support UseOuterToBuild for now")
	}

	outStreamCnt := reqProp.output
	if outStreamCnt == 1 {
		outStreamCnt = ctx.GetSessionVars().ExecutorConcurrency
	}
	if res, err = tryBroadcastHJ(ctx, p, outStreamCnt, reqProp); err != nil {
		return nil, err
	}
	// TODO: not support for now.
	// if res, err = tryHashPartitionHJ(ctx, p, outStreamCnt); err != nil {
	//     return nil, err
	// }
	return res, nil
}

func (p *PhysicalLimit) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	if res, err = tryAddXchgPreWork(p, reqProp, res); err != nil {
		return nil, err
	}

	return res, nil
}

func tryBroadcastHJ(ctx sessionctx.Context, node *PhysicalHashJoin, outStreamCnt int, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	if !hasAvailableThr(reqProp.available, 0) {
		return nil, nil
	}
	if res, err = tryAddXchgPreWork(node, reqProp, res); err != nil {
		return nil, err
	}
	buildSideIdx, probeSideIdx := 0, 1
	if node.InnerChildIdx == 1 {
		buildSideIdx = 1
		probeSideIdx = 1
	}
	if reqProp.output != 1 {
		// If parent of HashJoin already requires parallel, build child have to enforce BroadcastHT.
		node.GetChildXchgProps()[buildSideIdx].isBroadcastHT = true
		node.GetChildXchgProps()[probeSideIdx].available -= 1
	}

	avaBuildThr := reqProp.available - outStreamCnt
	if avaBuildThr <= 0 {
		return res, nil
	}
	// 1 means build side have to use broadcast, so available thread is less.
	avaProbeThr := reqProp.available - outStreamCnt - 1
	if avaProbeThr <= 0 {
		avaBuildThr = 1
	}

	newNode, err := node.Clone()
	if err != nil {
		return nil, err
	}

	buildReqProp := &XchgProperty{
		output:        outStreamCnt,
		isBroadcastHT: true,
		available:     avaBuildThr,
	}
	probeReqProp := &XchgProperty{
		output:    outStreamCnt,
		available: avaProbeThr,
	}
	newNode.GetChildXchgProps()[buildSideIdx] = buildReqProp
	newNode.GetChildXchgProps()[probeSideIdx] = probeReqProp

	sender := &PhysicalXchg{tp: TypeXchgSenderPassThrough}
	if err = initXchg(ctx, sender, newNode, newNode.Stats(), outStreamCnt); err != nil {
		return nil, err
	}
	receiver := &PhysicalXchg{tp: TypeXchgReceiverRandom}
	if err = initXchg(ctx, receiver, sender, newNode.Stats(), outStreamCnt); err != nil {
		return nil, err
	}

	res = append(res, receiver)
	return res, nil
}

func tryHashPartitionHJ(ctx sessionctx.Context, node PhysicalHashJoin, outStreamCnt int) (res []PhysicalHashJoin, err error) {
	return nil, errors.New("ntryHashPartitionHJ not support for now")
}

func tryAddXchgForBasicPlan(ctx sessionctx.Context, node PhysicalPlan, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	if !hasAvailableThr(reqProp.available, 0) {
		return nil, nil
	}
	if res, err = tryAddXchgPreWork(node, reqProp, res); err != nil {
		return nil, err
	}

	var newNode PhysicalPlan

	xchgOutput := ctx.GetSessionVars().ExecutorConcurrency
	if reqProp.output == 1 && hasAvailableThr(reqProp.available, xchgOutput) {
		if newNode, err = node.Clone(); err != nil {
			return nil, err
		}
		newProp := newXchgProperty()
		newProp.output = xchgOutput
		newProp.available = reqProp.available - newProp.output
		updateChildrenProp(newNode, newProp)

		sender := &PhysicalXchg{tp: TypeXchgSenderPassThrough}
		if err = initXchg(ctx, sender, newNode, newNode.Stats(), xchgOutput); err != nil {
			return nil, err
		}

		receiver := &PhysicalXchg{tp: TypeXchgReceiverRandom}
		if err = initXchg(ctx, receiver, sender, newNode.Stats(), xchgOutput); err != nil {
			return nil, err
		}

		res = append(res, receiver)
		return res, nil
	}

	if reqProp.isBroadcastHT {
		if newNode, err = node.Clone(); err != nil {
			return nil, err
		}
		newProp := newXchgProperty()
		newProp.available -= 1
		updateChildrenProp(newNode, newProp)

		sender := &PhysicalXchg{tp: TypeXchgSenderBroadcastHT}
		if err = initXchg(ctx, sender, newNode, newNode.Stats(), xchgOutput); err != nil {
			return nil, err
		}

		receiver := &PhysicalXchg{tp: TypeXchgReceiverPassThroughHT}
		if err = initXchg(ctx, receiver, sender, newNode.Stats(), xchgOutput); err != nil {
			return nil, err
		}

		res = append(res, receiver)
		return res, nil
	}

	if len(reqProp.nonOrderedPartition) != 0 {
		// if newNode, err = node.Clone(); err != nil {
		// 	return nil, err
		// }
		// updateChildrenProp(newNode, newXchgProperty())

		// sender := &PhysicalXchg{tp: TypeXchgSenderHash}
		// if err = initXchg(ctx, sender, newNode, newNode.Stats(), reqProp.output); err != nil {
		// 	return nil, err
		// }

		// receiver := &PhysicalXchg{tp: TypeXchgReceiverPassThrough}
		// if err = initXchg(ctx, receiver, sender, newNode.Stats(), reqProp.output); err != nil {
		// 	return nil, err
		// }

		// res = append(res, receiver)
		// return res, nil
		return nil, errors.New("hash partition not implemented yet")
	}
	return nil, errors.Errorf("invalid reqProp: %v", reqProp)
}

func tryAddXchgPreWork(node PhysicalPlan, reqProp *XchgProperty, res []PhysicalPlan) ([]PhysicalPlan, error) {
	if err := checkPropValidation(reqProp); err != nil {
		return nil, err
	}
	defProp := newXchgProperty()
	defProp.available = reqProp.available
	defProp.output = reqProp.output
	childrenXchgProps := make([]*XchgProperty, len(node.Children()))
	for i := 0; i < len(childrenXchgProps); i++ {
		childrenXchgProps[i] = defProp.Clone()
	}
	node.SetChildXchgProps(childrenXchgProps)

	res = append(res, node)

	return res, nil
}

func checkPropValidation(reqProp *XchgProperty) error {
	if reqProp.output < 1 {
		return errors.Errorf("output stream count not valid: %v", reqProp)
	}
	if reqProp.isBroadcastHT && len(reqProp.nonOrderedPartition) != 0 {
		return errors.New("cannot require both broadcast and non-order partition at the same time")
	}
	return nil
}

func updateChildrenProp(node PhysicalPlan, newProp *XchgProperty) {
	oldProps := node.GetChildXchgProps()
	for i := 0; i < len(oldProps); i++ {
		oldProps[i] = newProp
	}
}

func initXchg(ctx sessionctx.Context, xchg *PhysicalXchg, child PhysicalPlan, childStat *property.StatsInfo, reqOutput int) (err error) {
	// TODO: add string name.
	var tpStr string
	xchg.basePhysicalPlan = newBasePhysicalPlan(ctx, tpStr, xchg, 0)
	xchg.basePhysicalPlan.children = []PhysicalPlan{child}
	xchg.childStat = childStat
	xchg.stats = childStat

	xchg.inStreamCnt, xchg.outStreamCnt, err = getReceiverStreamCount(reqOutput, xchg.tp)
	if err != nil {
		return err
	}
	return nil
}

func isReaderNode(p PhysicalPlan) bool {
	// TODO: what if new reader is added.
	switch p.(type) {
	case *PhysicalTableReader, *PhysicalIndexReader, *PhysicalIndexLookUpReader, *PhysicalIndexMergeReader:
		return true
	default:
		return false
	}
}

func getReceiverStreamCount(reqOutput int, tp XchgType) (recvInStreamCnt int, recvOutStreamCnt int, err error) {
	switch tp {
	case TypeXchgReceiverPassThrough, TypeXchgReceiverPassThroughHT:
		return reqOutput, reqOutput, nil
	case TypeXchgReceiverRandom:
		return reqOutput, 1, nil
	case TypeXchgSenderBroadcastHT, TypeXchgSenderRandom:
		return 1, reqOutput, nil
	case TypeXchgSenderPassThrough:
		return reqOutput, reqOutput, nil
	case TypeXchgSenderHash:
		// TODO: may be not simple
		return -1, -1, errors.New("Not implemented Hash Sender for now")
	default:
		return -1, -1, errors.Errorf("unexpected xchg receiver type: %v", tp)
	}
}

func hasAvailableThr(available int, needed int) bool {
	if available > needed {
		return true
	}
	return false
}
