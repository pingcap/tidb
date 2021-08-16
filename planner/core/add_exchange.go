package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
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
	// TODO: change Random -> RandomMerge.
	TypeXchgReceiverRandom        XchgType = 0
	TypeXchgReceiverPassThrough   XchgType = 1
	TypeXchgReceiverPassThroughHT XchgType = 2

	TypeXchgSenderPassThrough XchgType = 3
	TypeXchgSenderBroadcastHT XchgType = 4
	TypeXchgSenderHash        XchgType = 5
	TypeXchgSenderRandom      XchgType = 6
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
	// Record the ID of cur stream.
	CurStreamID int
	ChkChs      []chan *chunk.Chunk
	ResChs      []chan *chunk.Chunk
}

// TODO: just use Cap initial.
func (xchg *PhysicalXchg) Tp() XchgType {
	return xchg.tp
}

func (xchg *PhysicalXchg) OutStreamCnt() int {
	return xchg.outStreamCnt
}

func (xchg *PhysicalXchg) InStreamCnt() int {
	return xchg.inStreamCnt
}

func (p *PhysicalXchg) IsSender() bool {
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
		curAvailable := reqProp.available
		xchgReceiver, isXchg := planToFind.(*PhysicalXchg)
		if isXchg {
			planToFind = xchgReceiver.Children()[0].Children()[0]
			curAvailable -= xchgReceiver.inStreamCnt
		}

		// Stop find best task recursively when got xxxReader.
		if isReaderNode(planToFind) {
			tmpTask := &rootTask{
				cst: planToFind.Cost(),
				p:   p,
			}
			if p != planToFind {
				// bestTask = p.attach2Task(convertToXchgTask(ctx, bestTask))
				bestTask = &xchgTask{
					rootTask:    *tmpTask,
					dop:         xchgReceiver.inStreamCnt,
					consumedThr: xchgReceiver.inStreamCnt,
				}
			} else {
				bestTask = tmpTask
			}
			// Here we assume there is only one possible plan for xxxReader.
			// See xxxReader.TryAddXchg().
			continue
		}

		childTasks := make([]task, 0, len(planToFind.Children()))
		childrenXchgProps := planToFind.GetChildXchgProps()
		bestChildrenTasksFound := true
		for i, child := range planToFind.Children() {
			childrenXchgProps[i].available = curAvailable
			bestChildTask, err := findBestXchgTask(ctx, child, childrenXchgProps[i])
			if err != nil {
				return invalidTask, err
			}
			if bestChildTask.invalid() {
				bestChildrenTasksFound = false
				break
			}
			childTasks = append(childTasks, bestChildTask)
			tmpTask, ok := bestChildTask.(*xchgTask)
			if !ok {
				return nil, errors.New("childTask is not xchgTask")
			}
			curAvailable -= tmpTask.consumedThr
		}
		if bestChildrenTasksFound {
			curTask := p.attach2Task(childTasks...)

			if curTask.cost() < bestTask.cost() || (bestTask.invalid() && !curTask.invalid()) {
				bestTask = curTask
			}
		}
	}
	return convertToXchgTask(ctx, bestTask), nil
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
		setupChans(sender, receiver, reqProp.output)

		res = append(res, receiver)
	}
	return res, nil
}

func (p *PhysicalHashJoin) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	// TODO: useOuterToBuild
	if p.UseOuterToBuild {
		return nil, errors.New("TryAddXchg not support UseOuterToBuild for now")
	}

	if !hasAvailableThr(reqProp.available, 0) {
		return nil, nil
	}

	buildSideIdx, probeSideIdx := 0, 1
	if p.InnerChildIdx == 1 {
		buildSideIdx = 1
		probeSideIdx = 0
	}

	if res, err = tryAddXchgPreWork(p, reqProp, res); err != nil {
		return nil, err
	}
	if reqProp.output != 1 {
		// If parent of HashJoin already requires parallel, build child have to enforce BroadcastHT.
		p.GetChildXchgProps()[buildSideIdx].isBroadcastHT = true
		p.IsBroadcastHJ = true
	}

	outStreamCnt := reqProp.output
	if outStreamCnt == 1 {
		outStreamCnt = ctx.GetSessionVars().ExecutorConcurrency
	}
	res1, err := tryBroadcastHJ(ctx, p, outStreamCnt, reqProp, buildSideIdx, probeSideIdx)
	if err != nil {
		return nil, err
	}
	res = append(res, res1...)
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

func tryBroadcastHJ(ctx sessionctx.Context, node *PhysicalHashJoin, outStreamCnt int, reqProp *XchgProperty, buildSideIdx int, probeSideIdx int) (res []PhysicalPlan, err error) {
	newNode, err := node.Clone()
	if err != nil {
		return nil, err
	}
	newHJNode, ok := newNode.(*PhysicalHashJoin)
	if !ok {
		return nil, errors.New("newNode must be HashJoin")
	}
	newHJNode.IsBroadcastHJ = true

	buildReqProp := &XchgProperty{
		output:        outStreamCnt,
		isBroadcastHT: true,
	}
	probeReqProp := &XchgProperty{
		output: outStreamCnt,
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
	setupChans(sender, receiver, outStreamCnt)

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
	if reqProp.output == 1 {
		if !hasAvailableThr(reqProp.available, xchgOutput) {
			return res, nil
		}
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
		setupChans(sender, receiver, xchgOutput)

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
		setupChans(sender, receiver, xchgOutput)

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

func setupChans(xchg1 *PhysicalXchg, xchg2 *PhysicalXchg, chanCnt int) {
	// TODO: session var to control this.
	chanSize := 5
	chkChs := make([]chan *chunk.Chunk, 0, chanCnt)
	resChs := make([]chan *chunk.Chunk, 0, chanCnt)
	for i := 0; i < chanCnt; i++ {
		chkChs = append(chkChs, make(chan *chunk.Chunk, chanSize))
		resChs = append(resChs, make(chan *chunk.Chunk, chanSize))
	}
	xchg1.ChkChs = chkChs
	xchg1.ResChs = resChs
	xchg2.ChkChs = chkChs
	xchg2.ResChs = resChs
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
		return 1, reqOutput, nil
	case TypeXchgReceiverRandom:
		return reqOutput, 1, nil
	case TypeXchgSenderBroadcastHT, TypeXchgSenderRandom:
		return 1, reqOutput, nil
	case TypeXchgSenderPassThrough:
		return reqOutput, 1, nil
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
