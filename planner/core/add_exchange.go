package core

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
)

var MaxThrNum int
var ForceUseHashPart bool
var ForceParallelHashAgg bool
var ForceParallelTopN bool

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

	HashPartition []*expression.Column
}

func (p *PhysicalXchg) Clone() (PhysicalPlan, error) {
	cloned := new(PhysicalXchg)
	base, err := p.basePhysicalPlan.cloneWithSelf(cloned)
	if err != nil {
		return nil, err
	}
	cloned.basePhysicalPlan = *base
	cloned.childStat = p.childStat
	cloned.tp = p.tp
	cloned.outStreamCnt = p.outStreamCnt
	cloned.inStreamCnt = p.inStreamCnt
	cloned.CurStreamID = p.CurStreamID
	cloned.HashPartition = cloneCols(p.HashPartition)

	chanSize := cap(p.ChkChs[0])
	chanCnt := len(p.ChkChs)
	chkChs := make([]chan *chunk.Chunk, 0, chanCnt)
	resChs := make([]chan *chunk.Chunk, 0, chanCnt)
	for i := 0; i < chanCnt; i++ {
		chkChs = append(chkChs, make(chan *chunk.Chunk, chanSize))
		resChs = append(resChs, make(chan *chunk.Chunk, chanSize))
	}
	cloned.ChkChs = chkChs
	cloned.ResChs = resChs
	return cloned, nil
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
	output        int
	available     int
	isBroadcastHT bool
	hashPartition []*expression.Column
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
		output:        prop.output,
		available:     prop.available,
		isBroadcastHT: prop.isBroadcastHT,
		hashPartition: make([]*expression.Column, len(prop.hashPartition)),
	}
	copy(res.hashPartition, prop.hashPartition)
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

func findBestXchgTask(ctx sessionctx.Context, root PhysicalPlan) (task, error) {
	initProp := newXchgProperty()
	// 1 means the main thread.
	initProp.available = MaxThrNum - 1
	return root.FindBestXchgTask(ctx, initProp)
}

func (p *PointGetPlan) FindBestXchgTask(ctx sessionctx.Context, reqProp *XchgProperty) (task, error) {
	return nil, errors.Errorf("FindBestXchgTask not implemented yet for PointGet")
}

func (p *BatchPointGetPlan) FindBestXchgTask(ctx sessionctx.Context, reqProp *XchgProperty) (task, error) {
	return nil, errors.Errorf("FindBestXchgTask not implemented yet for BatchPointGet")
}

func (p *basePhysicalPlan) FindBestXchgTask(ctx sessionctx.Context, reqProp *XchgProperty) (task, error) {
	return findBestXchgTaskForBasic(ctx, p.self, reqProp)
}

func (p *PhysicalXchg) FindBestXchgTask(ctx sessionctx.Context, reqProp *XchgProperty) (task, error) {
	var ok bool
	var sender *PhysicalXchg
	if p.IsSender() {
		sender = p
	} else {
		sender, ok = p.children[0].(*PhysicalXchg)
		if !ok {
			return nil, errors.Errorf("child of xchg receiver must be sender: %v", p.TP())
		}
	}
	consumedThr := p.inStreamCnt
	if consumedThr == 1 {
		consumedThr = p.outStreamCnt
	}
	var bestTask task
	senderChild := sender.children[0]
	childTasks := make([]task, 0, len(senderChild.Children()))
	childrenReqProps := senderChild.GetChildXchgProps()
	bestChildrenTasksFound := true
	curAvailable := reqProp.available
	for i, child := range senderChild.Children() {
		childrenReqProps[i].available = curAvailable
		bestChildTask, err := child.FindBestXchgTask(ctx, childrenReqProps[i])
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
	if !bestChildrenTasksFound {
		return invalidTask, nil
	}
	if p.IsSender() {
		bestTask = p.attach2Task(childTasks...)
	} else {
		bestSenderTask := sender.attach2Task(childTasks...)
		bestTask = p.attach2Task(bestSenderTask)
	}
	return bestTask, nil
}

func (p *PhysicalTableReader) FindBestXchgTask(ctx sessionctx.Context, reqProp *XchgProperty) (res task, err error) {
	possiblePlans, err := p.TryAddXchg(ctx, reqProp)
	if err != nil {
		return nil, err
	}
	if possiblePlans == nil {
		return invalidTask, nil
	}
	// Here we assume there is only one possible plan for xxxReader.
	// See xxxReader.TryAddXchg().
	if len(possiblePlans) != 1 {
		return nil, errors.New("Got multiple xchg plans for TableReader")
	}
	root, isRootXchg := possiblePlans[0].(*PhysicalXchg)
	if isRootXchg {
		res = &xchgTask{
			rootTask: rootTask{
				cst: p.Cost(),
				p:   root,
			},
			dop:         root.outStreamCnt,
			consumedThr: root.outStreamCnt,
		}
	} else {
		tmpTask := &rootTask{
			cst: p.Cost(),
			p:   p,
		}
		res = convertToXchgTask(ctx, tmpTask)
	}
	return res, nil
}

func findBestXchgTaskForBasic(ctx sessionctx.Context, node PhysicalPlan, reqProp *XchgProperty) (task, error) {
	possiblePlans, err := node.TryAddXchg(ctx, reqProp)
	if err != nil {
		return nil, err
	}

	var bestTask task = invalidTask
	for _, p := range possiblePlans {
		curAvailable := reqProp.available

		childTasks := make([]task, 0, len(p.Children()))
		childrenReqProps := p.GetChildXchgProps()
		// TODO: better to setup xchg's prop in initXchg()
		if _, isXchg := p.(*PhysicalXchg); isXchg {
			childrenReqProps = []*XchgProperty{reqProp.Clone()}
		}
		bestChildrenTasksFound := true
		for i, child := range p.Children() {
			childrenReqProps[i].available = curAvailable
			bestChildTask, err := child.FindBestXchgTask(ctx, childrenReqProps[i])
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
	return nil, errors.Errorf("TryAddXchg not implemented yet for BatchPointGet")
}

func (p *PhysicalLimit) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	if res, err = tryAddXchgPreWork(p, reqProp, res); err != nil {
		return nil, err
	}
	return res, nil
}

func (p *PhysicalXchg) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	if res, err = tryAddXchgPreWork(p, reqProp, res); err != nil {
		return nil, err
	}
	return res, nil
}

func (p *PhysicalSelection) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	return tryAddXchgForBasicPlan(ctx, p, reqProp)
}

func (p *PhysicalProjection) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	return tryAddXchgForBasicPlan(ctx, p, reqProp)
}

func (p *PhysicalTopN) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	// TODO: we may still have some cost model problem for TopN(rethink the attach2Task of the topmost TopN).
	if res, err = tryAddXchgForBasicPlan(ctx, p, reqProp); err != nil {
		return nil, err
	}
	var xchgIdx int
	var xchgReceiver *PhysicalXchg
	// TODO: check res, only one xchg exists.
	for ; xchgIdx < len(res); xchgIdx++ {
		var ok bool
		if xchgReceiver, ok = res[xchgIdx].(*PhysicalXchg); ok {
			break
		}
	}

	if xchgReceiver != nil {
		newNode, err := p.Clone()
		if err != nil {
			return nil, err
		}
		tmpNode, ok := newNode.(*PhysicalTopN)
		if !ok {
			return nil, errors.Errorf("cloned node must be topn: %v", tmpNode.TP())
		}
		ctx.GetSessionVars().PlanID++
		tmpNode.id = ctx.GetSessionVars().PlanID
		updateChildrenProp(newNode, newXchgProperty())
		newNode.SetChildren(xchgReceiver)
		res[xchgIdx] = newNode
		if ForceParallelTopN {
			res = res[:0]
			res = append(res, newNode)
		}
	}
	return res, nil
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
		} else if len(reqProp.hashPartition) != 0 {
			// TODO: maybe no need copy
			sender = &PhysicalXchg{tp: TypeXchgSenderHash, HashPartition: cloneCols(reqProp.hashPartition)}
			receiver = &PhysicalXchg{tp: TypeXchgReceiverPassThrough}
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

func (p *PhysicalHashAgg) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	if !hasAvailableThr(reqProp.available, 0) {
		return nil, nil
	}
	if res, err = tryAddXchgPreWork(p, reqProp, res); err != nil {
		return nil, err
	}

	// TODO: we can support multi level HashAgg later.
	if len(p.GroupByItems) == 0 {
		return nil, nil
	}
	newProp := newXchgProperty()
	newProp.hashPartition = make([]*expression.Column, 0, len(p.GroupByItems))
	for i := 0; i < len(p.GroupByItems); i++ {
		newProp.hashPartition = append(newProp.hashPartition, expression.ExtractColumns(p.GroupByItems[i])...)
	}

	if ForceParallelHashAgg {
		res = res[:0]
	}

	if reqProp.output == 1 {
		newNode, err := p.Clone()
		if err != nil {
			return nil, err
		}
		xchgOutput := ctx.GetSessionVars().ExecutorConcurrency
		newProp.output = xchgOutput
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
	} else {
		res = res[:0]
		// if len(reqProp.hashPartition) != 0 {
		//     if !isSubset(reqProp.hashPartition, newProp.hashPartition) {
		//         // TODO: log to denote why cannot generate paralle plan.
		//         return nil, nil
		//     }
		// }
		newNode, err := p.Clone()
		if err != nil {
			return nil, err
		}
		newProp.output = reqProp.output
		updateChildrenProp(newNode, newProp)
		res = append(res, newNode)
	}
	return res, nil
}

// check if cols2 is the subset of cols2
func isSubset(cols1 []*expression.Column, cols2 []*expression.Column) bool {
	for _, c2 := range cols2 {
		var found bool
		for _, c1 := range cols1 {
			if c2.UniqueID == c1.UniqueID {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (p *PhysicalHashJoin) TryAddXchg(ctx sessionctx.Context, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	// TODO: useOuterToBuild
	if p.UseOuterToBuild {
		return nil, nil
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
		// TODO: we should use cost model to check if broadcast or hash part is better.
		// For now we just use hint.
		if reqProp.isBroadcastHT {
			// If parent of HashJoin already requires parallel, build child have to enforce BroadcastHT.
			p.GetChildXchgProps()[buildSideIdx].isBroadcastHT = true
			p.IsBroadcastHJ = true
		} else {
			buildReqProp, probeReqProp := getReqPropsForHashPartition(p, reqProp.output, probeSideIdx, probeSideIdx)
			// if !isSubset(reqProp.hashPartition, buildReqProp.hashPartition) || !isSubset(reqProp.hashPartition, probeReqProp.hashPartition) {
			//     return nil, nil
			// }
			p.GetChildXchgProps()[buildSideIdx] = buildReqProp
			p.GetChildXchgProps()[probeSideIdx] = probeReqProp
		}
	} else {
		outStreamCnt := ctx.GetSessionVars().ExecutorConcurrency
		var res1 []PhysicalPlan

		if !ForceUseHashPart {
			if res1, err = tryBroadcastHJ(ctx, p, outStreamCnt, reqProp, buildSideIdx, probeSideIdx); err != nil {
				return nil, err
			}
		} else {
			if res1, err = tryHashPartitionHJ(ctx, p, outStreamCnt, reqProp, buildSideIdx, probeSideIdx); err != nil {
				return nil, err
			}
		}
		res = append(res, res1...)
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
		return nil, errors.Errorf("Cloned node must be HashJoin: %v", newHJNode.TP())
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

func tryHashPartitionHJ(ctx sessionctx.Context, node *PhysicalHashJoin, outStreamCnt int, reqProp *XchgProperty, buildSideIdx int, probeSideIdx int) (res []PhysicalPlan, err error) {
	newNode, err := node.Clone()
	if err != nil {
		return nil, err
	}

	buildReqProp, probeReqProp := getReqPropsForHashPartition(node, outStreamCnt, probeSideIdx, probeSideIdx)
	// if !isSubset(reqProp.hashPartition, buildReqProp.hashPartition) || !isSubset(reqProp.hashPartition, probeReqProp.hashPartition) {
	//     return nil, nil
	// }
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

func getReqPropsForHashPartition(node *PhysicalHashJoin, outStreamCnt int, buildSideIdx int, probeSideIdx int) (buildReqProp *XchgProperty, probeReqProp *XchgProperty) {
	var buildKeys []*expression.Column
	var probeKeys []*expression.Column
	if buildSideIdx == 0 {
		buildKeys = node.LeftJoinKeys
		probeKeys = node.RightJoinKeys
	} else {
		buildKeys = node.RightJoinKeys
		probeKeys = node.LeftJoinKeys
	}
	buildReqProp = &XchgProperty{
		output:        outStreamCnt,
		hashPartition: cloneCols(buildKeys),
	}
	probeReqProp = &XchgProperty{
		output:        outStreamCnt,
		hashPartition: cloneCols(probeKeys),
	}
	return buildReqProp, probeReqProp
}

func tryAddXchgForBasicPlan(ctx sessionctx.Context, node PhysicalPlan, reqProp *XchgProperty) (res []PhysicalPlan, err error) {
	if !hasAvailableThr(reqProp.available, 0) {
		return nil, nil
	}
	if res, err = tryAddXchgPreWork(node, reqProp, res); err != nil {
		return nil, err
	}

	var newNode PhysicalPlan

	if reqProp.output == 1 {
		xchgOutput := ctx.GetSessionVars().ExecutorConcurrency
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
		xchgOutput := reqProp.output
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

	if len(reqProp.hashPartition) != 0 {
		xchgOutput := reqProp.output
		if newNode, err = node.Clone(); err != nil {
			return nil, err
		}
		newProp := newXchgProperty()
		newProp.output = xchgOutput
		updateChildrenProp(newNode, newProp)

		sender := &PhysicalXchg{tp: TypeXchgSenderHash, HashPartition: cloneCols(reqProp.hashPartition)}
		if err = initXchg(ctx, sender, newNode, newNode.Stats(), xchgOutput); err != nil {
			return nil, err
		}
		receiver := &PhysicalXchg{tp: TypeXchgReceiverPassThrough}
		if err = initXchg(ctx, receiver, sender, newNode.Stats(), xchgOutput); err != nil {
			return nil, err
		}
		setupChans(sender, receiver, xchgOutput)

		res = append(res, receiver)
		return res, nil
	}
	return res, nil
}

func tryAddXchgPreWork(node PhysicalPlan, reqProp *XchgProperty, res []PhysicalPlan) ([]PhysicalPlan, error) {
	if err := checkPropValidation(reqProp); err != nil {
		return nil, err
	}
	childrenXchgProps := make([]*XchgProperty, len(node.Children()))
	for i := 0; i < len(childrenXchgProps); i++ {
		childrenXchgProps[i] = reqProp.Clone()
	}
	node.SetChildXchgProps(childrenXchgProps)

	res = append(res, node)

	return res, nil
}

func checkPropValidation(reqProp *XchgProperty) error {
	if reqProp.output < 1 {
		return errors.Errorf("output stream count not valid: %v", reqProp)
	}
	if reqProp.isBroadcastHT && len(reqProp.hashPartition) != 0 {
		return errors.New("cannot require both broadcast and non-order partition at the same time")
	}
	if reqProp.isBroadcastHT && reqProp.output == 1 {
		return errors.New("cannot require broadcastHT when output is 1")
	}
	if len(reqProp.hashPartition) != 0 && reqProp.output == 1 {
		return errors.New("cannot require hash partition when output is 1")
	}
	return nil
}

func updateChildrenProp(node PhysicalPlan, newProp *XchgProperty) {
	oldProps := node.GetChildXchgProps()
	for i := 0; i < len(oldProps); i++ {
		oldProps[i] = newProp
	}
}

func initXchg(ctx sessionctx.Context, xchg *PhysicalXchg, child PhysicalPlan, childStat *property.StatsInfo, streamCnt int) (err error) {
	// TODO: add string name.
	var tpStr string
	xchg.basePhysicalPlan = newBasePhysicalPlan(ctx, tpStr, xchg, 0)
	xchg.basePhysicalPlan.children = []PhysicalPlan{child}
	xchg.childStat = childStat
	xchg.stats = childStat

	xchg.inStreamCnt, xchg.outStreamCnt, err = getReceiverStreamCount(streamCnt, xchg.tp)
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
	case TypeXchgReceiverRandom:
		return reqOutput, 1, nil
	case TypeXchgSenderPassThrough:
		return reqOutput, 1, nil
	case TypeXchgReceiverPassThrough, TypeXchgReceiverPassThroughHT:
		return 1, reqOutput, nil
	case TypeXchgSenderBroadcastHT, TypeXchgSenderRandom:
		return 1, reqOutput, nil
	case TypeXchgSenderHash:
		return 1, reqOutput, nil
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
