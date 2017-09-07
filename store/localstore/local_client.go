package localstore

import (
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

type dbClient struct {
	store      *dbStore
	regionInfo []*regionInfo
}

func (c *dbClient) Send(ctx goctx.Context, req *kv.Request) kv.Response {
	it := &response{
		client:      c,
		concurrency: req.Concurrency,
	}
	it.tasks = buildRegionTasks(c, req)
	if len(it.tasks) == 0 {
		// Empty range doesn't produce any task.
		it.finished = true
		return it
	}
	if it.concurrency > len(it.tasks) {
		it.concurrency = len(it.tasks)
	} else if it.concurrency <= 0 {
		it.concurrency = 1
	}
	it.taskChan = make(chan *task, it.concurrency)
	it.errChan = make(chan error, it.concurrency)
	it.respChan = make(chan *regionResponse, it.concurrency)
	it.run()
	return it
}

func (c *dbClient) IsRequestTypeSupported(reqType, subType int64) bool {
	switch reqType {
	case kv.ReqTypeSelect, kv.ReqTypeIndex:
		switch subType {
		case kv.ReqSubTypeGroupBy, kv.ReqSubTypeBasic, kv.ReqSubTypeTopN:
			return true
		default:
			return supportExpr(tipb.ExprType(subType))
		}
	}
	return false
}

func supportExpr(exprType tipb.ExprType) bool {
	switch exprType {
	// data type.
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64,
		tipb.ExprType_Float32, tipb.ExprType_Float64, tipb.ExprType_String,
		tipb.ExprType_Bytes, tipb.ExprType_MysqlDuration, tipb.ExprType_MysqlDecimal,
		tipb.ExprType_MysqlTime, tipb.ExprType_ColumnRef:
		return true
	// logic operators.
	case tipb.ExprType_And, tipb.ExprType_Or, tipb.ExprType_Not, tipb.ExprType_Xor:
		return true
	// compare operators.
	case tipb.ExprType_LT, tipb.ExprType_LE, tipb.ExprType_EQ, tipb.ExprType_NE,
		tipb.ExprType_GE, tipb.ExprType_GT, tipb.ExprType_NullEQ,
		tipb.ExprType_In, tipb.ExprType_ValueList, tipb.ExprType_Like:
		return true
	// arithmetic operators.
	case tipb.ExprType_Plus, tipb.ExprType_Div, tipb.ExprType_Minus,
		tipb.ExprType_Mul, tipb.ExprType_IntDiv, tipb.ExprType_Mod:
		return true
	// aggregate functions.
	case tipb.ExprType_Count, tipb.ExprType_First, tipb.ExprType_Sum,
		tipb.ExprType_Avg, tipb.ExprType_Max, tipb.ExprType_Min:
		return true
	// bitwise operators.
	case tipb.ExprType_BitAnd, tipb.ExprType_BitOr, tipb.ExprType_BitXor, tipb.ExprType_BitNeg:
		return true
	// control functions
	case tipb.ExprType_Case, tipb.ExprType_If, tipb.ExprType_IfNull, tipb.ExprType_NullIf:
		return true
	// other functions
	case tipb.ExprType_Coalesce, tipb.ExprType_IsNull:
		return true
	case tipb.ExprType_JsonType, tipb.ExprType_JsonExtract, tipb.ExprType_JsonUnquote, tipb.ExprType_JsonValid,
		tipb.ExprType_JsonObject, tipb.ExprType_JsonArray, tipb.ExprType_JsonMerge, tipb.ExprType_JsonSet,
		tipb.ExprType_JsonInsert, tipb.ExprType_JsonReplace, tipb.ExprType_JsonRemove, tipb.ExprType_JsonContains:
		return true
	case kv.ReqSubTypeDesc:
		return true
	default:
		return false
	}
}

func (c *dbClient) updateRegionInfo() {
	c.regionInfo = c.store.pd.GetRegionInfo()
}

type response struct {
	client      *dbClient
	reqSent     int
	respGot     int
	concurrency int
	tasks       []*task
	responses   []*regionResponse
	taskChan    chan *task
	respChan    chan *regionResponse
	errChan     chan error
	finished    bool
}

type task struct {
	request *regionRequest
	region  *localRegion
}

func (it *response) Next() (resp *coprocessor.Response, err error) {
	if it.finished {
		return nil, nil
	}
	var regionResp *regionResponse
	select {
	case regionResp = <-it.respChan:
	case err = <-it.errChan:
	}
	if err != nil {
		it.Close()
		return nil, errors.Trace(err)
	}
	if len(regionResp.newStartKey) != 0 {
		it.client.updateRegionInfo()
		retryTasks := it.createRetryTasks(regionResp)
		it.tasks = append(it.tasks, retryTasks...)
	}
	if it.reqSent < len(it.tasks) {
		it.taskChan <- it.tasks[it.reqSent]
		it.reqSent++
	}
	it.respGot++
	if it.reqSent == len(it.tasks) && it.respGot == it.reqSent {
		it.Close()
	}
	if regionResp.data != nil {
		return &coprocessor.Response{
			Data: regionResp.data,
		}, nil
	}
	return nil, nil
}

func (it *response) createRetryTasks(resp *regionResponse) []*task {
	return nil
}

func buildRegionTasks(client *dbClient, req *kv.Request) (tasks []*task) {
	infoCursor := 0
	rangeCursor := 0
	var regionReq *regionRequest
	infos := client.regionInfo
	for rangeCursor < len(req.KeyRanges) && infoCursor < len(infos) {
		info := infos[infoCursor]
		ran := req.KeyRanges[rangeCursor]

		rangeOnLeft := ran.EndKey.Cmp(info.startKey) <= 0
		rangeOnRight := info.endKey.Cmp(ran.StartKey) <= 0
		noDataOnRegion := rangeOnLeft || rangeOnRight
		if noDataOnRegion {
			if rangeOnLeft {
				rangeCursor++
			} else {
				infoCursor++
			}
		} else {
			regionReq = &regionRequest{
				Tp:       req.Tp,
				startKey: info.startKey,
				endKey:   info.endKey,
				data:     req.Data,
				ranges:   req.KeyRanges,
			}
			task := &task{
				region:  info.rs,
				request: regionReq,
			}
			tasks = append(tasks, task)
			infoCursor++
		}
	}
	if req.Desc {
		for i := 0; i < len(tasks)/2; i++ {
			j := len(tasks) - i - 1
			tasks[i], tasks[j] = tasks[j], tasks[i]
		}
	}
	return
}

func (it *response) Close() error {
	// Make goroutines quit.
	if it.finished {
		return nil
	}
	close(it.taskChan)
	it.finished = true
	return nil
}

func (it *response) run() {
	for i := 0; i < it.concurrency; i++ {
		go func() {
			for task := range it.taskChan {
				resp, err := task.region.Handle(task.request)
				if err != nil {
					it.errChan <- err
					break
				}
				it.respChan <- resp
			}
		}()
		it.taskChan <- it.tasks[i]
		it.reqSent++
	}
}
