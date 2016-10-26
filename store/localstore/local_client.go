package localstore

import (
	"io"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tipb/go-tipb"
)

type dbClient struct {
	store      *dbStore
	regionInfo []*regionInfo
}

func (c *dbClient) Send(req *kv.Request) kv.Response {
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

func (c *dbClient) SupportRequestType(reqType, subType int64) bool {
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
	case tipb.ExprType_Null, tipb.ExprType_Int64, tipb.ExprType_Uint64, tipb.ExprType_Float32,
		tipb.ExprType_Float64, tipb.ExprType_String, tipb.ExprType_Bytes,
		tipb.ExprType_MysqlDuration, tipb.ExprType_MysqlDecimal, tipb.ExprType_MysqlTime,
		tipb.ExprType_ColumnRef,
		tipb.ExprType_And, tipb.ExprType_Or,
		tipb.ExprType_LT, tipb.ExprType_LE, tipb.ExprType_EQ, tipb.ExprType_NE,
		tipb.ExprType_GE, tipb.ExprType_GT, tipb.ExprType_NullEQ,
		tipb.ExprType_In, tipb.ExprType_ValueList,
		tipb.ExprType_Not,
		tipb.ExprType_Like:
		return true
	case tipb.ExprType_Plus, tipb.ExprType_Div, tipb.ExprType_Minus, tipb.ExprType_Mul,
		tipb.ExprType_IntDiv, tipb.ExprType_Mod:
		return true
	case tipb.ExprType_Count, tipb.ExprType_First, tipb.ExprType_Sum, tipb.ExprType_Avg, tipb.ExprType_Max, tipb.ExprType_Min:
		return true
	case tipb.ExprType_Case, tipb.ExprType_Coalesce:
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

type localResponseReader struct {
	s []byte
	i int64
}

func (r *localResponseReader) Read(b []byte) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}
	if r.i >= int64(len(r.s)) {
		return 0, io.EOF
	}
	n = copy(b, r.s[r.i:])
	r.i += int64(n)
	return
}

func (r *localResponseReader) Close() error {
	r.i = int64(len(r.s))
	return nil
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

func (it *response) Next() (resp io.ReadCloser, err error) {
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
	return &localResponseReader{s: regionResp.data}, nil
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
