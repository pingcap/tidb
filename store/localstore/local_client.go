package localstore

import (
	"io"

	"github.com/pingcap/tidb/kv"
)

type localClient struct {
	store      *dbStore
	regionInfo []*regionInfo
}

func (c *localClient) Send(req *kv.Request) kv.ResponseIterator {
	it := c.buildRespIterator(req)
	it.run()
	return it
}

func (c *localClient) SupportRequestType(reqType, subType int64) bool {
	return reqType == kv.ReqTypeSelect
}

func (c *localClient) buildRespIterator(req *kv.Request) *respIterator {
	it := &respIterator{
		client:      c,
		concurrency: req.Concurrency,
	}
	it.tasks = buildRegionTasks(c, req)
	if it.concurrency > len(it.tasks) {
		it.concurrency = len(it.tasks)
	}
	it.taskChan = make(chan *task, it.concurrency)
	it.errChan = make(chan error, it.concurrency)
	it.respChan = make(chan *regionResponse, it.concurrency)
	return it
}

func (c *localClient) buildRegionTasks(req *kv.Request) []*task {
	var tasks []*task
	infoCursor := 0
	rangeCursor := 0
	var regionReq *regionRequest
	for {
		if rangeCursor >= len(req.KeyRanges) {
			break
		}
		info := c.regionInfo[infoCursor]
		ran := req.KeyRanges[rangeCursor]
		if containsRange(info, ran) {
			regionReq = &regionRequest{
				Tp:       kv.ReqTypeSelect,
				startKey: info.startKey,
				endKey:   info.endKey,
				data:     req.Data,
			}
			task := &task{
				region:  info.rs,
				request: regionReq,
			}
			tasks = append(tasks, task)
			infoCursor++
		} else {
			rangeCursor++
		}
	}

	return tasks
}

func containsRange(info *regionInfo, keyRange kv.KeyRange) bool {
	return info.startKey.Cmp(keyRange.EndKey) < 0 && info.endKey.Cmp(keyRange.StartKey) > 0
}

func (c *localClient) updateRegionInfo() {
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

type respIterator struct {
	client      *localClient
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
	region  *localRS
}

func (it *respIterator) Next() (resp io.ReadCloser, err error) {
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
		return nil, err
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

func (it *respIterator) createRetryTasks(resp *regionResponse) []*task {
	return nil
}

func buildRegionTasks(client *localClient, req *kv.Request) (tasks []*task) {
	infoCursor := 0
	rangeCursor := 0
	var regionReq *regionRequest
	infos := client.regionInfo
	for {
		if rangeCursor >= len(req.KeyRanges) || infoCursor >= len(infos) {
			break
		}
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
				Tp:       kv.ReqTypeSelect,
				startKey: info.startKey,
				endKey:   info.endKey,
				data:     req.Data,
			}
			task := &task{
				region:  info.rs,
				request: regionReq,
			}
			tasks = append(tasks, task)
			infoCursor++
		}
	}
	return
}

func (it *respIterator) Close() error {
	// Make goroutines quit.
	if it.finished {
		return nil
	}
	close(it.taskChan)
	it.finished = true
	return nil
}

func (it *respIterator) run() {
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
