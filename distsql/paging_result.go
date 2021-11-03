package distsql

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	startPagingSize uint64 = 2
	endPageSize            = startPagingSize * 128
)

// pagingResult wraps selectResult and separate requests into pages.
type pagingResult struct {
	label          string
	ctx            sessionctx.Context
	kvReq          *kv.Request
	fieldTypes     []*types.FieldType
	fb             *statistics.QueryFeedback
	sr             *selectResult
	dag            *tipb.DAGRequest
	currPageSize   uint64
	currPageLoaded uint64
	err            error
	start          time.Time
}

func (p *pagingResult) firstPage() error {
	p.start = time.Now()
	p.currPageSize = startPagingSize
	p.dag = &tipb.DAGRequest{}
	err := p.dag.Unmarshal(p.kvReq.Data)
	if err != nil {
		return err
	}
	if len(p.dag.Executors) > 0 {
		limit := &tipb.Executor{
			Tp: tipb.ExecType_TypeLimit,
			Limit: &tipb.Limit{
				Limit: p.currPageSize,
				Child: p.dag.Executors[len(p.dag.Executors)-1],
			},
		}
		p.dag.Executors = append(p.dag.Executors, limit)
		p.kvReq.Data, err = p.dag.Marshal()
		if err != nil {
			return err
		}
	} else {
		return errors.New("unexpected executor with length 0")
	}
	return nil
}

func (p *pagingResult) nextPage(ctx context.Context) {
	if p.currPageSize < endPageSize {
		p.currPageSize *= 2
	}
	e := p.dag.Executors[len(p.dag.Executors)-1]
	if e.Tp != tipb.ExecType_TypeLimit {
		p.err = fmt.Errorf("unexpected executor type %d", e.Tp)
		return
		e.Limit.Limit = p.currPageSize
	}
	p.kvReq.Data, p.err = p.dag.Marshal()
	for _, respRange := range p.sr.respRanges {
		var key []byte
		if p.kvReq.Desc {
			key = respRange.Start
		} else {
			key = respRange.End
		}
		i := sort.Search(len(p.kvReq.KeyRanges), func(i int) bool {
			return p.kvReq.KeyRanges[i].StartKey.Cmp(key)+
				p.kvReq.KeyRanges[i].EndKey.Cmp(key) == 0
		})
		if p.kvReq.Desc {
			p.kvReq.KeyRanges[i].EndKey = key
			if i < len(p.kvReq.KeyRanges)-1 {
				p.kvReq.KeyRanges = p.kvReq.KeyRanges[:i+1]
			}
		} else {
			p.kvReq.KeyRanges[i].StartKey = key
			if i > 0 {
				p.kvReq.KeyRanges = p.kvReq.KeyRanges[i-1:]
			}
		}

		if len(p.kvReq.KeyRanges) == 0 {
			return
		}
	}
	if p.err != nil {
		return
	}
	sr, err := Select(ctx, p.ctx, p.kvReq, p.fieldTypes, p.fb)
	if err != nil {
		p.err = err
		return
	}
	if r, ok := sr.(*selectResult); !ok {
		p.err = fmt.Errorf("unexpected result type %T", sr)
		return
	} else {
		p.sr = r
	}
	return
}

// NextRaw gets the next raw result.
func (p *pagingResult) NextRaw(ctx context.Context) ([]byte, error) {
	if p.err != nil {
		return nil, p.err
	}
	data, err := p.sr.NextRaw(ctx)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (p *pagingResult) next(ctx context.Context, chk *chunk.Chunk) error {
	if p.sr.selectResp == nil || p.sr.respChkIdx == len(p.sr.selectResp.Chunks) {
		err := p.sr.fetchResp(ctx)
		if err != nil {
			return err
		}
		if p.sr.selectResp == nil {
			return nil
		}
	}
	// TODO(Shenghui Wu): add metrics
	switch p.sr.selectResp.GetEncodeType() {
	case tipb.EncodeType_TypeDefault:
		return p.sr.readFromDefault(ctx, chk)
	case tipb.EncodeType_TypeChunk:
		return p.sr.readFromChunk(ctx, chk)
	}
	return errors.Errorf("unsupported encode type:%v", p.sr.encodeType)
}

// Next reads the data into chunk.
func (p *pagingResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	if p.err != nil {
		return p.err
	}
	chk.Reset()
	for !chk.IsFull() {
		err := p.next(ctx, chk)
		if err != nil {
			return err
		}
		if p.sr.selectResp == nil {
			p.sr.Close()
			p.currPageLoaded += uint64(chk.NumRows())
			if p.currPageLoaded == p.currPageSize {
				p.nextPage(ctx)
			}
		}
	}
	return nil
}

// Close closes the iterator.
func (p *pagingResult) Close() error {
	if p.sr != nil {
		return p.sr.Close()
	}
	return nil
}
