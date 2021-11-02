package distsql

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

const (
	startPagingSize uint64 = 32
	endPageSize            = startPagingSize * 128
)

// pagingResult wraps selectResult and separate requests into pages.
type pagingResult struct {
	label          string
	sr             *selectResult
	dag            *tipb.DAGRequest
	currPageSize   uint64
	currPageLoaded uint64
}

func (p *pagingResult) firstPage(kvReq *kv.Request) error {
	p.currPageSize = startPagingSize
	err := p.dag.Unmarshal(kvReq.Data)
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
		kvReq.Data, err = p.dag.Marshal()
		if err != nil {
			return err
		}
	} else {
		return errors.New("unexpected executor with length 0")
	}
	return nil
}

// NextRaw gets the next raw result.
func (p *pagingResult) NextRaw(context.Context) ([]byte, error) {
	return nil, nil
}

// Next reads the data into chunk.
func (p *pagingResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	err := p.sr.Next(ctx, chk)
	if err != nil {
		return err
	}
	if p.sr.selectResp == nil {
		p.currPageLoaded += uint64(chk.NumRows())
		if p.currPageLoaded == p.currPageSize {

		}
	}
	return nil
}

// Close closes the iterator.
func (p *pagingResult) Close() error {
	return p.sr.Close()
}
