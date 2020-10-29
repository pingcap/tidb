package mocktikv

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// TODO: Let the join support conditions / multiple keys
type join struct {
	*tipb.Join

	hashMap map[string][][][]byte

	buildKey *expression.Column
	probeKey *expression.Column

	buildSideIdx int64

	built bool

	buildChild executor
	probeChild executor

	idx          int
	reservedRows [][][]byte
}

func (e *join) buildHashTable(ctx context.Context) error {
	for {
		row, err := e.buildChild.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("join build", zap.Int("len", len(row)))
		if row == nil {
			return nil
		}
		keyCol := row[e.buildKey.Index]
		if rowSet, ok := e.hashMap[string(keyCol)]; ok {
			rowSet = append(rowSet, row)
			e.hashMap[string(keyCol)] = rowSet
		} else {
			e.hashMap[string(keyCol)] = [][][]byte{row}
		}
	}
}

func (e *join) fetchRows(ctx context.Context) (bool, error) {
	row, err := e.probeChild.Next(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	log.Info("join probe", zap.Int("len", len(row)))
	if row == nil {
		return true, nil
	}
	e.idx = 0
	e.reservedRows = make([][][]byte, 0)
	keyCol := row[e.probeKey.Index]
	if rowSet, ok := e.hashMap[string(keyCol)]; ok {
		for _, matched := range rowSet {
			newRow := make([][]byte, 0)
			if e.buildSideIdx == 0 {
				newRow = append(newRow, matched...)
				newRow = append(newRow, row...)
			} else {
				newRow = append(newRow, row...)
				newRow = append(newRow, matched...)
			}
			log.Info("matched key", zap.ByteStrings("new row", newRow))
			e.reservedRows = append(e.reservedRows, newRow)
		}
	}
	return false, nil
}

func (e *join) Next(ctx context.Context) ([][]byte, error) {
	if !e.built {
		err := e.buildHashTable(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.built = true
	}
	for {
		if e.idx < len(e.reservedRows) {
			idx := e.idx
			e.idx++
			return e.reservedRows[idx], nil
		}
		eof, err := e.fetchRows(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if eof {
			return nil, nil
		}
	}
}

func (e *join) SetSrcExec(executor) {}

func (e *join) GetSrcExec() executor {
	return nil
}

func (e *join) Counts() []int64 {
	return nil
}

func (e *join) ExecDetails() []*execDetail {
	return nil
}

func (e *join) ResetCounts() {}

func (e *join) Cursor() ([]byte, bool) {
	return nil, false
}
