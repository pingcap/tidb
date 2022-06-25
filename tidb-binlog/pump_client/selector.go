// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"sync"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tipb/go-binlog"
	"go.uber.org/zap"
)

const (
	// Range means range strategy.
	Range = "range"

	// Hash means hash strategy.
	Hash = "hash"

	// Score means choose pump by it's score.
	Score = "score"

	// LocalUnix means will only use the local pump by unix socket.
	LocalUnix = "local unix"
)

var (
	// tsMap saves the map of start_ts with pump when send prepare binlog.
	// And Commit binlog should send to the same pump.
	tsMap = make(map[int64]*PumpStatus)

	selectorLock sync.RWMutex
)

// PumpSelector selects pump for sending binlog.
type PumpSelector interface {
	// SetPumps set pumps to be selected.
	SetPumps([]*PumpStatus)

	// Select returns a situable pump. Tips: should call this function only one time for commit/rollback binlog.
	Select(binlog *pb.Binlog, retryTime int) *PumpStatus

	// Feedback set the corresponding relations between startTS and pump.
	Feedback(startTS int64, binlogType pb.BinlogType, pump *PumpStatus)
}

// HashSelector select a pump by hash.
type HashSelector struct {
	// the pumps to be selected.
	Pumps []*PumpStatus
}

// NewHashSelector returns a new HashSelector.
func NewHashSelector() PumpSelector {
	return &HashSelector{
		Pumps: make([]*PumpStatus, 0, 10),
	}
}

// SetPumps implement PumpSelector.SetPumps.
func (h *HashSelector) SetPumps(pumps []*PumpStatus) {
	selectorLock.Lock()
	h.Pumps = pumps
	selectorLock.Unlock()
}

// Select implement PumpSelector.Select.
func (h *HashSelector) Select(binlog *pb.Binlog, retryTime int) *PumpStatus {
	// TODO: use status' label to match suitable pump.
	selectorLock.Lock()
	defer selectorLock.Unlock()

	if binlog.Tp != pb.BinlogType_Prewrite {
		// binlog is commit binlog or rollback binlog, choose the same pump by start ts map.
		if pump, ok := tsMap[binlog.StartTs]; ok {
			return pump
		}

		// this should never happened
		log.Warn("[pumps client] binlog don't have matched prewrite binlog", zap.Stringer("binlog type", binlog.Tp), zap.Int64("startTs", binlog.StartTs))
		return nil
	}

	if len(h.Pumps) == 0 {
		return nil
	}
	i := (binlog.StartTs + int64(retryTime)) % int64(len(h.Pumps))
	pump := h.Pumps[int(i)]
	return pump
}

// Feedback implement PumpSelector.Feedback
func (h *HashSelector) Feedback(startTS int64, binlogType pb.BinlogType, pump *PumpStatus) {
	maintainTSMap(startTS, binlogType, pump)
}

// RangeSelector select a pump by range.
type RangeSelector struct {
	// Offset saves the offset in Pumps.
	Offset int

	// the pumps to be selected.
	Pumps []*PumpStatus
}

// NewRangeSelector returns a new ScoreSelector.
func NewRangeSelector() PumpSelector {
	return &RangeSelector{
		Offset: 0,
		Pumps:  make([]*PumpStatus, 0, 10),
	}
}

// SetPumps implement PumpSelector.SetPumps.
func (r *RangeSelector) SetPumps(pumps []*PumpStatus) {
	selectorLock.Lock()
	r.Pumps = pumps
	r.Offset = 0
	selectorLock.Unlock()
}

// Select implement PumpSelector.Select.
func (r *RangeSelector) Select(binlog *pb.Binlog, retryTime int) *PumpStatus {
	// TODO: use status' label to match suitable pump.
	selectorLock.Lock()
	defer selectorLock.Unlock()

	if binlog.Tp != pb.BinlogType_Prewrite {
		// binlog is commit binlog or rollback binlog, choose the same pump by start ts map.
		if pump, ok := tsMap[binlog.StartTs]; ok {
			return pump
		}

		// this should never happened
		log.Warn("[pumps client] binlog don't have matched prewrite binlog", zap.Stringer("binlog type", binlog.Tp), zap.Int64("startTs", binlog.StartTs))
		return nil
	}

	if len(r.Pumps) == 0 {
		return nil
	}

	if r.Offset >= len(r.Pumps) {
		r.Offset = 0
	}

	pump := r.Pumps[r.Offset]

	r.Offset++
	return pump
}

// Feedback implement PumpSelector.Select
func (r *RangeSelector) Feedback(startTS int64, binlogType pb.BinlogType, pump *PumpStatus) {
	maintainTSMap(startTS, binlogType, pump)
}

// LocalUnixSelector will always select the local pump, used for compatible with kafka version tidb-binlog.
type LocalUnixSelector struct {
	// the pump to be selected.
	Pump *PumpStatus
}

// NewLocalUnixSelector returns a new LocalUnixSelector.
func NewLocalUnixSelector() PumpSelector {
	return &LocalUnixSelector{}
}

// SetPumps implement PumpSelector.SetPumps.
func (u *LocalUnixSelector) SetPumps(pumps []*PumpStatus) {
	selectorLock.Lock()
	if len(pumps) == 0 {
		u.Pump = nil
	} else {
		u.Pump = pumps[0]
	}
	selectorLock.Unlock()
}

// Select implement PumpSelector.Select.
func (u *LocalUnixSelector) Select(binlog *pb.Binlog, retryTime int) *PumpStatus {
	selectorLock.RLock()
	defer selectorLock.RUnlock()

	return u.Pump
}

// Feedback implement PumpSelector.Feedback
func (u *LocalUnixSelector) Feedback(startTS int64, binlogType pb.BinlogType, pump *PumpStatus) {
}

// ScoreSelector select a pump by pump's score.
type ScoreSelector struct{}

// NewScoreSelector returns a new ScoreSelector.
func NewScoreSelector() PumpSelector {
	return &ScoreSelector{}
}

// SetPumps implement PumpSelector.SetPumps.
func (s *ScoreSelector) SetPumps(pumps []*PumpStatus) {
	// TODO
}

// Select implement PumpSelector.Select.
func (s *ScoreSelector) Select(binlog *pb.Binlog, retryTime int) *PumpStatus {
	// TODO
	return nil
}

// Feedback implement PumpSelector.Feedback
func (s *ScoreSelector) Feedback(startTS int64, binlogType pb.BinlogType, pump *PumpStatus) {
	// TODO
}

// NewSelector returns a PumpSelector according to the strategy.
func NewSelector(strategy string) PumpSelector {
	switch strategy {
	case Range:
		return NewRangeSelector()
	case Hash:
		return NewHashSelector()
	case Score:
		return NewScoreSelector()
	case LocalUnix:
		return NewLocalUnixSelector()
	default:
		log.Warn("[pumps client] unknown strategy, use range as default", zap.String("strategy", strategy))
		return NewRangeSelector()
	}
}

func maintainTSMap(startTS int64, binlogType pb.BinlogType, pump *PumpStatus) {
	selectorLock.Lock()
	if binlogType != pb.BinlogType_Prewrite {
		delete(tsMap, startTS)
	} else {
		tsMap[startTS] = pump
	}
	selectorLock.Unlock()
}
