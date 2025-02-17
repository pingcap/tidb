// Copyright 2023 PingCAP, Inc.
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

package mpp

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
)

// RecoveryHandler tries to recovery mpp error.
type RecoveryHandler struct {
	holder         *mppResultHolder
	handlers       []handlerImpl
	maxRecoveryCnt uint32
	curRecoveryCnt uint32
	enable         bool
}

// RecoveryInfo contains info that can help recovery error.
type RecoveryInfo struct {
	MPPErr error

	// Nodes that involved into MPP computation.
	NodeCnt int
}

const (
	memLimitErrPattern = "Memory limit"
)

// NewRecoveryHandler returns new instance of RecoveryHandler.
func NewRecoveryHandler(useAutoScaler bool, holderCap uint64, enable bool, parent *memory.Tracker) *RecoveryHandler {
	return &RecoveryHandler{
		enable:   enable,
		handlers: []handlerImpl{newMemLimitHandlerImpl(useAutoScaler)},
		holder:   newMPPResultHolder(holderCap, parent),
		// Default recovery 3 time.
		maxRecoveryCnt: 3,
	}
}

// Enabled return true when mpp err recovery enabled.
func (m *RecoveryHandler) Enabled() bool {
	return m.enable
}

// CanHoldResult tells whether we can insert intermediate results.
func (m *RecoveryHandler) CanHoldResult() bool {
	return m.holder.capacity > 0 && !m.holder.cannotHold
}

// HoldResult tries to hold mpp result. You should call Enabled() and CanHoldResult() to check first.
func (m *RecoveryHandler) HoldResult(resp *mppResponse) {
	m.holder.insert(resp)
}

// NumHoldResp returns the number of resp holded.
func (m *RecoveryHandler) NumHoldResp() int {
	return len(m.holder.resps)
}

// PopFrontResp pop one resp.
func (m *RecoveryHandler) PopFrontResp() (*mppResponse, error) {
	if !m.enable || len(m.holder.resps) == 0 {
		return nil, errors.Errorf("pop resp failed. enable: %v, size: %v", m.enable, len(m.holder.resps))
	}
	resp := m.holder.resps[0]
	m.holder.resps = m.holder.resps[1:]
	m.holder.memTracker.Consume(-resp.MemSize())
	m.holder.cannotHold = true
	return resp, nil
}

// ResetHolder reset the dynamic data, like resps and recovery cnt.
// Will not touch other metadata, like enable.
func (m *RecoveryHandler) ResetHolder() {
	m.holder.reset()
}

// RecoveryCnt returns the recovery count.
func (m *RecoveryHandler) RecoveryCnt() uint32 {
	return m.curRecoveryCnt
}

// Recovery tries to recovery error. Reasons that cannot recovery:
//  1. Already return result to client because holder is full.
//  2. Recovery method of this kind of error not implemented or error is not recoveryable.
//  3. Retry time exceeds maxRecoveryCnt.
func (m *RecoveryHandler) Recovery(info *RecoveryInfo) error {
	if !m.enable {
		return errors.New("mpp err recovery is not enabled")
	}

	if info == nil || info.MPPErr == nil {
		return errors.New("RecoveryInfo is nil or mppErr is nil")
	}

	if m.curRecoveryCnt >= m.maxRecoveryCnt {
		return errors.Errorf("exceeds max recovery cnt: cur: %v, max: %v", m.curRecoveryCnt, m.maxRecoveryCnt)
	}

	m.curRecoveryCnt++

	for _, h := range m.handlers {
		if h.chooseHandlerImpl(info.MPPErr) {
			return h.doRecovery(info)
		}
	}
	return errors.New("no handler to recovery this type of mpp err")
}

type handlerImpl interface {
	chooseHandlerImpl(mppErr error) bool
	doRecovery(info *RecoveryInfo) error
}

var _ handlerImpl = &memLimitHandlerImpl{}

type memLimitHandlerImpl struct {
	useAutoScaler bool
}

func newMemLimitHandlerImpl(useAutoScaler bool) *memLimitHandlerImpl {
	return &memLimitHandlerImpl{
		useAutoScaler: useAutoScaler,
	}
}

func (h *memLimitHandlerImpl) chooseHandlerImpl(mppErr error) bool {
	if strings.Contains(mppErr.Error(), memLimitErrPattern) && h.useAutoScaler {
		return true
	}
	return false
}

func (*memLimitHandlerImpl) doRecovery(info *RecoveryInfo) error {
	// Ignore fetched topo, because AutoScaler will keep the topo for a while.
	// And the new topo will be fetched when dispatch mpp task again.
	if _, err := tiflashcompute.GetGlobalTopoFetcher().RecoveryAndGetTopo(tiflashcompute.RecoveryTypeMemLimit, info.NodeCnt); err != nil {
		return err
	}
	return nil
}

type mppResultHolder struct {
	memTracker *memory.Tracker
	resps      []*mppResponse
	capacity   uint64
	cannotHold bool
}

func newMPPResultHolder(holderCap uint64, parent *memory.Tracker) *mppResultHolder {
	tracker := memory.NewTracker(parent.Label(), 0)
	tracker.AttachTo(parent)
	return &mppResultHolder{
		capacity:   holderCap,
		resps:      make([]*mppResponse, 0, holderCap),
		memTracker: tracker,
	}
}

func (h *mppResultHolder) insert(resp *mppResponse) {
	h.resps = append(h.resps, resp)

	// TODO: Better use row number as threshold. Need to add row number info in tipb.MPPDataPacket.
	if len(h.resps) >= int(h.capacity) {
		h.cannotHold = true
	}
	h.memTracker.Consume(resp.MemSize())
}

func (h *mppResultHolder) reset() {
	h.cannotHold = false
	h.resps = h.resps[:0]
	h.memTracker.Detach()
}
