package mpperr

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
)

type MPPErrRecovery struct {
	enable   bool
	handlers []errHandler
	holder   *mppResultHolder
}

func NewMPPErrRecovery(useAutoScaler bool, holderCap uint64, enable bool) *MPPErrRecovery {
	return &MPPErrRecovery{
		enable:   enable,
		handlers: []errHandler{newMemLimitErrHandler(useAutoScaler)},
		holder:   newMPPResultHolder(holderCap),
	}
}

func (m *MPPErrRecovery) CanHoldResult() bool {
	return m.enable && m.holder.capacity > 0 && !m.holder.holdFailed
}

// HoldResult tries to hold mpp result.
// Return true when hold success, return false if cannot hold anymore.
func (m *MPPErrRecovery) HoldResult(chk *chunk.Chunk) bool {
	return m.enable && m.holder.insert(chk)
}

func (m *MPPErrRecovery) NumHoldChk() int {
	return len(m.holder.chks)
}

func (m *MPPErrRecovery) PopFrontChk() *chunk.Chunk {
	if !m.enable || len(m.holder.chks) == 0 {
		return nil
	}
	chk := m.holder.chks[0]
	m.holder.chks = m.holder.chks[1:]
	return chk
}

// Reset reset the dynamic data, like chk and recovery cnt.
// Will not touch other metadata, like enable.
func (m *MPPErrRecovery) Reset() {
	for _, h := range m.handlers {
		h.reset()
	}
	m.holder.reset()
}

// Recovery try to recovery error.
// Return true if can recovery, otherwise return false. Reasons that cannot recovery:
//  1. already return result to client.
//  2. recovery method of this kind of error not implemented or error is not recoveryable.
//
// Return err when got err when trying to recovery.
func (m *MPPErrRecovery) Recovery(mppErr error) (bool, error) {
	if mppErr == nil {
		return false, nil
	}

	if m.holder.holdFailed {
		return false, nil
	}

	for _, h := range m.handlers {
		if h.chooseErrHandler(mppErr) {
			return h.doRecovery()
		}
	}
	return false, nil
}

type errHandler interface {
	chooseErrHandler(mppErr error) bool
	doRecovery() (bool, error)
	reset()
}

var _ errHandler = &memLimitErrHandler{}

type memLimitErrHandler struct {
	curRecoveryCnt uint32
	maxRecoveryCnt uint32

	useAutoScaler bool
}

func newMemLimitErrHandler(useAutoScaler bool) *memLimitErrHandler {
	return &memLimitErrHandler{
		useAutoScaler: useAutoScaler,
	}
}

func (h *memLimitErrHandler) chooseErrHandler(mppErr error) bool {
	if strings.Contains(mppErr.Error(), "Memory Limit") && h.useAutoScaler {
		return true
	}
	return false
}

func (h *memLimitErrHandler) doRecovery() (bool, error) {
	if h.curRecoveryCnt >= h.maxRecoveryCnt {
		return false, errors.Errorf("exceeds max recovery cnt: cur: %v, max: %v", h.curRecoveryCnt, h.maxRecoveryCnt)
	}

	// Ignore fetched topo, because AutoScaler will keep the topo for a while.
	// And will use the new topo when dispatch mpp task again.
	_, err := tiflashcompute.GetGlobalTopoFetcher().FetchAndGetTopo(tiflashcompute.RecoveryTypeMemLimit)
	if err != nil {
		return true, err
	}
	return true, nil
}

func (h *memLimitErrHandler) reset() {
	h.curRecoveryCnt = 0
}

type mppResultHolder struct {
	// todo
	// memTracker *
	capacity   uint64
	holdFailed bool
	curRows    uint64
	chks       []*chunk.Chunk
}

func newMPPResultHolder(holderCap uint64) *mppResultHolder {
	return &mppResultHolder{
		capacity: holderCap,
		chks:     []*chunk.Chunk{},
	}
}

func (h *mppResultHolder) insert(chk *chunk.Chunk) bool {
	if h.curRows+uint64(chk.NumRows()) >= h.capacity {
		h.holdFailed = true
		return false
	}

	h.chks = append(h.chks, chk)
	h.curRows += uint64(chk.NumRows())
	return true
}

func (h *mppResultHolder) reset() {
	h.chks = h.chks[:0]
}
