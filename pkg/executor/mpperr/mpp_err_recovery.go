package mpperr

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
)

// MPPErrRecovery tries to recovery mpp error.
type MPPErrRecovery struct {
	enable   bool
	handlers []errHandler
	holder   *mppResultHolder

	curRecoveryCnt uint32
	maxRecoveryCnt uint32
}

// RecoveryInfo contains info that can help recovery error.
type RecoveryInfo struct {
	MPPErr  error

	// Nodes that involved into MPP computation.
	NodeCnt int
}

// NewMPPErrRecovery returns new instance of MPPErrRecovery.
func NewMPPErrRecovery(useAutoScaler bool, holderCap uint64, enable bool) *MPPErrRecovery {
	return &MPPErrRecovery{
		enable:   enable,
		handlers: []errHandler{newMemLimitErrHandler(useAutoScaler)},
		holder:   newMPPResultHolder(holderCap),
		// todo: sys var
		maxRecoveryCnt: 3,
	}
}

// CanHoldResult tells whether we can insert intermediate results.
func (m *MPPErrRecovery) CanHoldResult() bool {
	return m.enable && m.holder.capacity > 0 && !m.holder.full
}

// HoldResult tries to hold mpp result. You should call CanHoldResult to check first.
// Return true when hold success, return false if cannot hold anymore.
func (m *MPPErrRecovery) HoldResult(chk *chunk.Chunk) bool {
	return m.holder.insert(chk)
}

// NumHoldChk returns the number of chunk holded.
func (m *MPPErrRecovery) NumHoldChk() int {
	return len(m.holder.chks)
}

// PopFrontChk pop one chunk.
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
func (m *MPPErrRecovery) ResetHolder() {
	m.holder.reset()
}

// Recovery tries to recovery error. Reasons that cannot recovery:
//  1. already return result to client because holder is full.
//  2. recovery method of this kind of error not implemented or error is not recoveryable.
//  3. 
// Return err when got err when trying to recovery.
func (m *MPPErrRecovery) Recovery(info *RecoveryInfo) error {
	if info == nil || info.MPPErr == nil {
		return errors.New("RecoveryInfo is nil of mppErr is nil")
	}

	if m.holder.full {
		return errors.New("mpp result holder is full")
	}

	if m.curRecoveryCnt >= m.maxRecoveryCnt {
		return errors.Errorf("exceeds max recovery cnt: cur: %v, max: %v", m.curRecoveryCnt, m.maxRecoveryCnt)
	}

	m.curRecoveryCnt++

	for _, h := range m.handlers {
		if h.chooseErrHandler(info.MPPErr) {
			return h.doRecovery(info)
		}
	}
	return errors.New("no handler to recovery this type of mpp err")
}

type errHandler interface {
	chooseErrHandler(mppErr error) bool
	doRecovery(info *RecoveryInfo) error
}

var _ errHandler = &memLimitErrHandler{}

type memLimitErrHandler struct {
	useAutoScaler bool
}

func newMemLimitErrHandler(useAutoScaler bool) *memLimitErrHandler {
	return &memLimitErrHandler{
		useAutoScaler: useAutoScaler,
	}
}

func (h *memLimitErrHandler) chooseErrHandler(mppErr error) bool {
	if strings.Contains(mppErr.Error(), "Memory limit") && h.useAutoScaler {
		return true
	}
	return false
}

func (h *memLimitErrHandler) doRecovery(info *RecoveryInfo) error {
	// Ignore fetched topo, because AutoScaler will keep the topo for a while.
	// And will use the new topo when dispatch mpp task again.
	if _, err := tiflashcompute.GetGlobalTopoFetcher().RecoveryAndGetTopo(tiflashcompute.RecoveryTypeMemLimit, info.NodeCnt); err != nil {
		return err
	}
	return nil
}

type mppResultHolder struct {
	// todo
	// memTracker *
	capacity   uint64
	full bool
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
		h.full = true
		return false
	}

	h.chks = append(h.chks, chk)
	h.curRows += uint64(chk.NumRows())
	return true
}

func (h *mppResultHolder) reset() {
	h.chks = h.chks[:0]
}
