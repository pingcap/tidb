package usage

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/parser/model"
)

// StatsUsage maps (tableID, columnID) to the last time when the column stats are used(needed).
// All methods of it are thread-safe.
type StatsUsage struct {
	usage map[model.TableItemID]time.Time
	lock  sync.RWMutex
}

// NewStatsUsage creates a new StatsUsage.
func NewStatsUsage() *StatsUsage {
	return &StatsUsage{
		usage: make(map[model.TableItemID]time.Time),
	}
}

// Reset resets the StatsUsage.
func (m *StatsUsage) Reset() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.usage = make(map[model.TableItemID]time.Time)
}

// GetUsageAndReset gets the usage and resets the StatsUsage.
func (m *StatsUsage) GetUsageAndReset() map[model.TableItemID]time.Time {
	m.lock.Lock()
	defer m.lock.Unlock()
	ret := m.usage
	m.usage = make(map[model.TableItemID]time.Time)
	return ret
}

// Merge merges the usageMap into the StatsUsage.
func (m *StatsUsage) Merge(other map[model.TableItemID]time.Time) {
	if len(other) == 0 {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	for id, t := range other {
		if mt, ok := m.usage[id]; !ok || mt.Before(t) {
			m.usage[id] = t
		}
	}
}
