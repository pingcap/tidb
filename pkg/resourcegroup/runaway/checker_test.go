// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runaway

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/util"
)

// newTestRunawayRecordFlusher creates a minimal batchFlusher for testing markRunaway.
func newTestRunawayRecordFlusher() *batchFlusher[recordKey, *Record] {
	return newTestBatchFlusher(
		100,
		func(m map[recordKey]*Record, k recordKey, v *Record) {
			if existing, ok := m[k]; ok {
				existing.Repeats++
			} else {
				m[k] = v
			}
		},
		func(m map[recordKey]*Record) error { return nil },
	)
}

func TestActiveGroupCounterOrdering(t *testing.T) {
	t.Run("NormalOrder", func(t *testing.T) {
		m := &Manager{}
		group := "rg_normal"
		// Simulate insertion callback: +1
		counter, _ := m.loadOrStoreActiveCounter(group)
		counter.Add(1)
		// Simulate eviction callback: -1
		counter, _ = m.loadOrStoreActiveCounter(group)
		counter.Add(-1)
		assert.Equal(t, int64(0), m.getActiveWatchCount(group))
	})

	t.Run("ReversedOrder", func(t *testing.T) {
		m := &Manager{}
		group := "rg_reversed"
		// Simulate eviction callback arriving before insertion callback: -1 first
		counter, _ := m.loadOrStoreActiveCounter(group)
		counter.Add(-1)
		// Then insertion callback: +1
		counter, _ = m.loadOrStoreActiveCounter(group)
		counter.Add(1)
		assert.Equal(t, int64(0), m.getActiveWatchCount(group))
	})

	t.Run("NonExistentGroup", func(t *testing.T) {
		m := &Manager{}
		assert.Equal(t, int64(0), m.getActiveWatchCount("no_such_group"))
	})

	t.Run("ConcurrentInsertionAndEviction", func(t *testing.T) {
		m := &Manager{}
		group := "rg_concurrent"
		const n = 1000
		var wg sync.WaitGroup
		wg.Add(2 * n)
		for range n {
			go func() {
				defer wg.Done()
				c, _ := m.loadOrStoreActiveCounter(group)
				c.Add(1)
			}()
			go func() {
				defer wg.Done()
				c, _ := m.loadOrStoreActiveCounter(group)
				c.Add(-1)
			}()
		}
		wg.Wait()
		assert.Equal(t, int64(0), m.getActiveWatchCount(group))
	})
}

func TestConcurrentResetAndCheckThresholds(t *testing.T) {
	checker := &Checker{}

	// Simulate concurrent calls to ResetTotalProcessedKeys and CheckThresholds
	var wg sync.WaitGroup
	numGoroutines := 5
	processKeys := int64(10)

	// Goroutines for CheckThresholds
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_ = checker.CheckThresholds(&util.RUDetails{}, processKeys, nil)
			}
		}()
	}

	// Goroutines for ResetTotalProcessedKeys
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				checker.ResetTotalProcessedKeys()
				time.Sleep(time.Millisecond) // simulate some delay
			}
		}()
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Final check to ensure no race conditions occurred
	finalValue := atomic.LoadInt64(&checker.totalProcessedKeys)
	assert.GreaterOrEqual(t, finalValue, int64(0), "unexpected negative totalProcessedKeys value")
}

func TestNewChecker(t *testing.T) {
	t.Run("NilSettings", func(t *testing.T) {
		c := NewChecker(nil, "rg", nil, "SELECT 1", "sql_d", "plan_d", time.Now())
		assert.True(t, c.deadline.IsZero())
		assert.Equal(t, int64(0), c.ruThreshold)
		assert.Equal(t, int64(0), c.processedKeysThreshold)
		assert.Nil(t, c.settings)
	})

	t.Run("WithAllThresholds", func(t *testing.T) {
		start := time.Now()
		settings := &rmpb.RunawaySettings{
			Rule: &rmpb.RunawayRule{
				ExecElapsedTimeMs: 5000,
				RequestUnit:       1000,
				ProcessedKeys:     500,
			},
		}
		c := NewChecker(nil, "rg", settings, "SELECT 1", "sql_d", "plan_d", start)
		assert.Equal(t, start.Add(5000*time.Millisecond), c.deadline)
		assert.Equal(t, int64(1000), c.ruThreshold)
		assert.Equal(t, int64(500), c.processedKeysThreshold)
	})

	t.Run("ZeroElapsedTimeSkipsDeadline", func(t *testing.T) {
		settings := &rmpb.RunawaySettings{
			Rule: &rmpb.RunawayRule{
				ExecElapsedTimeMs: 0,
				ProcessedKeys:     500,
			},
		}
		c := NewChecker(nil, "rg", settings, "SELECT 1", "sql_d", "plan_d", time.Now())
		assert.True(t, c.deadline.IsZero())
		assert.Equal(t, int64(500), c.processedKeysThreshold)
	})
}

func TestExceedsThresholds(t *testing.T) {
	t.Run("NoThresholds", func(t *testing.T) {
		c := &Checker{}
		assert.Empty(t, c.exceedsThresholds(time.Now(), nil, 0))
	})

	t.Run("DeadlineExceeded", func(t *testing.T) {
		c := &Checker{deadline: time.Now().Add(-time.Second)}
		cause := c.exceedsThresholds(time.Now(), nil, 0)
		assert.Contains(t, cause, "ElapsedTime")
	})

	t.Run("DeadlineNotExceeded", func(t *testing.T) {
		c := &Checker{deadline: time.Now().Add(time.Hour)}
		assert.Empty(t, c.exceedsThresholds(time.Now(), nil, 0))
	})

	t.Run("ProcessedKeysNotExceeded", func(t *testing.T) {
		c := &Checker{processedKeysThreshold: 100}
		assert.Empty(t, c.exceedsThresholds(time.Now(), nil, 50))
	})

	t.Run("ProcessedKeysAtThreshold", func(t *testing.T) {
		c := &Checker{processedKeysThreshold: 100}
		cause := c.exceedsThresholds(time.Now(), nil, 100)
		assert.Contains(t, cause, "ProcessedKeys")
	})

	t.Run("DeadlineTakesPriority", func(t *testing.T) {
		c := &Checker{
			deadline:               time.Now().Add(-time.Second),
			processedKeysThreshold: 100,
		}
		cause := c.exceedsThresholds(time.Now(), nil, 200)
		assert.Contains(t, cause, "ElapsedTime")
		assert.NotContains(t, cause, "ProcessedKeys")
	})
}

func TestCheckerCheckAction(t *testing.T) {
	t.Run("Unmarked", func(t *testing.T) {
		c := &Checker{}
		assert.Equal(t, rmpb.RunawayAction_NoneAction, c.CheckAction())
	})

	t.Run("MarkedByWatchRule", func(t *testing.T) {
		c := &Checker{
			markedByQueryWatchRule: true,
			watchAction:            rmpb.RunawayAction_CoolDown,
		}
		assert.Equal(t, rmpb.RunawayAction_CoolDown, c.CheckAction())
	})

	t.Run("MarkedBySettings", func(t *testing.T) {
		c := &Checker{
			settings: &rmpb.RunawaySettings{Action: rmpb.RunawayAction_Kill},
		}
		c.markedByIdentifyInRunawaySettings.Store(true)
		assert.Equal(t, rmpb.RunawayAction_Kill, c.CheckAction())
	})

	t.Run("BothMarked_WatchTakesPriority", func(t *testing.T) {
		c := &Checker{
			markedByQueryWatchRule: true,
			watchAction:            rmpb.RunawayAction_CoolDown,
			settings:               &rmpb.RunawaySettings{Action: rmpb.RunawayAction_Kill},
		}
		c.markedByIdentifyInRunawaySettings.Store(true)
		assert.Equal(t, rmpb.RunawayAction_CoolDown, c.CheckAction())
	})
}

func TestGetSettingConvictIdentifier(t *testing.T) {
	t.Run("NilSettings", func(t *testing.T) {
		c := &Checker{}
		assert.Empty(t, c.getSettingConvictIdentifier())
	})

	t.Run("NilWatch", func(t *testing.T) {
		c := &Checker{settings: &rmpb.RunawaySettings{}}
		assert.Empty(t, c.getSettingConvictIdentifier())
	})

	t.Run("PlanType", func(t *testing.T) {
		c := &Checker{
			planDigest: "plan123", sqlDigest: "sql123", originalSQL: "SELECT 1",
			settings: &rmpb.RunawaySettings{Watch: &rmpb.RunawayWatch{Type: rmpb.RunawayWatchType_Plan}},
		}
		assert.Equal(t, "plan123", c.getSettingConvictIdentifier())
	})

	t.Run("SimilarType", func(t *testing.T) {
		c := &Checker{
			planDigest: "plan123", sqlDigest: "sql123", originalSQL: "SELECT 1",
			settings: &rmpb.RunawaySettings{Watch: &rmpb.RunawayWatch{Type: rmpb.RunawayWatchType_Similar}},
		}
		assert.Equal(t, "sql123", c.getSettingConvictIdentifier())
	})

	t.Run("ExactType", func(t *testing.T) {
		c := &Checker{
			planDigest: "plan123", sqlDigest: "sql123", originalSQL: "SELECT 1",
			settings: &rmpb.RunawaySettings{Watch: &rmpb.RunawayWatch{Type: rmpb.RunawayWatchType_Exact}},
		}
		assert.Equal(t, "SELECT 1", c.getSettingConvictIdentifier())
	})
}

func TestNilCheckerSafety(t *testing.T) {
	var c *Checker
	assert.Equal(t, rmpb.RunawayAction_NoneAction, c.CheckAction())
	assert.False(t, c.isMarkedByIdentifyInRunawaySettings())
	assert.Empty(t, c.getSettingConvictIdentifier())

	switchGroup, err := c.BeforeExecutor()
	assert.Empty(t, switchGroup)
	assert.NoError(t, err)

	assert.NoError(t, c.BeforeCopRequest(nil))

	exceedCause, shouldKill := c.CheckRuleKillAction()
	assert.Empty(t, exceedCause)
	assert.False(t, shouldKill)

	assert.NoError(t, c.CheckThresholds(nil, 0, nil))
	c.ResetTotalProcessedKeys() // should not panic
}

func TestCheckThresholds(t *testing.T) {
	newCheckerWithAction := func(action rmpb.RunawayAction) (*Checker, *Manager) {
		m := &Manager{runawayRecordFlusher: newTestRunawayRecordFlusher()}
		c := &Checker{
			manager:                m,
			resourceGroupName:      "rg_threshold",
			processedKeysThreshold: 100,
			settings: &rmpb.RunawaySettings{
				Action: action,
				Rule:   &rmpb.RunawayRule{ProcessedKeys: 100},
			},
		}
		return c, m
	}

	t.Run("BelowThreshold", func(t *testing.T) {
		c, _ := newCheckerWithAction(rmpb.RunawayAction_Kill)
		err := c.CheckThresholds(&util.RUDetails{}, 50, nil)
		assert.NoError(t, err)
		assert.False(t, c.markedByIdentifyInRunawaySettings.Load())
	})

	t.Run("KillOnExceed", func(t *testing.T) {
		c, m := newCheckerWithAction(rmpb.RunawayAction_Kill)
		err := c.CheckThresholds(&util.RUDetails{}, 200, nil)
		assert.Error(t, err)
		assert.True(t, c.markedByIdentifyInRunawaySettings.Load())
		assert.Equal(t, 1, m.runawayRecordFlusher.bufferLen())
	})

	t.Run("CoolDownOnExceed", func(t *testing.T) {
		c, m := newCheckerWithAction(rmpb.RunawayAction_CoolDown)
		err := c.CheckThresholds(&util.RUDetails{}, 200, nil)
		assert.NoError(t, err)
		assert.True(t, c.markedByIdentifyInRunawaySettings.Load())
		assert.Equal(t, 1, m.runawayRecordFlusher.bufferLen())
	})
}

func TestCheckRuleKillAction(t *testing.T) {
	newCheckerWithDeadline := func(action rmpb.RunawayAction) *Checker {
		m := &Manager{runawayRecordFlusher: newTestRunawayRecordFlusher()}
		return &Checker{
			manager:           m,
			resourceGroupName: "rg_rule_kill",
			deadline:          time.Now().Add(-time.Second),
			settings: &rmpb.RunawaySettings{
				Action: action,
				Rule:   &rmpb.RunawayRule{ExecElapsedTimeMs: 1},
			},
		}
	}

	t.Run("NoSettings", func(t *testing.T) {
		c := &Checker{}
		cause, kill := c.CheckRuleKillAction()
		assert.Empty(t, cause)
		assert.False(t, kill)
	})

	t.Run("KillActionExceeded", func(t *testing.T) {
		c := newCheckerWithDeadline(rmpb.RunawayAction_Kill)
		cause, kill := c.CheckRuleKillAction()
		assert.Contains(t, cause, "ElapsedTime")
		assert.True(t, kill)
		assert.True(t, c.markedByIdentifyInRunawaySettings.Load())
	})

	t.Run("CoolDownActionExceeded", func(t *testing.T) {
		c := newCheckerWithDeadline(rmpb.RunawayAction_CoolDown)
		cause, kill := c.CheckRuleKillAction()
		assert.Contains(t, cause, "ElapsedTime")
		assert.False(t, kill)
	})

	t.Run("NotExceeded", func(t *testing.T) {
		m := &Manager{runawayRecordFlusher: newTestRunawayRecordFlusher()}
		c := &Checker{
			manager:           m,
			resourceGroupName: "rg_rule_kill",
			deadline:          time.Now().Add(time.Hour),
			settings: &rmpb.RunawaySettings{
				Action: rmpb.RunawayAction_Kill,
				Rule:   &rmpb.RunawayRule{ExecElapsedTimeMs: 99999999},
			},
		}
		cause, kill := c.CheckRuleKillAction()
		assert.Empty(t, cause)
		assert.False(t, kill)
	})

	t.Run("AlreadyMarked", func(t *testing.T) {
		c := newCheckerWithDeadline(rmpb.RunawayAction_Kill)
		c.markedByIdentifyInRunawaySettings.Store(true)
		cause, kill := c.CheckRuleKillAction()
		assert.Empty(t, cause)
		assert.False(t, kill)
	})
}

func TestMarkRunawayBySettingsCAS(t *testing.T) {
	m := &Manager{runawayRecordFlusher: newTestRunawayRecordFlusher()}
	c := &Checker{
		manager:           m,
		resourceGroupName: "rg_cas",
		settings: &rmpb.RunawaySettings{
			Action: rmpb.RunawayAction_Kill,
			Rule:   &rmpb.RunawayRule{},
		},
	}
	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	now := time.Now()
	for range n {
		go func() {
			defer wg.Done()
			c.markRunawayByIdentifyInRunawaySettings(&now, "test cause")
		}()
	}
	wg.Wait()
	assert.True(t, c.markedByIdentifyInRunawaySettings.Load())
	// CAS ensures only one goroutine successfully marks and enqueues a record.
	assert.Equal(t, 1, m.runawayRecordFlusher.bufferLen())
}
