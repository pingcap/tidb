package mvs

import (
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func resetTimer(timer *mvsTimer, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}

// NotifyDDLChange marks MV metadata as dirty and wakes the service loop.
func (t *MVService) NotifyDDLChange() {
	t.ddlDirty.Store(true)
	t.notifier.Wake()
}

func (t *MVService) Run() {
	defer func() {
		if r := recover(); r != nil {
			t.mh.observeRunEvent(mvRunEventRecoveredPanic)
			logutil.BgLogger().Error("MVService panicked", zap.Any("error", r))
		}
	}()
	if !t.sch.init() {
		t.mh.observeRunEvent(mvRunEventInitFailed)
		return
	}
	t.executor.Run()
	timer := mvsNewTimer(0)
	metricTimer := mvsNewTimer(t.basicInterval)

	defer func() {
		timer.Stop()
		metricTimer.Stop()
		t.executor.Close()
		t.mh.reportMetrics(t)
	}()

	lastServerRefresh := time.Time{}
	for {
		ddlDirty := false
		select {
		case <-timer.C:
		case <-metricTimer.C:
			t.mh.reportMetrics(t)
			resetTimer(metricTimer, t.basicInterval)
		case <-t.notifier.C:
			t.notifier.clear()
			ddlDirty = t.ddlDirty.Swap(false)
		case <-t.ctx.Done():
			return
		}

		now := mvsNow()

		if now.Sub(lastServerRefresh) >= t.serverRefreshInterval {
			if err := t.sch.refresh(); err != nil {
				t.mh.observeRunEvent(mvRunEventServerRefreshError)
				logutil.BgLogger().Warn("refresh all TiDB server info failed", zap.Error(err))
			} else {
				t.mh.observeRunEvent(mvRunEventServerRefreshOK)
			}
			lastServerRefresh = now
		}

		shouldFetch := t.shouldFetchMVMeta(now)
		if ddlDirty || shouldFetch {
			if ddlDirty {
				t.mh.observeRunEvent(mvRunEventFetchByDDL)
			}
			if shouldFetch {
				t.mh.observeRunEvent(mvRunEventFetchByInterval)
			}
			// if server info is stale, wait for the next refresh cycle to fetch server info
			if err := t.fetchAllMVMeta(); err != nil {
				// avoid tight retries on fetch failures
				t.lastRefresh.Store(now.UnixMilli())
			}
		}

		mvLogToPurge, mvToRefresh := t.fetchExecTasks(now)
		t.purgeMVLog(mvLogToPurge)
		t.refreshMV(mvToRefresh)

		next := t.nextScheduleTime(now)
		resetTimer(timer, mvsUntil(next))
	}
}

func (t *MVService) shouldFetchMVMeta(now time.Time) bool {
	last := t.lastRefresh.Load()
	if last == 0 {
		return true
	}
	return now.Sub(mvsUnixMilli(last)) >= t.fetchInterval
}

func (t *MVService) nextFetchTime(now time.Time) time.Time {
	last := t.lastRefresh.Load()
	if last == 0 {
		return now
	}
	next := mvsUnixMilli(last).Add(t.fetchInterval)
	if next.Before(now) {
		return now
	}
	return next
}

func (t *MVService) nextDueTime() (time.Time, bool) {
	next := time.Time{}
	has := false
	{
		t.mvRefreshMu.Lock()
		if item := t.mvRefreshMu.prio.Front(); item != nil {
			next = mvsUnixMilli(item.Value.orderTs)
			has = true
		}
		t.mvRefreshMu.Unlock()
	}

	{
		t.mvLogPurgeMu.Lock()
		if item := t.mvLogPurgeMu.prio.Front(); item != nil {
			due := mvsUnixMilli(item.Value.orderTs)
			if !has || due.Before(next) {
				next = due
				has = true
			}
		}
		t.mvLogPurgeMu.Unlock()
	}
	return next, has
}

func (t *MVService) nextScheduleTime(now time.Time) time.Time {
	next := t.nextFetchTime(now)
	if due, ok := t.nextDueTime(); ok && due.Before(next) {
		next = due
	}
	if next.Before(now) {
		return now
	}
	return next
}
