package call

import (
	"bytes"
	"fmt"
	"time"
)

// ExecStats collects executors's execution info.
type ExecStats map[string]*ExecStat

// ExecStat collects one executor's execution info.
type ExecStat struct {
	loop    int
	consume time.Duration
	rows    int
}

// NewExecutorStats creates new executor collector.
func NewExecutorStats() ExecStats {
	return ExecStats(make(map[string]*ExecStat))
}

// GetExecStat gets execStat for a executor.
func (e ExecStats) GetExecStat(planID string) *ExecStat {
	if e == nil {
		return nil
	}
	execStat, exists := e[planID]
	if !exists {
		execStat = &ExecStat{}
		e[planID] = execStat
	}
	return execStat
}

func (e ExecStats) String() string {
	var buff bytes.Buffer
	buff.WriteString("(")
	for planID, stat := range e {
		buff.WriteString(planID + ":" + stat.String() + ",")
	}
	buff.WriteString(")")
	return buff.String()
}

// Record records executor's execution.
func (e *ExecStat) Record(d time.Duration, rowNum int) {
	e.loop++
	e.consume += d
	e.rows += rowNum
}

func (e *ExecStat) String() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("actual_time:%f, loops:%d, rows:%d", e.consume.Seconds()*1e3, e.loop, e.rows)
}
