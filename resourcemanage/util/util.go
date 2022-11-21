package util

import "time"

// GorotinuePool is a pool interface
type GorotinuePool interface {
	Release()

	Tune(size int)
	LastTunerTs() time.Time
	MaxInFlight() int64
	InFlight() int64
	MinRT() uint64
	MaxPASS() uint64
	Cap() int
	LongRTT() float64
	ShortRTT() uint64
	GetQueueSize() int64
	Running() int
}

// PoolContainer is a pool container
type PoolContainer struct {
	Pool      GorotinuePool
	Component Component
}

// TaskPriority is the priority of the task.
type TaskPriority int

const (
	HighPriority TaskPriority = iota
	NormalPriority
	LowPriority
)

// Component is ID for difference component
type Component int

const (
	UNKNOWN Component = iota // it is only for test
	DDL
)
