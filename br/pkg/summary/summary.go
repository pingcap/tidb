// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package summary

import (
	"time"

	"go.uber.org/zap"
)

// SetUnit set unit "backup/restore" for summary log.
func SetUnit(unit string) {
	collector.SetUnit(unit)
}

// CollectSuccessUnit collects success time costs.
func CollectSuccessUnit(name string, unitCount int, arg interface{}) {
	collector.CollectSuccessUnit(name, unitCount, arg)
}

// CollectFailureUnit collects fail reason.
func CollectFailureUnit(name string, reason error) {
	collector.CollectFailureUnit(name, reason)
}

// CollectDuration collects log time field.
func CollectDuration(name string, t time.Duration) {
	collector.CollectDuration(name, t)
}

// CollectInt collects log int field.
func CollectInt(name string, t int) {
	collector.CollectInt(name, t)
}

// CollectUint collects log uint64 field.
func CollectUint(name string, t uint64) {
	collector.CollectUInt(name, t)
}

// SetSuccessStatus sets final success status.
func SetSuccessStatus(success bool) {
	collector.SetSuccessStatus(success)
}

// Summary outputs summary log.
func Summary(name string) {
	collector.Summary(name)
}

func Log(msg string, fields ...zap.Field) {
	collector.Log(msg, fields...)
}
