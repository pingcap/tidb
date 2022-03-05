// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package summary

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSumDurationInt(t *testing.T) {
	fields := []zap.Field{}
	logger := func(msg string, fs ...zap.Field) {
		fields = append(fields, fs...)
	}
	col := NewLogCollector(logger)
	col.CollectDuration("a", time.Second)
	col.CollectDuration("b", time.Second)
	col.CollectDuration("b", time.Second)
	col.CollectInt("c", 2)
	col.CollectInt("c", 2)
	col.SetSuccessStatus(true)
	col.Summary("foo")

	require.Equal(t, 7, len(fields))
	assertContains := func(field zap.Field) {
		for _, f := range fields {
			if f.Key == field.Key {
				require.Equal(t, field, f)
				return
			}
		}
		t.Error(field, "is not in", fields)
	}
	assertContains(zap.Duration("a", time.Second))
	assertContains(zap.Duration("b", 2*time.Second))
	assertContains(zap.Int("c", 4))
}
