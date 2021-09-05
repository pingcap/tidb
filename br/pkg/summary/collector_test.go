// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package summary

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"go.uber.org/zap"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testCollectorSuite{})

type testCollectorSuite struct {
}

func (suit *testCollectorSuite) TestSumDurationInt(c *C) {
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

	c.Assert(len(fields), Equals, 7)
	assertContains := func(field zap.Field) {
		for _, f := range fields {
			if f.Key == field.Key {
				c.Assert(f, DeepEquals, field)
				return
			}
		}
		c.Error(fields, "do not contain", field)
	}
	assertContains(zap.Duration("a", time.Second))
	assertContains(zap.Duration("b", 2*time.Second))
	assertContains(zap.Int("c", 4))
}
