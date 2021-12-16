// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package log_test

import (
	"regexp"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var _ = Suite(&testFilterSuite{})

type testFilterSuite struct{}

func (s *testFilterSuite) TestFilter(c *C) {
	logger, buffer := log.MakeTestLogger()
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	c.Assert(
		buffer.Stripped(), Equals,
		`{"$lvl":"WARN","$msg":"the message","number":123456,"array":[7,8,9]}`,
	)

	logger, buffer = log.MakeTestLogger(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return log.NewFilterCore(c, "github.com/pingcap/br/")
	}), zap.AddCaller())
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	c.Assert(buffer.Stripped(), HasLen, 0)

	logger, buffer = log.MakeTestLogger(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return log.NewFilterCore(c, "github.com/pingcap/tidb/br/").With([]zap.Field{zap.String("a", "b")})
	}), zap.AddCaller())
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	c.Assert(
		buffer.Stripped(), Equals,
		`{"$lvl":"WARN","$msg":"the message","a":"b","number":123456,"array":[7,8,9]}`,
	)

	logger, buffer = log.MakeTestLogger(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return log.NewFilterCore(c, "github.com/pingcap/br/").With([]zap.Field{zap.String("a", "b")})
	}), zap.AddCaller())
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	c.Assert(buffer.Stripped(), HasLen, 0)

	// Fields won't trigger filter.
	logger, buffer = log.MakeTestLogger(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return log.NewFilterCore(c, "github.com/pingcap/check/").With([]zap.Field{zap.String("a", "b")})
	}), zap.AddCaller())
	logger.Warn("the message", zap.String("stack", "github.com/pingcap/tidb/br/"))
	c.Assert(buffer.Stripped(), HasLen, 0)
}

// PASS: filter_test.go:82: testFilterSuite.BenchmarkFilterRegexMatchString 1000000               1163 ns/op
// PASS: filter_test.go:64: testFilterSuite.BenchmarkFilterStringsContains  10000000               159 ns/op
//
// Run `go test github.com/pingcap/tidb/br/pkg/lightning/log -check.b -test.v` to get benchmark result.
func (s *testFilterSuite) BenchmarkFilterStringsContains(c *C) {
	c.ResetTimer()

	inputs := []string{
		"github.com/pingcap/tidb/some/package/path",
		"github.com/tikv/pd/some/package/path",
		"github.com/pingcap/tidb/br/some/package/path",
	}
	filters := []string{"github.com/pingcap/tidb/", "github.com/tikv/pd/"}
	for i := 0; i < c.N; i++ {
		for i := range inputs {
			for j := range filters {
				_ = strings.Contains(inputs[i], filters[j])
			}
		}
	}
}

func (s *testFilterSuite) BenchmarkFilterRegexMatchString(c *C) {
	c.ResetTimer()

	inputs := []string{
		"github.com/pingcap/tidb/some/package/path",
		"github.com/tikv/pd/some/package/path",
		"github.com/pingcap/tidb/br/some/package/path",
	}
	filters := regexp.MustCompile(`github.com/(pingcap/tidb|tikv/pd)/`)
	for i := 0; i < c.N; i++ {
		for i := range inputs {
			_ = filters.MatchString(inputs[i])
		}
	}
}
