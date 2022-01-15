// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package log_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestFilter(t *testing.T) {
	logger, buffer := log.MakeTestLogger()
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))

	require.Equal(t, `{"$lvl":"WARN","$msg":"the message","number":123456,"array":[7,8,9]}`, buffer.Stripped())

	logger, buffer = log.MakeTestLogger(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return log.NewFilterCore(c, "github.com/pingcap/br/")
	}), zap.AddCaller())
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	require.Len(t, buffer.Stripped(), 0)

	logger, buffer = log.MakeTestLogger(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return log.NewFilterCore(c, "github.com/pingcap/tidb/br/").With([]zap.Field{zap.String("a", "b")})
	}), zap.AddCaller())
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	require.Equal(t, `{"$lvl":"WARN","$msg":"the message","a":"b","number":123456,"array":[7,8,9]}`, buffer.Stripped())

	logger, buffer = log.MakeTestLogger(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return log.NewFilterCore(c, "github.com/pingcap/br/").With([]zap.Field{zap.String("a", "b")})
	}), zap.AddCaller())
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	require.Len(t, buffer.Stripped(), 0)

	// Fields won't trigger filter.
	logger, buffer = log.MakeTestLogger(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return log.NewFilterCore(c, "github.com/pingcap/check/").With([]zap.Field{zap.String("a", "b")})
	}), zap.AddCaller())
	logger.Warn("the message", zap.String("stack", "github.com/pingcap/tidb/br/"))
	require.Len(t, buffer.Stripped(), 0)
}

// BenchmarkFilterStringsContains-16     	16693887	        66.68 ns/op
// BenchmarkFilterRegexMatchString-16    	 2350828	       510.6 ns/op
//
// Run `go test -run='^$' -bench=. -v github.com/pingcap/tidb/br/pkg/lightning/log` to get benchmark result.
func BenchmarkFilterStringsContains(b *testing.B) {
	b.ResetTimer()

	inputs := []string{
		"github.com/pingcap/tidb/some/package/path",
		"github.com/tikv/pd/some/package/path",
		"github.com/pingcap/tidb/br/some/package/path",
	}
	filters := []string{"github.com/pingcap/tidb/", "github.com/tikv/pd/"}
	for i := 0; i < b.N; i++ {
		for i := range inputs {
			for j := range filters {
				_ = strings.Contains(inputs[i], filters[j])
			}
		}
	}
}

func BenchmarkFilterRegexMatchString(b *testing.B) {
	b.ResetTimer()

	inputs := []string{
		"github.com/pingcap/tidb/some/package/path",
		"github.com/tikv/pd/some/package/path",
		"github.com/pingcap/tidb/br/some/package/path",
	}
	filters := regexp.MustCompile(`github.com/(pingcap/tidb|tikv/pd)/`)
	for i := 0; i < b.N; i++ {
		for i := range inputs {
			_ = filters.MatchString(inputs[i])
		}
	}
}
