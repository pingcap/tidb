// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"fmt"

	"github.com/pingcap/tidb/util"
	"go.uber.org/zap"
)

// WithRecover is a wrapper for recover
func WithRecover(fn func(), logger *zap.Logger, funcName string) {
	defer func() {
		if r := recover(); r != nil {
			buf := util.GetStack()
			logger.Error("recover from panic",
				zap.String("function", funcName),
				zap.String("err", fmt.Sprintf("%v", r)),
				zap.ByteString("stack", buf),
			)
		}
	}()

	fn()
}
