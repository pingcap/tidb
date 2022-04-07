package utils

import (
	"fmt"
	"runtime"

	"go.uber.org/zap"
)

// WithRecover is a wrapper for recover
func WithRecover(fn func(), logger *zap.Logger, funcName string) {
	const size = 4096
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, size)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logger.Error("recover from panic",
				zap.String("function", funcName),
				zap.String("err", fmt.Sprintf("%v", r)),
				zap.String("stack", string(buf)),
			)
		}
	}()

	fn()
}
