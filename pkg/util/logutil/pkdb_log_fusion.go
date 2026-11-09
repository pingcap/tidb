//go:build fusion

package logutil

import (
	"github.com/tikv/client-go/v2/tikv"
)

// func setAppLoggerFunc(log *zap.Logger) {
// 	tikv.SetAppLogger(log)
// }

func init() {
	setAppLoggerFunc = tikv.SetAppLogger
}
