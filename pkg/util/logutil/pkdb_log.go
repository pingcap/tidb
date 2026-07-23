package logutil

import "go.uber.org/zap"

var setAppLoggerFunc func(log *zap.Logger)
