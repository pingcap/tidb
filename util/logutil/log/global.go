// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"context"
	"fmt"
	"log/slog"
)

// Debug logs a message at DebugLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Debug(msg string, attrs ...slog.Attr) {
	slog.LogAttrs(context.Background(), slog.LevelDebug, msg, attrs...)
}

func Debugf(format string, args ...any) {
	slog.Default().Debug(fmt.Sprintf(format, args...))
}

// Info logs a message at InfoLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Info(msg string, attrs ...slog.Attr) {
	slog.LogAttrs(context.Background(), slog.LevelInfo, msg, attrs...)
}

func Infof(format string, args ...any) {
	slog.Default().Info(fmt.Sprintf(format, args...))
}

// Warn logs a message at WarnLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Warn(msg string, attrs ...slog.Attr) {
	slog.LogAttrs(context.Background(), slog.LevelWarn, msg, attrs...)
}

func Warnf(format string, args ...any) {
	slog.Default().Warn(fmt.Sprintf(format, args...))
}

// Error logs a message at ErrorLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Error(msg string, attrs ...slog.Attr) {
	slog.LogAttrs(context.Background(), slog.LevelError, msg, attrs...)
}

// Panic logs a message at PanicLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then panics, even if logging at PanicLevel is disabled.
// func Panic(msg string, fields ...zap.Field) {
// 	ll().Panic(msg, fields...)
// }

// Fatal logs a message at FatalLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then calls os.Exit(1), even if logging at FatalLevel is
// disabled.
func Fatal(msg string, args ...slog.Attr) {
	slog.Log(context.Background(), LevelFatal, msg, args)
}

const LevelFatal = slog.Level(12)

var With = slog.With

// // With creates a child logger and adds structured context to it.
// // Fields added to the child don't affect the parent, and vice versa.
// //
// // Deprecated: With should not add caller skip, since it's not a logging function.
// // Please use log.L().With instead. With is kept for compatibility.
// // See https://github.com/pingcap/log/issues/32 for more details.
// func With(fields ...zap.Field) *zap.Logger {
// 	return L().WithOptions(zap.AddCallerSkip(1)).With(fields...)
// }

// // SetLevel alters the logging level.
// func SetLevel(l zapcore.Level) {
// 	globalProperties.Load().(*ZapProperties).Level.SetLevel(l)
// }

// // GetLevel gets the logging level.
// func GetLevel() zapcore.Level {
// 	return globalProperties.Load().(*ZapProperties).Level.Level()
// }
