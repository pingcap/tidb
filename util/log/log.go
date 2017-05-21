// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import "github.com/Sirupsen/logrus"

// Info logs a message at level Info on the standard logger.
func Info(v ...interface{}) {
	logrus.Info(v...)
}

// Infof logs a message at level Info on the standard logger.
func Infof(format string, v ...interface{}) {
	logrus.Infof(format, v...)
}

// Debug logs a message at level Debug on the standard logger.
func Debug(v ...interface{}) {
	logrus.Debug(v...)
}

// Debugf logs a message at level Debug on the standard logger.
func Debugf(format string, v ...interface{}) {
	logrus.Debugf(format, v...)
}

// Warn logs a message at level Warn on the standard logger.
func Warn(v ...interface{}) {
	logrus.Warning(v...)
}

// Warnf logs a message at level Warn on the standard logger.
func Warnf(format string, v ...interface{}) {
	logrus.Warningf(format, v...)
}

// Warning logs a message at level Warn on the standard logger.
func Warning(v ...interface{}) {
	logrus.Warning(v...)
}

// Warningf logs a message at level Warn on the standard logger.
func Warningf(format string, v ...interface{}) {
	logrus.Warningf(format, v...)
}

// Error logs a message at level Error on the standard logger.
func Error(v ...interface{}) {
	logrus.Error(v...)
}

// Errorf logs a message at level Error on the standard logger.
func Errorf(format string, v ...interface{}) {
	logrus.Errorf(format, v...)
}

// Fatal logs a message at level Fatal on the standard logger.
func Fatal(v ...interface{}) {
	logrus.Fatal(v...)
}

// Fatalf logs a message at level Fatal on the standard logger.
func Fatalf(format string, v ...interface{}) {
	logrus.Fatalf(format, v...)
}

// reexports logrus structure logging functions

// WithField re-exports logrus logging function.
func WithField(key string, value interface{}) *logrus.Entry {
	return logrus.WithField(key, value)
}

// WithFields re-exports logrus logging function.
func WithFields(fields logrus.Fields) *logrus.Entry {
	return logrus.WithFields(fields)
}

// WithError re-exports logrus logging function.
func WithError(err error) *logrus.Entry {
	return logrus.WithError(err)
}
