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

func Info(v ...interface{}) {
	logrus.Info(v...)
}

func Infof(format string, v ...interface{}) {
	logrus.Infof(format, v...)
}

func Debug(v ...interface{}) {
	logrus.Debug(v...)
}

func Debugf(format string, v ...interface{}) {
	logrus.Debugf(format, v...)
}

func Warn(v ...interface{}) {
	logrus.Warning(v...)
}

func Warnf(format string, v ...interface{}) {
	logrus.Warningf(format, v...)
}

func Warning(v ...interface{}) {
	logrus.Warning(v...)
}

func Warningf(format string, v ...interface{}) {
	logrus.Warningf(format, v...)
}

func Error(v ...interface{}) {
	logrus.Error(v...)
}

func Errorf(format string, v ...interface{}) {
	logrus.Errorf(format, v...)
}

func Fatal(v ...interface{}) {
	logrus.Fatal(v...)
}

func Fatalf(format string, v ...interface{}) {
	logrus.Fatalf(format, v...)
}

// reexports logrus structure logging functions

func WithField(key string, value interface{}) *logrus.Entry {
	return logrus.WithField(key, value)
}

func WithFields(fields logrus.Fields) *logrus.Entry {
	return logrus.WithFields(fields)
}

func WithError(err error) *logrus.Entry {
	return logrus.WithError(err)
}
