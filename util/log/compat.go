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

import (
	"github.com/Sirupsen/logrus"
)

const (
	// LevelFatal level. Logs and then calls `os.Exit(1)`.
	LevelFatal = logrus.FatalLevel
	// LevelError level. Logs. Used for errors that should definitely be noted.
	LevelError = logrus.ErrorLevel
	// LevelWarn level. Non-critical entries that deserve eyes.
	LevelWarn = logrus.WarnLevel
	// LevelInfo level. General operational entries about what's going on inside the application.
	LevelInfo = logrus.InfoLevel
	// LevelDebug level. Usually only enabled when debugging.
	LevelDebug = logrus.DebugLevel
)

// SetFlags does nothing.
func SetFlags(_ int) {
}

// SetLevelByString sets current log level by string represent.
func SetLevelByString(level string) {
	logrus.SetLevel(stringToLogLevel(level))
}

// SetHighlighting sets color output
func SetHighlighting(highlighting bool) {
	if highlighting && logrus.IsTerminal(logrus.StandardLogger().Out) {
		logrus.SetFormatter(stringToLogFormatter("console", false))
	} else {
		logrus.SetFormatter(stringToLogFormatter("text", false))
	}
}

// SetRotateByDay does nothing.
func SetRotateByDay() {
}

// SetRotateByHour does nothing.
func SetRotateByHour() {
}
