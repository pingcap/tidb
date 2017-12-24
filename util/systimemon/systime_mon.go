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

package systimemon

import (
	"time"

	log "github.com/sirupsen/logrus"
)

// StartMonitor calls systimeErrHandler if system time jump backward.
func StartMonitor(now func() time.Time, systimeErrHandler func()) {
	log.Info("start system time monitor")
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		last := now().UnixNano()
		<-tick.C
		if now().UnixNano() < last {
			log.Errorf("system time jump backward, last:%v", last)
			systimeErrHandler()
		}
	}
}
