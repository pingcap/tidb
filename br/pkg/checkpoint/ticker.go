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

package checkpoint

import "time"

type TimeTicker interface {
	Ch() <-chan time.Time
	Stop()
}

type timeTicker struct {
	*time.Ticker
}

func (ticker timeTicker) Ch() <-chan time.Time {
	return ticker.C
}

type manualTicker struct{}

func (ticker manualTicker) Ch() <-chan time.Time {
	return nil
}

func (ticker manualTicker) Stop() {}

func dispatcherTicker(d time.Duration) TimeTicker {
	if d > 0 {
		return timeTicker{time.NewTicker(d)}
	}
	return manualTicker{}
}
