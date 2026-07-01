// Copyright 2026 PingCAP, Inc.
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

package sqlkiller

import "testing"

func TestConnectionAliveCallbackCleanupKeepsNewerCallback(t *testing.T) {
	var killer SQLKiller
	var oldCallbackCalls, newCallbackCalls int

	oldCallback := func() bool {
		oldCallbackCalls++
		return false
	}
	newCallback := func() bool {
		newCallbackCalls++
		return true
	}

	killer.IsConnectionAlive.Store(&oldCallback)
	killer.IsConnectionAlive.Store(&newCallback)

	killer.IsConnectionAlive.CompareAndSwap(&oldCallback, nil)
	killer.CheckConnectionAlive()

	if oldCallbackCalls != 0 {
		t.Fatalf("expected old callback not to be called, got %d calls", oldCallbackCalls)
	}
	if newCallbackCalls != 1 {
		t.Fatalf("expected newer callback to remain installed, got %d calls", newCallbackCalls)
	}
	if killer.GetKillSignal() != UnspecifiedKillSignal {
		t.Fatalf("expected no kill signal from the newer alive callback, got %d", killer.GetKillSignal())
	}

	killer.IsConnectionAlive.CompareAndSwap(&newCallback, nil)
	killer.CheckConnectionAlive()
	if newCallbackCalls != 1 {
		t.Fatalf("expected newer callback to be cleared, got %d calls", newCallbackCalls)
	}
}
