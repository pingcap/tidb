// Copyright 2016 PingCAP, Inc.
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

package oracles

import (
	"testing"
	"time"
)

func TestLocalOracle(t *testing.T) {
	l := NewLocalOracle()
	m := map[uint64]struct{}{}
	for i := 0; i < 100000; i++ {
		ts, err := l.GetTimestamp()
		if err != nil {
			t.Error(err)
		}
		m[ts] = struct{}{}
	}

	if len(m) != 100000 {
		t.Error("generated same ts")
	}
}

func TestIsExpired(t *testing.T) {
	o := NewLocalOracle()
	ts, _ := o.GetTimestamp()
	time.Sleep(1 * time.Second)
	expire, _ := o.IsExpired(uint64(ts), 500)
	if !expire {
		t.Error("should expired")
	}
	expire, _ = o.IsExpired(uint64(ts), 2000)
	if expire {
		t.Error("should not expired")
	}
}
