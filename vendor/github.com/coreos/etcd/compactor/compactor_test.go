// Copyright 2015 The etcd Authors
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

package compactor

import (
	"reflect"
	"testing"
	"time"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/pkg/testutil"
	"github.com/jonboulle/clockwork"
	"golang.org/x/net/context"
)

func TestPeriodic(t *testing.T) {
	retentionHours := 2

	fc := clockwork.NewFakeClock()
	rg := &fakeRevGetter{testutil.NewRecorderStream(), 0}
	compactable := &fakeCompactable{testutil.NewRecorderStream()}
	tb := &Periodic{
		clock:        fc,
		periodInHour: retentionHours,
		rg:           rg,
		c:            compactable,
	}

	tb.Run()
	defer tb.Stop()

	n := int(time.Hour / checkCompactionInterval)
	// collect 5 hours of revisions
	for i := 0; i < 5; i++ {
		// advance one hour, one revision for each interval
		for j := 0; j < n; j++ {
			rg.Wait(1)
			fc.Advance(checkCompactionInterval)
		}

		// compaction doesn't happen til 2 hours elapses
		if i+1 < retentionHours {
			continue
		}

		a, err := compactable.Wait(1)
		if err != nil {
			t.Fatal(err)
		}
		expectedRevision := int64(1 + (i+1)*n - retentionHours*n)
		if !reflect.DeepEqual(a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision}) {
			t.Errorf("compact request = %v, want %v", a[0].Params[0], &pb.CompactionRequest{Revision: expectedRevision})
		}
	}

	// unblock the rev getter, so we can stop the compactor routine.
	_, err := rg.Wait(1)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPeriodicPause(t *testing.T) {
	fc := clockwork.NewFakeClock()
	compactable := &fakeCompactable{testutil.NewRecorderStream()}
	rg := &fakeRevGetter{testutil.NewRecorderStream(), 0}
	tb := &Periodic{
		clock:        fc,
		periodInHour: 1,
		rg:           rg,
		c:            compactable,
	}

	tb.Run()
	tb.Pause()

	// tb will collect 3 hours of revisions but not compact since paused
	n := int(time.Hour / checkCompactionInterval)
	for i := 0; i < 3*n; i++ {
		rg.Wait(1)
		fc.Advance(checkCompactionInterval)
	}
	// tb ends up waiting for the clock

	select {
	case a := <-compactable.Chan():
		t.Fatalf("unexpected action %v", a)
	case <-time.After(10 * time.Millisecond):
	}

	// tb resumes to being blocked on the clock
	tb.Resume()

	// unblock clock, will kick off a compaction at hour 3:05
	rg.Wait(1)
	fc.Advance(checkCompactionInterval)
	a, err := compactable.Wait(1)
	if err != nil {
		t.Fatal(err)
	}
	// compact the revision from hour 2:05
	wreq := &pb.CompactionRequest{Revision: int64(1 + 2*n + 1)}
	if !reflect.DeepEqual(a[0].Params[0], wreq) {
		t.Errorf("compact request = %v, want %v", a[0].Params[0], wreq.Revision)
	}
}

type fakeCompactable struct {
	testutil.Recorder
}

func (fc *fakeCompactable) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	fc.Record(testutil.Action{Name: "c", Params: []interface{}{r}})
	return &pb.CompactionResponse{}, nil
}

type fakeRevGetter struct {
	testutil.Recorder
	rev int64
}

func (fr *fakeRevGetter) Rev() int64 {
	fr.Record(testutil.Action{Name: "g"})
	fr.rev++
	return fr.rev
}
