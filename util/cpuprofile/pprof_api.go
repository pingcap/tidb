// Copyright 2021 PingCAP, Inc.
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

package cpuprofile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/pprof/profile"
	goutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// ProfileHTTPHandler is same as pprof.Profile.
// The difference is ProfileHTTPHandler uses cpuprofile.GetCPUProfile to fetch profile data.
func ProfileHTTPHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	sec, err := strconv.ParseInt(r.FormValue("seconds"), 10, 64)
	if sec <= 0 || err != nil {
		sec = 30
	}

	if durationExceedsWriteTimeout(r, float64(sec)) {
		serveError(w, http.StatusBadRequest, "profile duration exceeds server's WriteTimeout")
		return
	}

	// Set Content Type assuming StartCPUProfile will work,
	// because if it does it starts writing.
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="profile"`)

	pc := NewPprofAPICollector()
	err = pc.StartCPUProfile(w)
	if err != nil {
		serveError(w, http.StatusInternalServerError, "Could not enable CPU profiling: "+err.Error())
		return
	}
	// TODO: fix me.
	// This can be fixed by always starts a 1 second profiling one by one,
	// but to aggregate (merge) multiple profiles into one according to the precision.
	//  |<-- 1s -->|
	// -|----------|----------|----------|----------|----------|-----------|-----> Background profile task timeline.
	//                            |________________________________|
	//       (start cpu profile)  v                                v (stop cpu profile)    // expected profile timeline
	//                        |________________________________|                           // actual profile timeline
	time.Sleep(time.Second * time.Duration(sec))
	err = pc.StopCPUProfile()
	if err != nil {
		serveError(w, http.StatusInternalServerError, "Could not enable CPU profiling: "+err.Error())
	}
}

const (
	labelSQLDigest  = "sql_digest"
	labelPlanDigest = "plan_digest"
)

// PprofAPICollector is a cpu profile consumer for Pprof API usage.
type PprofAPICollector struct {
	ctx       context.Context
	cancel    context.CancelFunc
	started   atomic.Bool
	firstRead chan struct{}
	wg        sync.WaitGroup

	dataCh   ProfileConsumer
	profiles []*ProfileData
	writer   io.Writer
}

// NewPprofAPICollector returns a new NewPprofAPICollector.
func NewPprofAPICollector() *PprofAPICollector {
	ctx, cancel := context.WithCancel(context.Background())
	return &PprofAPICollector{
		ctx:       ctx,
		cancel:    cancel,
		firstRead: make(chan struct{}),
		dataCh:    make(ProfileConsumer, 1),
	}
}

// StartCPUProfile is a substitute for the `pprof.StartCPUProfile` function.
// You should use this function instead of `pprof.StartCPUProfile`.
// Otherwise you may fail, or affect the TopSQL feature and pprof profile HTTP API .
func (pc *PprofAPICollector) StartCPUProfile(w io.Writer) error {
	if !pc.started.CAS(false, true) {
		return errors.New("PprofAPICollector already started")
	}
	pc.writer = w
	pc.wg.Add(1)
	go goutil.WithRecovery(pc.readProfileData, nil)
	return nil
}

// StopCPUProfile is a substitute for the `pprof.StopCPUProfile` function.
func (pc *PprofAPICollector) StopCPUProfile() error {
	if !pc.started.Load() {
		return nil
	}

	// wait for reading least 1 profile data.
	<-pc.firstRead
	pc.cancel()
	pc.wg.Wait()

	data, err := pc.buildProfileData()
	if err != nil {
		return err
	}
	return data.Write(pc.writer)
}

// WaitProfilingFinish waits for collecting `seconds` profile data finished.
func (pc *PprofAPICollector) readProfileData() {
	// register cpu profile consumer.
	Register(pc.dataCh)
	defer func() {
		Unregister(pc.dataCh)
		pc.wg.Done()
	}()

	pc.profiles = []*ProfileData{}
	for {
		select {
		case <-pc.ctx.Done():
			return
		case data := <-pc.dataCh:
			pc.profiles = append(pc.profiles, data)
			if len(pc.profiles) == 1 {
				close(pc.firstRead)
			}
		}
	}
}

func (pc *PprofAPICollector) buildProfileData() (*profile.Profile, error) {
	ds := make([]*profile.Profile, 0, len(pc.profiles))
	for _, data := range pc.profiles {
		if data.Error != nil {
			return nil, data.Error
		}
		pd, err := profile.ParseData(data.Data.Bytes())
		if err != nil {
			return nil, err
		}
		ds = append(ds, pd)
	}

	profileData, err := profile.Merge(ds)
	if err != nil {
		return nil, err
	}
	pc.removeLabel(profileData)
	return profileData, nil
}

// removeLabel uses to remove the sql_digest and plan_digest labels for pprof cpu profile data.
// Since TopSQL will set the sql_digest and plan_digest label, they are strange for other users.
func (pc *PprofAPICollector) removeLabel(profileData *profile.Profile) {
	for _, s := range profileData.Sample {
		for k := range s.Label {
			if k == labelSQLDigest || k == labelPlanDigest {
				delete(s.Label, k)
			}
		}
	}
}

func durationExceedsWriteTimeout(r *http.Request, seconds float64) bool {
	srv, ok := r.Context().Value(http.ServerContextKey).(*http.Server)
	return ok && srv.WriteTimeout != 0 && seconds >= srv.WriteTimeout.Seconds()
}

func serveError(w http.ResponseWriter, status int, txt string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Go-Pprof", "1")
	w.Header().Del("Content-Disposition")
	w.WriteHeader(status)
	_, err := fmt.Fprintln(w, txt)
	if err != nil {
		logutil.BgLogger().Info("write http response error", zap.Error(err))
	}
}
