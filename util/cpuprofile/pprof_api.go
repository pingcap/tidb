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

	pc := NewCollector()
	err = pc.StartCPUProfile(w)
	if err != nil {
		serveError(w, http.StatusInternalServerError, "Could not enable CPU profiling: "+err.Error())
		return
	}
	time.Sleep(time.Second * time.Duration(sec))
	err = pc.StopCPUProfile()
	if err != nil {
		serveError(w, http.StatusInternalServerError, "Could not enable CPU profiling: "+err.Error())
	}
}

// Collector is a cpu profile collector, it collect cpu profile data from globalCPUProfiler.
type Collector struct {
	ctx       context.Context
	cancel    context.CancelFunc
	started   bool
	firstRead chan struct{}
	wg        sync.WaitGroup

	dataCh ProfileConsumer
	writer io.Writer

	// Following fields uses to store the result data of collected.
	result *profile.Profile
	err    error
}

// NewCollector returns a new NewCollector.
func NewCollector() *Collector {
	ctx, cancel := context.WithCancel(context.Background())
	return &Collector{
		ctx:       ctx,
		cancel:    cancel,
		firstRead: make(chan struct{}),
		dataCh:    make(ProfileConsumer, 1),
	}
}

// StartCPUProfile is a substitute for the `pprof.StartCPUProfile` function.
// You should use this function instead of `pprof.StartCPUProfile`.
// Otherwise you may fail, or affect the TopSQL feature and pprof profile HTTP API .
// WARN: this function is not thread-safe.
func (pc *Collector) StartCPUProfile(w io.Writer) error {
	if pc.started {
		return errors.New("Collector already started")
	}
	pc.started = true
	pc.writer = w
	pc.wg.Add(1)
	go goutil.WithRecovery(pc.readProfileData, nil)
	return nil
}

// StopCPUProfile is a substitute for the `pprof.StopCPUProfile` function.
// WARN: this function is not thread-safe.
func (pc *Collector) StopCPUProfile() error {
	if !pc.started {
		return nil
	}

	// wait for reading least 1 profile data.
	select {
	case <-pc.firstRead:
	case <-time.After(DefProfileDuration * 2):
	}

	pc.cancel()
	pc.wg.Wait()

	data, err := pc.buildProfileData()
	if err != nil || data == nil {
		return err
	}
	return data.Write(pc.writer)
}

// WaitProfilingFinish waits for collecting `seconds` profile data finished.
func (pc *Collector) readProfileData() {
	// register cpu profile consumer.
	Register(pc.dataCh)
	defer func() {
		Unregister(pc.dataCh)
		pc.wg.Done()
	}()

	pc.result, pc.err = nil, nil
	firstRead := true
	for {
		select {
		case <-pc.ctx.Done():
			return
		case data := <-pc.dataCh:
			pc.err = pc.handleProfileData(data)
			if pc.err != nil {
				return
			}
			if firstRead {
				firstRead = false
				close(pc.firstRead)
			}
		}
	}
}

func (pc *Collector) handleProfileData(data *ProfileData) error {
	if data.Error != nil {
		return data.Error
	}
	pd, err := profile.ParseData(data.Data.Bytes())
	if err != nil {
		return err
	}
	if pc.result == nil {
		pc.result = pd
		return nil
	}
	pc.result, err = profile.Merge([]*profile.Profile{pc.result, pd})
	return err
}

func (pc *Collector) buildProfileData() (*profile.Profile, error) {
	if pc.err != nil || pc.result == nil {
		return nil, pc.err
	}

	pc.removeLabel(pc.result)
	return pc.result, nil
}

const labelSQL = "sql"

// removeLabel uses to remove the sql_digest and plan_digest labels for pprof cpu profile data.
// Since TopSQL will set the sql_digest and plan_digest label, they are strange for other users.
func (pc *Collector) removeLabel(profileData *profile.Profile) {
	for _, s := range profileData.Sample {
		for k := range s.Label {
			if k != labelSQL {
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
