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
	"time"

	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// GetCPUProfile uses to get the cpu profile data from GlobalCPUProfiler.
// You should use GetCPUProfile instead of `pprof.StartCPUProfile`, `pprof.StopCPUProfile`.
// Otherwise you may fail, or affect the TopSQL feature and pprof profile HTTP API .
func GetCPUProfile(ctx context.Context, seconds uint64, w io.Writer) error {
	pc := NewPprofAPIConsumer()
	profileData, err := pc.WaitProfilingFinish(ctx, seconds)
	if err != nil {
		return err
	}
	return profileData.Write(w)
}

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

	// TODO: fix me.
	// This can be fixed by always starts a 1 second profiling one by one,
	// but to aggregate (merge) multiple profiles into one according to the precision.
	//  |<-- 1s -->|
	// -|----------|----------|----------|----------|----------|-----------|-----> Background profile task timeline.
	//                            |________________________________|
	//       (start cpu profile)  v                                v (stop cpu profile)    // expected profile timeline
	//                        |________________________________|                           // actual profile timeline
	err = GetCPUProfile(r.Context(), uint64(sec), w)
	if err != nil {
		serveError(w, http.StatusInternalServerError, "Could not enable CPU profiling: "+err.Error())
	}
}

const (
	labelSQLDigest  = "sql_digest"
	labelPlanDigest = "plan_digest"
)

var defProfileTimeout = time.Second * 10

// PprofAPIConsumer is a cpu profile consumer for Pprof API usage.
type PprofAPIConsumer struct {
	dataCh   ProfileConsumer
	profiles []*profile.Profile
}

// NewPprofAPIConsumer returns a new NewPprofAPIConsumer.
func NewPprofAPIConsumer() *PprofAPIConsumer {
	return &PprofAPIConsumer{
		dataCh: make(ProfileConsumer, 1),
	}
}

// WaitProfilingFinish waits for collecting `seconds` profile data finished.
func (pc *PprofAPIConsumer) WaitProfilingFinish(ctx context.Context, seconds uint64) (*profile.Profile, error) {
	// register cpu profile consumer.
	Register(pc.dataCh)
	defer Unregister(pc.dataCh)

	cumulate := time.Duration(0)
	pc.profiles = make([]*profile.Profile, 0, int(seconds))
	profileDuration := time.Second * time.Duration(seconds)
	timeoutCh := time.After(profileDuration + defProfileTimeout)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeoutCh:
			return nil, errors.New("profiling failed, should never happen")
		case data := <-pc.dataCh:
			err := pc.handleProfileData(data)
			if err != nil {
				return nil, err
			}
			cumulate += data.End.Sub(data.Begin)
			if cumulate >= profileDuration {
				return pc.getMergedProfile()
			}
		}
	}
}

func (pc *PprofAPIConsumer) handleProfileData(data *ProfileData) error {
	if data.Error != nil {
		return data.Error
	}
	pd, err := profile.ParseData(data.Data.Bytes())
	if err != nil {
		return err
	}
	pc.profiles = append(pc.profiles, pd)
	return nil
}

func (pc *PprofAPIConsumer) getMergedProfile() (*profile.Profile, error) {
	profileData, err := profile.Merge(pc.profiles)
	if err != nil {
		return nil, err
	}
	pc.removeLabel(profileData)
	return profileData, nil
}

// removeLabel uses to remove the sql_digest and plan_digest labels for pprof cpu profile data.
// Since TopSQL will set the sql_digest and plan_digest label, they are strange for other users.
func (pc *PprofAPIConsumer) removeLabel(profileData *profile.Profile) {
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
