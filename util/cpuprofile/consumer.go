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
	"time"

	"github.com/google/pprof/profile"
)

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
