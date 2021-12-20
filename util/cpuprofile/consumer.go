package cpuprofile

import (
	"errors"
	"time"

	"github.com/google/pprof/profile"
)

const (
	labelSQLDigest  = "sql_digest"
	labelPlanDigest = "plan_digest"
)

var defProfileTimeout = time.Second * 10

type PprofAPIConsumer struct {
	seconds  uint64
	cumulate time.Duration
	dataCh   ProfileConsumer
	profiles []*profile.Profile
}

func NewPprofAPIConsumer(seconds uint64) *PprofAPIConsumer {
	return &PprofAPIConsumer{
		seconds:  seconds,
		dataCh:   make(ProfileConsumer, 1),
		profiles: make([]*profile.Profile, 0, seconds),
	}
}

func (pc *PprofAPIConsumer) WaitProfilingFinish() (*profile.Profile, error) {
	Register(pc.dataCh)
	defer Unregister(pc.dataCh)

	profileDuration := time.Second * time.Duration(pc.seconds)
	timeoutCh := time.After(profileDuration + defProfileTimeout)
	for {
		select {
		case <-timeoutCh:
			return nil, errors.New("profiling failed, should never happen")
		case data := <-pc.dataCh:
			err := pc.handleProfileData(data)
			if err != nil {
				return nil, err
			}
			if pc.cumulate >= profileDuration {
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
	pc.cumulate += data.End.Sub(data.Begin)
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
