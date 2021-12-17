package cpuprofile

import (
	"errors"
	"io"
	"time"

	"github.com/google/pprof/profile"
)

const labelSQL = "sql"

var defProfileTimeout = time.Second * 10

func GetCPUProfile(seconds uint64, w io.Writer) error {
	pc := NewPprofAPIConsumer(seconds)
	profileData, err := pc.WaitProfilingFinish()
	if err != nil {
		return err
	}
	return profileData.Write(w)
}

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
	pd, err := data.Parse()
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

func (pc *PprofAPIConsumer) removeLabel(profileData *profile.Profile) {
	for _, s := range profileData.Sample {
		for k := range s.Label {
			if k != labelSQL {
				delete(s.Label, k)
			}
		}
	}
}
