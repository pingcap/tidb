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

package placement

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

// Refer to https://github.com/tikv/pd/issues/2701 .
// IMO, it is indeed not bad to have a copy of definition.
// After all, placement rules are communicated using an HTTP API. Loose
//  coupling is a good feature.

// Bundle is a group of all rules and configurations. It is used to support rule cache.
type Bundle struct {
	ID       string  `json:"group_id"`
	Index    int     `json:"group_index"`
	Override bool    `json:"group_override"`
	Rules    []*Rule `json:"rules"`
}

// NewBundle will create a bundle with the provided ID.
// Note that you should never pass negative id.
func NewBundle(id int64) *Bundle {
	return &Bundle{
		ID: GroupID(id),
	}
}

// NewBundleFromConstraintsOptions will transform constraints options into the bundle.
func NewBundleFromConstraintsOptions(constraints string, leaderConstraints string, learnerConstraints string, followerConstraints string, voterConstraints string, followerCount uint64, voterCount uint64, learnerCount uint64) (*Bundle, error) {
	if voterCount+followerCount == 0 {
		return nil, fmt.Errorf("%w: not enough voter, please specify voter or follower count", ErrInvalidPlacemeOptions)
	}

	CommonConstraints, err := NewConstraintsFromYaml([]byte(constraints))
	if err != nil {
		return nil, fmt.Errorf("%w: 'Constraints' should be [constraint1, ...] or any yaml compatible array representation", err)
	}

	LeaderConstraints, err := NewConstraintsFromYaml([]byte(leaderConstraints))
	if err != nil {
		return nil, fmt.Errorf("%w: 'LeaderConstraints' should be [constraint1, ...] or any yaml compatible array representation", err)
	}
	for _, cnst := range CommonConstraints {
		if err := LeaderConstraints.Add(cnst); err != nil {
			return nil, fmt.Errorf("%w: LeaderConstraints conflicts with Constraints", err)
		}
	}

	Rules := []*Rule{}
	if len(LeaderConstraints) > 0 {
		Rules = append(Rules, NewRule(Leader, 1, LeaderConstraints))
	}

	if voterCount > 0 {
		VoterRules, err := NewRules(Voter, voterCount, voterConstraints)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid VoterConstraints", err)
		}
		for _, rule := range VoterRules {
			for _, cnst := range CommonConstraints {
				if err := rule.Constraints.Add(cnst); err != nil {
					return nil, fmt.Errorf("%w: VoterConstraints conflicts with Constraints", err)
				}
			}
		}
		Rules = append(Rules, VoterRules...)
	}

	if followerCount > 0 {
		FollowerRules, err := NewRules(Follower, followerCount, followerConstraints)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid FollowerConstraints", err)
		}
		for _, rule := range FollowerRules {
			for _, cnst := range CommonConstraints {
				if err := rule.Constraints.Add(cnst); err != nil {
					return nil, fmt.Errorf("%w: FollowerConstraints conflicts with Constraints", err)
				}
			}
		}
		Rules = append(Rules, FollowerRules...)
	}

	if learnerCount > 0 {
		LearnerRules, err := NewRules(Learner, learnerCount, learnerConstraints)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid LearnerConstraints", err)
		}
		for _, rule := range LearnerRules {
			for _, cnst := range CommonConstraints {
				if err := rule.Constraints.Add(cnst); err != nil {
					return nil, fmt.Errorf("%w: LearnerConstraints conflicts with Constraints", err)
				}
			}
		}
		Rules = append(Rules, LearnerRules...)
	}

	return &Bundle{Rules: Rules}, nil
}

// NewBundleFromSugarOptions will transform syntax sugar options into the bundle.
func NewBundleFromSugarOptions(primaryRegion string, regions []string, followers uint64, schedule string) (*Bundle, error) {
	if followers == 0 {
		followers = 2
	}
	var constraints Constraints
	var err error

	if len(primaryRegion) == 0 {
		return nil, fmt.Errorf("%w: empty primary region", ErrInvalidPlacemeOptions)
	}

	Rules := []*Rule{}
	switch strings.ToLower(schedule) {
	case "", "even":
		constraints, err = NewConstraints([]string{fmt.Sprintf("+region=%s", primaryRegion)})
		if err != nil {
			return nil, fmt.Errorf("%w: invalid PrimaryRegion '%s'", err, primaryRegion)
		}
		Rules = append(Rules, NewRule(Leader, 1, constraints))
	case "majority_in_primary":
		// number of quorum members, without the leader
		primary := (followers + 1) / 2
		constraints, err = NewConstraints([]string{fmt.Sprintf("+region=%s", primaryRegion)})
		if err != nil {
			return nil, fmt.Errorf("%w: invalid 'PrimaryRegion', '%s'", err, primaryRegion)
		}
		Rules = append(Rules, NewRule(Leader, 1, constraints))
		Rules = append(Rules, NewRule(Follower, primary, constraints))
		// even split the remaining followers
		followers = followers - primary
	default:
		return nil, fmt.Errorf("%w: unsupported schedule %s", ErrInvalidPlacemeOptions, schedule)
	}

	if uint64(len(regions)) > followers {
		return nil, fmt.Errorf("%w: remain %d region to schedule, only %d follower left", ErrInvalidPlacemeOptions, uint64(len(regions)), followers)
	}

	if len(regions) == 0 {
		constraints, err := NewConstraints(nil)
		if err != nil {
			return nil, err
		}
		Rules = append(Rules, NewRule(Follower, followers, constraints))
	} else {
		count := followers / uint64(len(regions))
		rem := followers - count*uint64(len(regions))
		for _, region := range regions {
			constraints, err = NewConstraints([]string{fmt.Sprintf("+region=%s", region)})
			if err != nil {
				return nil, fmt.Errorf("%w: invalid region of 'Regions', '%s'", err, region)
			}
			replica := count
			if rem > 0 {
				replica += 1
				rem--
			}
			Rules = append(Rules, NewRule(Follower, replica, constraints))
		}
	}

	return &Bundle{Rules: Rules}, nil
}

// NewBundleFromOptions will transform options into the bundle.
func NewBundleFromOptions(options []*ast.PlacementOption) (*Bundle, error) {
	var isSyntaxSugar bool
	if len(options) == 0 {
		return nil, fmt.Errorf("%w: empty options", ErrInvalidPlacemeOptions)
	}
detect:
	for _, opt := range options {
		switch opt.Tp {
		case ast.PlacementOptionFollowerCount:
			// both sugar and non-sugar syntax could contain this option
		case ast.PlacementOptionPolicy:
			// not a direct placement option
			return nil, fmt.Errorf("%w: it is a placement policy reference, not direct options", ErrInvalidPlacemeOptions)
		case ast.PlacementOptionPrimaryRegion, ast.PlacementOptionRegions, ast.PlacementOptionSchedule:
			isSyntaxSugar = true
			break detect
		case ast.PlacementOptionConstraints, ast.PlacementOptionLeaderConstraints, ast.PlacementOptionLearnerConstraints, ast.PlacementOptionFollowerConstraints, ast.PlacementOptionVoterConstraints, ast.PlacementOptionVoterCount, ast.PlacementOptionLearnerCount:
			isSyntaxSugar = false
			break detect
		}
	}
	if isSyntaxSugar {
		var primaryRegion string
		var regions []string
		var schedule string
		var followerCount uint64
		for _, opt := range options {
			switch opt.Tp {
			case ast.PlacementOptionPrimaryRegion:
				primaryRegion = strings.TrimSpace(opt.StrValue)
			case ast.PlacementOptionRegions:
				regions = strings.Split(opt.StrValue, ",")
				for i, r := range regions {
					regions[i] = strings.TrimSpace(r)
				}
			case ast.PlacementOptionFollowerCount:
				followerCount = opt.UintValue
			case ast.PlacementOptionSchedule:
				schedule = strings.ToLower(opt.StrValue)
			default:
				return nil, fmt.Errorf("%w: unsupported option %+v", ErrInvalidPlacemeOptions, opt)
			}
		}
		return NewBundleFromSugarOptions(primaryRegion, regions, followerCount, schedule)
	} else {
		var constraints string
		var leaderConstraints string
		var learnerConstraints string
		var followerConstraints string
		var voterConstraints string
		var learnerCount uint64
		var followerCount uint64
		var voterCount uint64
		for _, opt := range options {
			switch opt.Tp {
			case ast.PlacementOptionConstraints:
				constraints = opt.StrValue
			case ast.PlacementOptionLeaderConstraints:
				leaderConstraints = opt.StrValue
			case ast.PlacementOptionLearnerConstraints:
				learnerConstraints = opt.StrValue
			case ast.PlacementOptionFollowerConstraints:
				followerConstraints = opt.StrValue
			case ast.PlacementOptionVoterConstraints:
				voterConstraints = opt.StrValue
			case ast.PlacementOptionFollowerCount:
				followerCount = opt.UintValue
			case ast.PlacementOptionVoterCount:
				voterCount = opt.UintValue
			case ast.PlacementOptionLearnerCount:
				learnerCount = opt.UintValue
			default:
				return nil, fmt.Errorf("%w: unsupported option %+v", ErrInvalidPlacemeOptions, opt)
			}
		}
		return NewBundleFromConstraintsOptions(constraints, leaderConstraints, learnerConstraints, followerConstraints, voterConstraints, followerCount, voterCount, learnerCount)
	}
}

// ApplyPlacementSpec will apply actions defined in PlacementSpec to the bundle.
func (b *Bundle) ApplyPlacementSpec(specs []*ast.PlacementSpec) error {
	for _, spec := range specs {
		var role PeerRoleType
		switch spec.Role {
		case ast.PlacementRoleFollower:
			role = Follower
		case ast.PlacementRoleLeader:
			if spec.Replicas == 0 {
				spec.Replicas = 1
			}
			if spec.Replicas > 1 {
				return ErrLeaderReplicasMustOne
			}
			role = Leader
		case ast.PlacementRoleLearner:
			role = Learner
		case ast.PlacementRoleVoter:
			role = Voter
		default:
			return ErrMissingRoleField
		}

		if spec.Tp == ast.PlacementAlter || spec.Tp == ast.PlacementDrop {
			origLen := len(b.Rules)
			newRules := b.Rules[:0]
			for _, r := range b.Rules {
				if r.Role != role {
					newRules = append(newRules, r)
				}
			}
			b.Rules = newRules

			// alter == drop + add new rules
			if spec.Tp == ast.PlacementDrop {
				// error if no rules will be dropped
				if len(b.Rules) == origLen {
					return fmt.Errorf("%w: %s", ErrNoRulesToDrop, role)
				}
				continue
			}
		}

		newRules, err := NewRules(role, spec.Replicas, spec.Constraints)
		if err != nil {
			return err
		}
		b.Rules = append(b.Rules, newRules...)
	}

	return nil
}

// String implements fmt.Stringer.
func (b *Bundle) String() string {
	t, err := json.Marshal(b)
	failpoint.Inject("MockMarshalFailure", func(val failpoint.Value) {
		if _, ok := val.(bool); ok {
			err = errors.New("test")
		}
	})
	if err != nil {
		return ""
	}
	return string(t)
}

// Tidy will post optimize Rules, trying to generate rules that suits PD.
func (b *Bundle) Tidy() error {
	extraCnt := map[PeerRoleType]int{}
	newRules := b.Rules[:0]
	for i, rule := range b.Rules {
		// useless Rule
		if rule.Count <= 0 {
			continue
		}
		// merge all empty constraints
		if len(rule.Constraints) == 0 {
			extraCnt[rule.Role] += rule.Count
			continue
		}
		// refer to tidb#22065.
		// add -engine=tiflash to every rule to avoid schedules to tiflash instances.
		// placement rules in SQL is not compatible with `set tiflash replica` yet
		err := rule.Constraints.Add(Constraint{
			Op:     NotIn,
			Key:    EngineLabelKey,
			Values: []string{EngineLabelTiFlash},
		})
		if err != nil {
			return err
		}
		// Constraints.Add() will automatically avoid duplication
		// if -engine=tiflash is added and there is only one constraint
		// then it must be -engine=tiflash
		// it is seen as an empty constraint, so merge it
		if len(rule.Constraints) == 1 {
			extraCnt[rule.Role] += rule.Count
			continue
		}
		rule.ID = strconv.Itoa(i)
		newRules = append(newRules, rule)
	}
	for role, cnt := range extraCnt {
		// add -engine=tiflash, refer to tidb#22065.
		newRules = append(newRules, &Rule{
			ID:    string(role),
			Role:  role,
			Count: cnt,
			Constraints: []Constraint{{
				Op:     NotIn,
				Key:    EngineLabelKey,
				Values: []string{EngineLabelTiFlash},
			}},
		})
	}
	b.Rules = newRules
	return nil
}

// Reset resets the bundle ID and keyrange of all rules.
func (b *Bundle) Reset(newID int64) *Bundle {
	b.ID = GroupID(newID)
	startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(newID)))
	endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTableRecordPrefix(newID+1)))
	for _, rule := range b.Rules {
		rule.GroupID = b.ID
		rule.StartKeyHex = startKey
		rule.EndKeyHex = endKey
	}
	return b
}

// Clone is used to duplicate a bundle.
func (b *Bundle) Clone() *Bundle {
	newBundle := &Bundle{}
	*newBundle = *b
	if len(b.Rules) > 0 {
		newBundle.Rules = make([]*Rule, 0, len(b.Rules))
		for i := range b.Rules {
			newBundle.Rules = append(newBundle.Rules, b.Rules[i].Clone())
		}
	}
	return newBundle
}

// IsEmpty is used to check if a bundle is empty.
func (b *Bundle) IsEmpty() bool {
	return len(b.Rules) == 0 && b.Index == 0 && !b.Override
}

// ObjectID extracts the db/table/partition ID from the group ID
func (b *Bundle) ObjectID() (int64, error) {
	// If the rule doesn't come from TiDB, skip it.
	if !strings.HasPrefix(b.ID, BundleIDPrefix) {
		return 0, ErrInvalidBundleIDFormat
	}
	id, err := strconv.ParseInt(b.ID[len(BundleIDPrefix):], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s", ErrInvalidBundleID, err)
	}
	if id <= 0 {
		return 0, fmt.Errorf("%w: %s doesn't include an id", ErrInvalidBundleID, b.ID)
	}
	return id, nil
}

func isValidLeaderRule(rule *Rule, dcLabelKey string) bool {
	if rule.Role == Leader && rule.Count == 1 {
		for _, con := range rule.Constraints {
			if con.Op == In && con.Key == dcLabelKey && len(con.Values) == 1 {
				return true
			}
		}
	}
	return false
}

// GetLeaderDC returns the leader's DC by Bundle if found.
func (b *Bundle) GetLeaderDC(dcLabelKey string) (string, bool) {
	for _, rule := range b.Rules {
		if isValidLeaderRule(rule, dcLabelKey) {
			return rule.Constraints[0].Values[0], true
		}
	}
	return "", false
}
