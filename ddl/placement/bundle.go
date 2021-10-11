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
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
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
func NewBundleFromConstraintsOptions(options *model.PlacementSettings) (*Bundle, error) {
	if options == nil {
		return nil, fmt.Errorf("%w: options can not be nil", ErrInvalidPlacementOptions)
	}

	if len(options.PrimaryRegion) > 0 || len(options.Regions) > 0 || len(options.Schedule) > 0 {
		return nil, fmt.Errorf("%w: should be [LEADER/VOTER/LEARNER/FOLLOWER]_CONSTRAINTS=.. [VOTERS/FOLLOWERS/LEARNERS]=.., mixed other sugar options %s", ErrInvalidPlacementOptions, options)
	}

	constraints := options.Constraints
	leaderConstraints := options.LeaderConstraints
	learnerConstraints := options.LearnerConstraints
	followerConstraints := options.FollowerConstraints
	followerCount := options.Followers
	learnerCount := options.Learners

	CommonConstraints, err := NewConstraintsFromYaml([]byte(constraints))
	if err != nil {
		return nil, fmt.Errorf("%w: 'Constraints' should be [constraint1, ...] or any yaml compatible array representation", err)
	}

	Rules := []*Rule{}

	LeaderConstraints, err := NewConstraintsFromYaml([]byte(leaderConstraints))
	if err != nil {
		return nil, fmt.Errorf("%w: 'LeaderConstraints' should be [constraint1, ...] or any yaml compatible array representation", err)
	}
	for _, cnst := range CommonConstraints {
		if err := LeaderConstraints.Add(cnst); err != nil {
			return nil, fmt.Errorf("%w: LeaderConstraints conflicts with Constraints", err)
		}
	}
	if len(LeaderConstraints) > 0 {
		Rules = append(Rules, NewRule(Leader, 1, LeaderConstraints))
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
func NewBundleFromSugarOptions(options *model.PlacementSettings) (*Bundle, error) {
	if options == nil {
		return nil, fmt.Errorf("%w: options can not be nil", ErrInvalidPlacementOptions)
	}

	if len(options.LeaderConstraints) > 0 || len(options.LearnerConstraints) > 0 || len(options.FollowerConstraints) > 0 || len(options.Constraints) > 0 || options.Learners > 0 {
		return nil, fmt.Errorf("%w: should be PRIMARY_REGION=.. REGIONS=.. FOLLOWERS=.. SCHEDULE=.., mixed other constraints into options %s", ErrInvalidPlacementOptions, options)
	}

	primaryRegion := strings.TrimSpace(options.PrimaryRegion)
	if len(primaryRegion) == 0 {
		return nil, fmt.Errorf("%w: empty primary region", ErrInvalidPlacementOptions)
	}

	var regions []string
	if k := strings.TrimSpace(options.Regions); len(k) > 0 {
		regions = strings.Split(k, ",")
		for i, r := range regions {
			regions[i] = strings.TrimSpace(r)
		}
	}

	followers := options.Followers
	if followers == 0 {
		followers = 2
	}
	schedule := options.Schedule

	// regions must include the primary
	sort.Strings(regions)
	primaryIndex := sort.SearchStrings(regions, primaryRegion)
	if primaryIndex >= len(regions) || regions[primaryIndex] != primaryRegion {
		return nil, fmt.Errorf("%w: primary region must be included in regions", ErrInvalidPlacementOptions)
	}

	var Rules []*Rule

	switch strings.ToLower(schedule) {
	case "", "even":
		primaryCount := uint64(math.Ceil(float64(followers+1) / float64(len(regions))))
		Rules = append(Rules, NewRule(Voter, primaryCount, NewConstraintsDirect(NewConstraintDirect("region", In, primaryRegion))))

		if len(regions) > 1 {
			// delete primary from regions
			regions = regions[:primaryIndex+copy(regions[primaryIndex:], regions[primaryIndex+1:])]
			Rules = append(Rules, NewRule(Follower, followers+1-primaryCount, NewConstraintsDirect(NewConstraintDirect("region", In, regions...))))
		}
	case "majority_in_primary":
		// calculate how many replicas need to be in the primary region for quorum
		primaryCount := uint64(math.Ceil(float64(followers+1)/2 + 1))
		Rules = append(Rules, NewRule(Voter, primaryCount, NewConstraintsDirect(NewConstraintDirect("region", In, primaryRegion))))

		if len(regions) > 1 {
			// delete primary from regions
			regions = regions[:primaryIndex+copy(regions[primaryIndex:], regions[primaryIndex+1:])]
			Rules = append(Rules, NewRule(Follower, followers+1-primaryCount, NewConstraintsDirect(NewConstraintDirect("region", In, regions...))))
		}
	default:
		return nil, fmt.Errorf("%w: unsupported schedule %s", ErrInvalidPlacementOptions, schedule)
	}

	return &Bundle{Rules: Rules}, nil
}

// Non-Exported functionality function, do not use it directly but NewBundleFromOptions
// here is for only directly used in the test.
func newBundleFromOptions(options *model.PlacementSettings) (bundle *Bundle, err error) {
	var isSyntaxSugar bool

	if options == nil {
		return nil, fmt.Errorf("%w: options can not be nil", ErrInvalidPlacementOptions)
	}

	if len(options.PrimaryRegion) > 0 || len(options.Regions) > 0 || len(options.Schedule) > 0 {
		isSyntaxSugar = true
	}

	if isSyntaxSugar {
		bundle, err = NewBundleFromSugarOptions(options)
	} else {
		bundle, err = NewBundleFromConstraintsOptions(options)
	}
	return bundle, err
}

// NewBundleFromOptions will transform options into the bundle.
func NewBundleFromOptions(options *model.PlacementSettings) (bundle *Bundle, err error) {
	bundle, err = newBundleFromOptions(options)
	if err != nil {
		return nil, err
	}
	if bundle == nil {
		return nil, nil
	}
	err = bundle.Tidy()
	if err != nil {
		return nil, err
	}
	return bundle, err
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
func (b *Bundle) Reset(ruleIndex int, newIDs []int64) *Bundle {
	// eliminate the redundant rules.
	var basicRules []*Rule
	if len(b.Rules) != 0 {
		// Make priority for rules with RuleIndexTable cause of duplication rules existence with RuleIndexPartition.
		// If RuleIndexTable doesn't exist, bundle itself is a independent series of rules for a partition.
		for _, rule := range b.Rules {
			if rule.Index == RuleIndexTable {
				basicRules = append(basicRules, rule)
			}
		}
		if len(basicRules) == 0 {
			basicRules = b.Rules
		}
	}

	// extend and reset basic rules for all new ids, the first id should be the group id.
	b.ID = GroupID(newIDs[0])
	b.Index = ruleIndex
	b.Override = true
	newRules := make([]*Rule, 0, len(basicRules)*len(newIDs))
	for i, newID := range newIDs {
		// rule.id should be distinguished with each other, otherwise it will be de-duplicated in pd http api.
		var ruleID string
		if ruleIndex == RuleIndexPartition {
			ruleID = "partition_rule_" + strconv.FormatInt(newID, 10)
		} else {
			if i == 0 {
				ruleID = "table_rule_" + strconv.FormatInt(newID, 10)
			} else {
				ruleID = "partition_rule_" + strconv.FormatInt(newID, 10)
			}
		}
		// Involve all the table level objects.
		startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(newID)))
		endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(newID+1)))
		for _, rule := range basicRules {
			clone := rule.Clone()
			clone.ID = ruleID
			clone.GroupID = b.ID
			clone.StartKeyHex = startKey
			clone.EndKeyHex = endKey
			if i == 0 {
				clone.Index = RuleIndexTable
			} else {
				clone.Index = RuleIndexPartition
			}
			newRules = append(newRules, clone)
		}
	}
	b.Rules = newRules
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
