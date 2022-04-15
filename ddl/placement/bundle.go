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
	"github.com/pingcap/tidb/parser/model"
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
	Rules = append(Rules, NewRule(Leader, 1, LeaderConstraints))

	FollowerRules, err := NewRules(Voter, followerCount, followerConstraints)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid FollowerConstraints", err)
	}
	for _, rule := range FollowerRules {
		// give a default of 2 followers
		if rule.Count == 0 {
			rule.Count = 2
		}
		for _, cnst := range CommonConstraints {
			if err := rule.Constraints.Add(cnst); err != nil {
				return nil, fmt.Errorf("%w: FollowerConstraints conflicts with Constraints", err)
			}
		}
	}
	Rules = append(Rules, FollowerRules...)

	LearnerRules, err := NewRules(Learner, learnerCount, learnerConstraints)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid LearnerConstraints", err)
	}
	for _, rule := range LearnerRules {
		if rule.Count == 0 {
			if len(rule.Constraints) > 0 {
				return nil, fmt.Errorf("%w: specify learner constraints without specify how many learners to be placed", ErrInvalidPlacementOptions)
			}
		}
		for _, cnst := range CommonConstraints {
			if err := rule.Constraints.Add(cnst); err != nil {
				return nil, fmt.Errorf("%w: LearnerConstraints conflicts with Constraints", err)
			}
		}
		if rule.Count > 0 {
			Rules = append(Rules, rule)
		}
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

	var Rules []*Rule

	// in case empty primaryRegion and regions, just return an empty bundle
	if primaryRegion == "" && len(regions) == 0 {
		Rules = append(Rules, NewRule(Voter, followers+1, NewConstraintsDirect()))
		return &Bundle{Rules: Rules}, nil
	}

	// regions must include the primary
	sort.Strings(regions)
	primaryIndex := sort.SearchStrings(regions, primaryRegion)
	if primaryIndex >= len(regions) || regions[primaryIndex] != primaryRegion {
		return nil, fmt.Errorf("%w: primary region must be included in regions", ErrInvalidPlacementOptions)
	}

	// primaryCount only makes sense when len(regions) > 0
	// but we will compute it here anyway for reusing code
	var primaryCount uint64
	switch strings.ToLower(schedule) {
	case "", "even":
		primaryCount = uint64(math.Ceil(float64(followers+1) / float64(len(regions))))
	case "majority_in_primary":
		// calculate how many replicas need to be in the primary region for quorum
		primaryCount = (followers+1)/2 + 1
	default:
		return nil, fmt.Errorf("%w: unsupported schedule %s", ErrInvalidPlacementOptions, schedule)
	}

	Rules = append(Rules, NewRule(Voter, primaryCount, NewConstraintsDirect(NewConstraintDirect("region", In, primaryRegion))))
	if followers+1 > primaryCount {
		// delete primary from regions
		regions = regions[:primaryIndex+copy(regions[primaryIndex:], regions[primaryIndex+1:])]

		if len(regions) > 0 {
			Rules = append(Rules, NewRule(Follower, followers+1-primaryCount, NewConstraintsDirect(NewConstraintDirect("region", In, regions...))))
		} else {
			Rules = append(Rules, NewRule(Follower, followers+1-primaryCount, NewConstraintsDirect()))
		}
	}

	return &Bundle{Rules: Rules}, nil
}

// Non-Exported functionality function, do not use it directly but NewBundleFromOptions
// here is for only directly used in the test.
func newBundleFromOptions(options *model.PlacementSettings) (bundle *Bundle, err error) {
	if options == nil {
		return nil, fmt.Errorf("%w: options can not be nil", ErrInvalidPlacementOptions)
	}

	if options.Followers > uint64(8) {
		return nil, fmt.Errorf("%w: followers should be less than or equal to 8: %d", ErrInvalidPlacementOptions, options.Followers)
	}

	// always prefer the sugar syntax, which gives better schedule results most of the time
	isSyntaxSugar := true
	if len(options.LeaderConstraints) > 0 || len(options.LearnerConstraints) > 0 || len(options.FollowerConstraints) > 0 || len(options.Constraints) > 0 || options.Learners > 0 {
		isSyntaxSugar = false
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
		for j, rule := range basicRules {
			clone := rule.Clone()
			// for the rules of one element id, distinguishing the rule ids to avoid the PD's overlap.
			clone.ID = ruleID + "_" + strconv.FormatInt(int64(j), 10)
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

// PolicyGetter is the interface to get the policy
type PolicyGetter interface {
	GetPolicy(policyID int64) (*model.PolicyInfo, error)
}

// NewTableBundle creates a bundle for table key range.
// If table is a partitioned table, it also contains the rules that inherited from table for every partition.
// The bundle does not contain the rules specified independently by each partition
func NewTableBundle(getter PolicyGetter, tbInfo *model.TableInfo) (*Bundle, error) {
	bundle, err := newBundleFromPolicy(getter, tbInfo.PlacementPolicyRef)
	if err != nil {
		return nil, err
	}

	if bundle == nil {
		return nil, nil
	}
	ids := []int64{tbInfo.ID}
	// build the default partition rules in the table-level bundle.
	if tbInfo.Partition != nil {
		for _, pDef := range tbInfo.Partition.Definitions {
			ids = append(ids, pDef.ID)
		}
	}
	bundle.Reset(RuleIndexTable, ids)
	return bundle, nil
}

// NewPartitionBundle creates a bundle for partition key range.
// It only contains the rules specified independently by the partition.
// That is to say the inherited rules from table is not included.
func NewPartitionBundle(getter PolicyGetter, def model.PartitionDefinition) (*Bundle, error) {
	bundle, err := newBundleFromPolicy(getter, def.PlacementPolicyRef)
	if err != nil {
		return nil, err
	}

	if bundle != nil {
		bundle.Reset(RuleIndexPartition, []int64{def.ID})
	}

	return bundle, nil
}

// NewPartitionListBundles creates a bundle list for a partition list
func NewPartitionListBundles(getter PolicyGetter, defs []model.PartitionDefinition) ([]*Bundle, error) {
	bundles := make([]*Bundle, 0, len(defs))
	// If the partition has the placement rules on their own, build the partition-level bundles additionally.
	for _, def := range defs {
		bundle, err := NewPartitionBundle(getter, def)
		if err != nil {
			return nil, err
		}

		if bundle != nil {
			bundles = append(bundles, bundle)
		}
	}
	return bundles, nil
}

// NewFullTableBundles returns a bundle list with both table bundle and partition bundles
func NewFullTableBundles(getter PolicyGetter, tbInfo *model.TableInfo) ([]*Bundle, error) {
	var bundles []*Bundle
	tableBundle, err := NewTableBundle(getter, tbInfo)
	if err != nil {
		return nil, err
	}

	if tableBundle != nil {
		bundles = append(bundles, tableBundle)
	}

	if tbInfo.Partition != nil {
		partitionBundles, err := NewPartitionListBundles(getter, tbInfo.Partition.Definitions)
		if err != nil {
			return nil, err
		}
		bundles = append(bundles, partitionBundles...)
	}

	return bundles, nil
}

func newBundleFromPolicy(getter PolicyGetter, ref *model.PolicyRefInfo) (*Bundle, error) {
	if ref != nil {
		policy, err := getter.GetPolicy(ref.ID)
		if err != nil {
			return nil, err
		}

		return NewBundleFromOptions(policy.PlacementSettings)
	}

	return nil, nil
}
