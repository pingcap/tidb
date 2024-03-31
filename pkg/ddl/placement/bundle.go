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
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	pd "github.com/tikv/pd/client/http"
	"gopkg.in/yaml.v2"
)

// Bundle is a group of all rules and configurations. It is used to support rule cache.
// Alias `pd.GroupBundle` is to wrap more methods.
type Bundle pd.GroupBundle

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
	leaderConst := options.LeaderConstraints
	learnerConstraints := options.LearnerConstraints
	followerConstraints := options.FollowerConstraints
	explicitFollowerCount := options.Followers
	explicitLearnerCount := options.Learners

	rules := []*pd.Rule{}
	commonConstraints, err := NewConstraintsFromYaml([]byte(constraints))
	if err != nil {
		// If it's not in array format, attempt to parse it as a dictionary for more detailed definitions.
		// The dictionary format specifies details for each replica. Constraints are used to define normal
		// replicas that should act as voters.
		// For example: CONSTRAINTS='{ "+region=us-east-1":2, "+region=us-east-2": 2, "+region=us-west-1": 1}'
		normalReplicasRules, err := NewRuleBuilder().
			SetRole(pd.Voter).
			SetConstraintStr(constraints).
			BuildRulesWithDictConstraintsOnly()
		if err != nil {
			return nil, err
		}
		rules = append(rules, normalReplicasRules...)
	}
	needCreateDefault := len(rules) == 0
	leaderConstraints, err := NewConstraintsFromYaml([]byte(leaderConst))
	if err != nil {
		return nil, fmt.Errorf("%w: 'LeaderConstraints' should be [constraint1, ...] or any yaml compatible array representation", err)
	}
	for _, cnst := range commonConstraints {
		if err := AddConstraint(&leaderConstraints, cnst); err != nil {
			return nil, fmt.Errorf("%w: LeaderConstraints conflicts with Constraints", err)
		}
	}
	leaderReplicas, followerReplicas := uint64(1), uint64(2)
	if explicitFollowerCount > 0 {
		followerReplicas = explicitFollowerCount
	}
	if !needCreateDefault {
		if len(leaderConst) == 0 {
			leaderReplicas = 0
		}
		if len(followerConstraints) == 0 {
			if explicitFollowerCount > 0 {
				return nil, fmt.Errorf("%w: specify follower count without specify follower constraints when specify other constraints", ErrInvalidPlacementOptions)
			}
			followerReplicas = 0
		}
	}

	// create leader rule.
	// if no constraints, we need create default leader rule.
	if leaderReplicas > 0 {
		leaderRule := NewRule(pd.Leader, leaderReplicas, leaderConstraints)
		rules = append(rules, leaderRule)
	}

	// create follower rules.
	// if no constraints, we need create default follower rules.
	if followerReplicas > 0 {
		builder := NewRuleBuilder().
			SetRole(pd.Voter).
			SetReplicasNum(followerReplicas).
			SetSkipCheckReplicasConsistent(needCreateDefault && (explicitFollowerCount == 0)).
			SetConstraintStr(followerConstraints)
		followerRules, err := builder.BuildRules()
		if err != nil {
			return nil, fmt.Errorf("%w: invalid FollowerConstraints", err)
		}
		for _, followerRule := range followerRules {
			for _, cnst := range commonConstraints {
				if err := AddConstraint(&followerRule.LabelConstraints, cnst); err != nil {
					return nil, fmt.Errorf("%w: FollowerConstraints conflicts with Constraints", err)
				}
			}
		}
		rules = append(rules, followerRules...)
	}

	// create learner rules.
	builder := NewRuleBuilder().
		SetRole(pd.Learner).
		SetReplicasNum(explicitLearnerCount).
		SetConstraintStr(learnerConstraints)
	learnerRules, err := builder.BuildRules()
	if err != nil {
		return nil, fmt.Errorf("%w: invalid LearnerConstraints", err)
	}
	for _, rule := range learnerRules {
		for _, cnst := range commonConstraints {
			if err := AddConstraint(&rule.LabelConstraints, cnst); err != nil {
				return nil, fmt.Errorf("%w: LearnerConstraints conflicts with Constraints", err)
			}
		}
	}
	rules = append(rules, learnerRules...)
	labels, err := newLocationLabelsFromSurvivalPreferences(options.SurvivalPreferences)
	if err != nil {
		return nil, err
	}
	for _, rule := range rules {
		rule.LocationLabels = labels
	}
	return &Bundle{Rules: rules}, nil
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

	var rules []*pd.Rule

	locationLabels, err := newLocationLabelsFromSurvivalPreferences(options.SurvivalPreferences)
	if err != nil {
		return nil, err
	}

	// in case empty primaryRegion and regions, just return an empty bundle
	if primaryRegion == "" && len(regions) == 0 {
		rules = append(rules, NewRule(pd.Voter, followers+1, NewConstraintsDirect()))
		for _, rule := range rules {
			rule.LocationLabels = locationLabels
		}
		return &Bundle{Rules: rules}, nil
	}

	// regions must include the primary
	slices.Sort(regions)
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

	rules = append(rules, NewRule(pd.Leader, 1, NewConstraintsDirect(NewConstraintDirect("region", pd.In, primaryRegion))))
	if primaryCount > 1 {
		rules = append(rules, NewRule(pd.Voter, primaryCount-1, NewConstraintsDirect(NewConstraintDirect("region", pd.In, primaryRegion))))
	}
	if cnt := followers + 1 - primaryCount; cnt > 0 {
		// delete primary from regions
		regions = regions[:primaryIndex+copy(regions[primaryIndex:], regions[primaryIndex+1:])]
		if len(regions) > 0 {
			rules = append(rules, NewRule(pd.Voter, cnt, NewConstraintsDirect(NewConstraintDirect("region", pd.In, regions...))))
		} else {
			rules = append(rules, NewRule(pd.Voter, cnt, NewConstraintsDirect()))
		}
	}

	// set location labels
	for _, rule := range rules {
		rule.LocationLabels = locationLabels
	}

	return &Bundle{Rules: rules}, nil
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

// newLocationLabelsFromSurvivalPreferences will parse the survival preferences into location labels.
func newLocationLabelsFromSurvivalPreferences(survivalPreferenceStr string) ([]string, error) {
	if len(survivalPreferenceStr) > 0 {
		labels := []string{}
		err := yaml.UnmarshalStrict([]byte(survivalPreferenceStr), &labels)
		if err != nil {
			return nil, ErrInvalidSurvivalPreferenceFormat
		}
		return labels, nil
	}
	return nil, nil
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
	tempRules := b.Rules[:0]
	id := 0
	for _, rule := range b.Rules {
		// useless Rule
		if rule.Count <= 0 {
			continue
		}
		// refer to tidb#22065.
		// add -engine=tiflash to every rule to avoid schedules to tiflash instances.
		// placement rules in SQL is not compatible with `set tiflash replica` yet
		err := AddConstraint(&rule.LabelConstraints, pd.LabelConstraint{
			Op:     pd.NotIn,
			Key:    EngineLabelKey,
			Values: []string{EngineLabelTiFlash},
		})
		if err != nil {
			return err
		}
		rule.ID = strconv.Itoa(id)
		tempRules = append(tempRules, rule)
		id++
	}

	groups := make(map[string]*constraintsGroup)
	finalRules := tempRules[:0]
	for _, rule := range tempRules {
		key := ConstraintsFingerPrint(&rule.LabelConstraints)
		existing, ok := groups[key]
		if !ok {
			groups[key] = &constraintsGroup{rules: []*pd.Rule{rule}}
			continue
		}
		existing.rules = append(existing.rules, rule)
	}
	for _, group := range groups {
		group.MergeRulesByRole()
	}
	if err := transformableLeaderConstraint(groups); err != nil {
		return err
	}
	for _, group := range groups {
		finalRules = append(finalRules, group.rules...)
	}
	// sort by id
	sort.SliceStable(finalRules, func(i, j int) bool {
		return finalRules[i].ID < finalRules[j].ID
	})
	b.Rules = finalRules
	return nil
}

// constraintsGroup is a group of rules with the same constraints.
type constraintsGroup struct {
	rules []*pd.Rule
	// canBecameLeader means the group has leader/voter role,
	// it's valid if it has leader.
	canBecameLeader bool
	// isLeaderGroup means it has specified leader role in this group.
	isLeaderGroup bool
}

func transformableLeaderConstraint(groups map[string]*constraintsGroup) error {
	var leaderGroup *constraintsGroup
	canBecameLeaderNum := 0
	for _, group := range groups {
		if group.isLeaderGroup {
			if leaderGroup != nil {
				return ErrInvalidPlacementOptions
			}
			leaderGroup = group
		}
		if group.canBecameLeader {
			canBecameLeaderNum++
		}
	}
	// If there is a specified group should have leader, and only this group can be a leader, that means
	// the leader's priority is certain, so we can merge the transformable rules into one.
	// eg:
	//  - [ group1 (L F), group2 (F) ], after merging is [group1 (2*V), group2 (F)], we still know the leader prefers group1.
	//  - [ group1 (L F), group2 (V) ], after merging is [group1 (2*V), group2 (V)], we can't know leader priority after merge.
	if leaderGroup != nil && canBecameLeaderNum == 1 {
		leaderGroup.MergeTransformableRoles()
	}
	return nil
}

// MergeRulesByRole merges the rules with the same role.
func (c *constraintsGroup) MergeRulesByRole() {
	// Create a map to store rules by role
	rulesByRole := make(map[pd.PeerRoleType][]*pd.Rule)

	// Iterate through each rule
	for _, rule := range c.rules {
		// Add the rule to the map based on its role
		rulesByRole[rule.Role] = append(rulesByRole[rule.Role], rule)
		if rule.Role == pd.Leader || rule.Role == pd.Voter {
			c.canBecameLeader = true
		}
		if rule.Role == pd.Leader {
			c.isLeaderGroup = true
		}
	}

	// Clear existing rules
	c.rules = nil

	// Iterate through each role and merge the rules
	for _, rules := range rulesByRole {
		mergedRule := rules[0]
		for i, rule := range rules {
			if i == 0 {
				continue
			}
			mergedRule.Count += rule.Count
			if mergedRule.ID > rule.ID {
				mergedRule.ID = rule.ID
			}
		}
		c.rules = append(c.rules, mergedRule)
	}
}

// MergeTransformableRoles merges all the rules to one that can be transformed to other roles.
func (c *constraintsGroup) MergeTransformableRoles() {
	if len(c.rules) == 0 || len(c.rules) == 1 {
		return
	}
	var mergedRule *pd.Rule
	newRules := make([]*pd.Rule, 0, len(c.rules))
	for _, rule := range c.rules {
		// Learner is not transformable, it should be promote by PD.
		if rule.Role == pd.Learner {
			newRules = append(newRules, rule)
			continue
		}
		if mergedRule == nil {
			mergedRule = rule
			continue
		}
		mergedRule.Count += rule.Count
		if mergedRule.ID > rule.ID {
			mergedRule.ID = rule.ID
		}
	}
	if mergedRule != nil {
		mergedRule.Role = pd.Voter
		newRules = append(newRules, mergedRule)
	}
	c.rules = newRules
}

// GetRangeStartAndEndKeyHex get startKeyHex and endKeyHex of range by rangeBundleID.
func GetRangeStartAndEndKeyHex(rangeBundleID string) (startKey string, endKey string) {
	startKey, endKey = "", ""
	if rangeBundleID == TiDBBundleRangePrefixForMeta {
		startKey = hex.EncodeToString(metaPrefix)
		endKey = hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(0)))
	}
	return startKey, endKey
}

// RebuildForRange rebuilds the bundle for system range.
func (b *Bundle) RebuildForRange(rangeName string, policyName string) *Bundle {
	rule := b.Rules
	switch rangeName {
	case KeyRangeGlobal:
		b.ID = TiDBBundleRangePrefixForGlobal
		b.Index = RuleIndexKeyRangeForGlobal
	case KeyRangeMeta:
		b.ID = TiDBBundleRangePrefixForMeta
		b.Index = RuleIndexKeyRangeForMeta
	}

	startKey, endKey := GetRangeStartAndEndKeyHex(b.ID)
	b.Override = true
	newRules := make([]*pd.Rule, 0, len(rule))
	for i, r := range b.Rules {
		cp := r.Clone()
		cp.ID = fmt.Sprintf("%s_rule_%d", strings.ToLower(policyName), i)
		cp.GroupID = b.ID
		cp.StartKeyHex = startKey
		cp.EndKeyHex = endKey
		cp.Index = i
		newRules = append(newRules, cp)
	}
	b.Rules = newRules
	return b
}

// Reset resets the bundle ID and keyrange of all rules.
func (b *Bundle) Reset(ruleIndex int, newIDs []int64) *Bundle {
	// eliminate the redundant rules.
	var basicRules []*pd.Rule
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
	newRules := make([]*pd.Rule, 0, len(basicRules)*len(newIDs))
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
		newBundle.Rules = make([]*pd.Rule, 0, len(b.Rules))
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

func isValidLeaderRule(rule *pd.Rule, dcLabelKey string) bool {
	if rule.Role == pd.Leader && rule.Count == 1 {
		for _, con := range rule.LabelConstraints {
			if con.Op == pd.In && con.Key == dcLabelKey && len(con.Values) == 1 {
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
			return rule.LabelConstraints[0].Values[0], true
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
