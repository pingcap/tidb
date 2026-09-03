// Copyright 2025 PingCAP, Inc.
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

package ddl

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// BuildStorageClassSettingsFromJSON builds storage class settings from a JSON object.
func BuildStorageClassSettingsFromJSON(input json.RawMessage) (*model.StorageClassSettings, error) {
	if input == nil {
		def := model.StorageClassDef{Tier: model.StorageClassTierDefault}
		return &model.StorageClassSettings{
			Defs: []*model.StorageClassDef{&def},
		}, nil
	}

	// Try parsing as a string
	tier, err := getStorageClassTierAsString(input)
	if err == nil {
		if err := checkTier(tier); err != nil {
			return nil, err
		}
		def := model.StorageClassDef{Tier: tier}
		return &model.StorageClassSettings{
			Defs: []*model.StorageClassDef{&def},
		}, nil
	}

	// Try parsing as a single object
	var def model.StorageClassDef
	defErr := decodeStorageClassDef(input, &def)
	if defErr == nil {
		normalizeStorageClassDef(&def)
		if err := checkStorageClassDef(&def); err != nil {
			return nil, err
		}

		return &model.StorageClassSettings{
			Defs: []*model.StorageClassDef{&def},
		}, nil
	}

	var typeError *json.UnmarshalTypeError
	if !errors.As(defErr, &typeError) {
		msg := fmt.Sprintf("invalid storage class def: '%-.192s'", input)
		return nil, dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
	}

	// Try parsing as a slice of objects
	var defs []*model.StorageClassDef
	if err := decodeStorageClassDef(input, &defs); err == nil {
		if err := normalizeStorageClassDefs(defs); err != nil {
			return nil, err
		}
		return &model.StorageClassSettings{
			Defs: defs,
		}, nil
	}

	// If all parsing attempts fail, return an error
	msg := fmt.Sprintf("invalid storage class def: '%-.192s'", input)
	return nil, dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
}

func getStorageClassTierAsString(msg json.RawMessage) (string, error) {
	var tier string
	if err := json.Unmarshal(msg, &tier); err != nil {
		return "", err
	}
	return strings.ToUpper(tier), nil
}

func decodeStorageClassDef(input json.RawMessage, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var extra struct{}
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("invalid storage class def: multiple JSON values")
}

func normalizeStorageClassDefs(defs []*model.StorageClassDef) error {
	for _, def := range defs {
		if def == nil {
			return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs("storage class def must not be null")
		}
		normalizeStorageClassDef(def)
		if err := checkStorageClassDef(def); err != nil {
			return err
		}
	}
	return nil
}

func normalizeStorageClassDef(def *model.StorageClassDef) {
	def.Tier = strings.ToUpper(def.Tier)
	for i, name := range def.NamesIn {
		def.NamesIn[i] = strings.ToLower(name)
	}
	for i := range def.Transitions {
		def.Transitions[i].Tier = strings.ToUpper(def.Transitions[i].Tier)
	}
}

func normalizeStorageClassTier(tier string) (string, error) {
	tier = strings.ToUpper(tier)
	if err := checkTier(tier); err != nil {
		return "", err
	}
	return tier, nil
}

func buildEngineAttributeFromStorageClassTier(tier string) (string, error) {
	tier, err := normalizeStorageClassTier(tier)
	if err != nil {
		return "", err
	}

	storageClass, err := json.Marshal(tier)
	if err != nil {
		return "", err
	}
	attr, err := json.Marshal(&model.EngineAttribute{StorageClass: storageClass})
	if err != nil {
		return "", err
	}
	return string(attr), nil
}

// GetEngineAttributeFromStorageClassTableOptions returns the effective engine
// attribute from table options, normalizing STORAGE_CLASS syntax sugar into the
// underlying ENGINE_ATTRIBUTE JSON form when present.
func GetEngineAttributeFromStorageClassTableOptions(options []*ast.TableOption) (string, bool, error) {
	var lastOpt *ast.TableOption
	var hasEngineAttribute, hasStorageClass bool
	for _, opt := range options {
		switch opt.Tp {
		case ast.TableOptionEngineAttribute:
			hasEngineAttribute = true
			lastOpt = opt
		case ast.TableOptionStorageClass:
			hasStorageClass = true
			lastOpt = opt
		}
	}
	if lastOpt == nil {
		return "", false, nil
	}
	if hasEngineAttribute && hasStorageClass {
		return "", false, errEngineAttributeAndStorageClassConflict()
	}
	for _, opt := range options {
		switch opt.Tp {
		case ast.TableOptionEngineAttribute:
			if err := validateEngineAttributeTableOption(opt.StrValue); err != nil {
				return "", false, err
			}
		case ast.TableOptionStorageClass:
			if _, err := normalizeStorageClassTier(opt.StrValue); err != nil {
				return "", false, err
			}
		}
	}
	if lastOpt.Tp == ast.TableOptionEngineAttribute {
		return lastOpt.StrValue, true, nil
	}

	engineAttribute, err := buildEngineAttributeFromStorageClassTier(lastOpt.StrValue)
	if err != nil {
		return "", false, err
	}
	return engineAttribute, true, nil
}

func validateEngineAttributeTableOption(input string) error {
	attr, err := model.ParseEngineAttributeFromString(input)
	if err != nil {
		return dbterror.ErrEngineAttributeInvalidFormat.GenWithStackByArgs(fmt.Sprintf("'%v'", err))
	}
	if attr.StorageClass == nil {
		return dbterror.ErrUnsupportedEngineAttribute
	}
	_, err = BuildStorageClassSettingsFromJSON(attr.StorageClass)
	return err
}

// CheckStorageClassConflictInAlterTableSpecs rejects ALTER TABLE statements that
// mix raw ENGINE_ATTRIBUTE and STORAGE_CLASS syntax sugar across alter specs.
func CheckStorageClassConflictInAlterTableSpecs(specs []*ast.AlterTableSpec) error {
	var hasEngineAttribute, hasStorageClass bool
	for _, spec := range specs {
		if spec.Tp != ast.AlterTableOption {
			continue
		}
		for _, opt := range spec.Options {
			switch opt.Tp {
			case ast.TableOptionEngineAttribute:
				hasEngineAttribute = true
			case ast.TableOptionStorageClass:
				hasStorageClass = true
			}
		}
	}
	if hasEngineAttribute && hasStorageClass {
		return errEngineAttributeAndStorageClassConflict()
	}
	return nil
}

func errEngineAttributeAndStorageClassConflict() error {
	msg := "can not specify 'ENGINE_ATTRIBUTE' and 'STORAGE_CLASS' together"
	return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
}

// GetSimpleTableStorageClassForShowCreate returns the table-level storage class tier when it can be
// losslessly rendered as the STORAGE_CLASS syntax sugar in SHOW CREATE TABLE.
func GetSimpleTableStorageClassForShowCreate(tbInfo *model.TableInfo) (string, bool, error) {
	if len(tbInfo.EngineAttribute) == 0 {
		return "", false, nil
	}

	storageClass, ok, err := getOnlyStorageClassEngineAttribute(tbInfo.EngineAttribute)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}

	settings, err := BuildStorageClassSettingsFromJSON(storageClass)
	if err != nil {
		return "", false, err
	}
	if len(settings.Defs) != 1 {
		return "", false, nil
	}

	def := settings.Defs[0]
	if def == nil {
		return "", false, nil
	}
	if !def.HasNoScopeDef() || len(def.Transitions) > 0 {
		return "", false, nil
	}
	tier, err := normalizeStorageClassTier(def.Tier)
	if err != nil {
		return "", false, err
	}

	return tier, true, nil
}

func getOnlyStorageClassEngineAttribute(engineAttribute string) (json.RawMessage, bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(engineAttribute), &fields); err != nil {
		return nil, false, err
	}
	storageClass, ok := fields["storage_class"]
	if !ok || len(fields) != 1 {
		return nil, false, nil
	}
	return storageClass, true, nil
}

var tiers = map[string]struct{}{
	model.StorageClassTierStandard: {},
	model.StorageClassTierIA:       {},
}

func checkTier(tier string) error {
	_, ok := tiers[tier]
	if !ok {
		msg := fmt.Sprintf("invalid storage class tier: %s", tier)
		return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
	}
	return nil
}

func checkTransitions(defaultTier string, rules []model.StorageClassTransitRule) error {
	// Currently only STANDARD -> IA is reasonable.
	ok := defaultTier == model.StorageClassTierStandard &&
		len(rules) == 1 &&
		rules[0].Tier == model.StorageClassTierIA &&
		rules[0].TotalSeconds() > 0
	if !ok {
		msg := "only transition from 'STANDARD' to 'IA' is allowed"
		return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
	}

	return nil
}

func checkStorageClassDef(def *model.StorageClassDef) error {
	if err := checkTier(def.Tier); err != nil {
		return err
	}

	if len(def.Transitions) > 0 {
		if err := checkTransitions(def.Tier, def.Transitions); err != nil {
			return err
		}
	}

	scopeFields := 0
	if len(def.NamesIn) > 0 {
		scopeFields++
	}
	if def.LessThan != nil {
		scopeFields++
	}
	if len(def.ValuesIn) > 0 {
		scopeFields++
	}
	if scopeFields > 1 {
		msg := "can not specify 'names_in', 'less_than', or 'values_in' together"
		return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
	}

	return nil
}

func setStorageClassForTable(tbInfo *model.TableInfo, tier string, transitions []model.StorageClassTransitRule) {
	tbInfo.StorageClassTier = tier
	tbInfo.StorageClassTransitions = transitions
	logutil.BgLogger().Info("storage class: set table storage class", zap.Int64("tableID", tbInfo.ID),
		zap.String("tier", tier), zap.Any("transitions", transitions))
}

// BuildStorageClassForTable builds storage class tier for a table.
func BuildStorageClassForTable(tbInfo *model.TableInfo, settings *model.StorageClassSettings) error {
	if settings == nil {
		return nil
	}

	for _, def := range settings.Defs {
		if def.HasNoScopeDef() {
			setStorageClassForTable(tbInfo, def.Tier, def.Transitions)
			return nil
		}
	}

	setStorageClassForTable(tbInfo, model.StorageClassTierDefault, nil)
	return nil
}

func setStorageClassTierForPartition(tbInfo *model.TableInfo, part *model.PartitionDefinition, tier string, transitions []model.StorageClassTransitRule) {
	part.StorageClassTier = tier
	part.StorageClassTransitions = transitions
	logutil.BgLogger().Info("storage class: set partition storage class",
		zap.Int64("tableID", tbInfo.ID), zap.String("partitionName", part.Name.L),
		zap.String("tier", tier), zap.Any("transitions", transitions))
}

// BuildStorageClassForPartitions builds storage class tier for partitions.
func BuildStorageClassForPartitions(
	partitions []model.PartitionDefinition,
	tbInfo *model.TableInfo,
	settings *model.StorageClassSettings,
) error {
	if settings == nil {
		return nil
	}
	for _, def := range settings.Defs {
		if err := checkStorageClassPartitionScope(tbInfo, partitions, def); err != nil {
			return err
		}
	}

	defaultDef := firstNoScopeStorageClassDef(settings.Defs)

PartitionLoop:
	for i := range partitions {
		part := &partitions[i]
		for _, def := range settings.Defs {
			if def.HasNoScopeDef() {
				continue
			}

			if len(def.NamesIn) > 0 {
				if isPartitionMatchNamesIn(part, def.NamesIn) {
					setStorageClassTierForPartition(tbInfo, part, def.Tier, def.Transitions)
					continue PartitionLoop
				}
			}

			if def.LessThan != nil {
				matched, err := isPartitionMatchLessThan(tbInfo, part, *def.LessThan)
				if err != nil {
					return err
				}
				if matched {
					setStorageClassTierForPartition(tbInfo, part, def.Tier, def.Transitions)
					continue PartitionLoop
				}
			}

			if len(def.ValuesIn) > 0 {
				if isPartitionMatchValuesIn(part, def.ValuesIn) {
					setStorageClassTierForPartition(tbInfo, part, def.Tier, def.Transitions)
					continue PartitionLoop
				}
			}
		}

		if defaultDef != nil {
			setStorageClassTierForPartition(tbInfo, part, defaultDef.Tier, defaultDef.Transitions)
			continue
		}
		setStorageClassTierForPartition(tbInfo, part, model.StorageClassTierDefault, nil)
	}

	return nil
}

func firstNoScopeStorageClassDef(defs []*model.StorageClassDef) *model.StorageClassDef {
	for _, def := range defs {
		if def.HasNoScopeDef() {
			return def
		}
	}
	return nil
}

func checkStorageClassPartitionScope(tbInfo *model.TableInfo, partitions []model.PartitionDefinition, def *model.StorageClassDef) error {
	if tbInfo.Partition == nil {
		return nil
	}
	if !def.HasNoScopeDef() &&
		(tbInfo.Partition.Type == ast.PartitionTypeHash || tbInfo.Partition.Type == ast.PartitionTypeKey) {
		return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(
			"partition-scoped storage_class does not support HASH or KEY partitions")
	}
	if def.LessThan != nil {
		if tbInfo.Partition.Type != ast.PartitionTypeRange {
			return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs("'less_than' only supports RANGE partitions")
		}
		for _, part := range partitions {
			if len(part.LessThan) != 1 {
				return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs("'less_than' only supports single-column RANGE partitions")
			}
		}
	}
	if len(def.ValuesIn) > 0 {
		if tbInfo.Partition.Type != ast.PartitionTypeList {
			return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs("'values_in' only supports LIST partitions")
		}
		for _, part := range partitions {
			for _, partValues := range part.InValues {
				if len(partValues) != 1 {
					return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs("'values_in' only supports single-column LIST partitions")
				}
			}
		}
	}
	return nil
}

func isPartitionMatchNamesIn(part *model.PartitionDefinition, namesIn []string) bool {
	intest.Assert(len(namesIn) > 0)

	for _, name := range namesIn {
		if part.Name.L == name {
			return true
		}
	}
	return false
}

func isPartitionMatchLessThan(tbInfo *model.TableInfo, part *model.PartitionDefinition, lessThan string) (bool, error) {
	if len(part.LessThan) != 1 {
		return false, nil
	}
	return partitionValueLessThanOrEqual(tbInfo, part.LessThan[0], lessThan)
}

func isPartitionMatchValuesIn(part *model.PartitionDefinition, valuesIn []string) bool {
	intest.Assert(len(valuesIn) > 0)

	for _, partValues := range part.InValues {
		if len(partValues) != 1 {
			continue
		}
		for _, value := range valuesIn {
			if partitionValueEquals(partValues[0], value) {
				return true
			}
		}
	}
	return false
}

func partitionValueEquals(left, right string) bool {
	if left == right {
		return true
	}
	leftKeyword := isPartitionValueKeyword(left)
	rightKeyword := isPartitionValueKeyword(right)
	if leftKeyword || rightKeyword {
		if !leftKeyword || !rightKeyword {
			return false
		}
		return strings.EqualFold(left, right)
	}
	return driver.UnwrapFromSingleQuotes(left) == driver.UnwrapFromSingleQuotes(right)
}

func compareRangePartitionValues(tbInfo *model.TableInfo, left, right string) (int, error) {
	leftMaxValue := strings.EqualFold(left, partitionMaxValue)
	rightMaxValue := strings.EqualFold(right, partitionMaxValue)
	switch {
	case leftMaxValue && rightMaxValue:
		return 0, nil
	case leftMaxValue:
		return 1, nil
	case rightMaxValue:
		return -1, nil
	}

	if tbInfo != nil && tbInfo.Partition != nil && len(tbInfo.Partition.Columns) > 0 {
		return compareRangeColumnsPartitionValues(tbInfo, left, right)
	}
	return compareNumericRangePartitionValues(tbInfo, left, right)
}

func compareRangeColumnsPartitionValues(tbInfo *model.TableInfo, left, right string) (int, error) {
	colInfo := findColumnByName(tbInfo.Partition.Columns[0].L, tbInfo)
	if colInfo == nil {
		return 0, dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs("'less_than' can not find RANGE COLUMNS partition column")
	}
	ctx := exprstatic.NewExprContext()
	if !isSingleQuotedPartitionValue(right) {
		right = driver.WrapInSingleQuotes(right)
	}
	cmp, err := parseAndEvalBoolExpr(ctx, left, right, colInfo, tbInfo)
	if err != nil {
		msg := fmt.Sprintf("invalid 'less_than' value: %s", right)
		return 0, dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
	}
	return int(cmp), nil
}

func compareNumericRangePartitionValues(tbInfo *model.TableInfo, left, right string) (int, error) {
	ctx := exprstatic.NewExprContext()
	unsigned := false
	if tbInfo != nil && tbInfo.Partition != nil && tbInfo.Partition.Expr != "" {
		unsigned = isPartExprUnsigned(ctx.GetEvalCtx(), tbInfo)
	}
	leftValue, _, err := getRangeValue(ctx, left, unsigned)
	if err != nil {
		msg := fmt.Sprintf("invalid RANGE partition value: %s", left)
		return 0, dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
	}
	rightValue, _, err := getRangeValue(ctx, right, unsigned)
	if err != nil {
		msg := fmt.Sprintf("invalid 'less_than' value: %s", right)
		return 0, dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
	}
	if unsigned {
		return compareUint64(leftValue.(uint64), rightValue.(uint64)), nil
	}
	return compareInt64(leftValue.(int64), rightValue.(int64)), nil
}

func compareUint64(left, right uint64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareInt64(left, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func isSingleQuotedPartitionValue(value string) bool {
	return len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\''
}

func partitionValueLessThanOrEqual(tbInfo *model.TableInfo, left, right string) (bool, error) {
	cmp, err := compareRangePartitionValues(tbInfo, left, right)
	if err != nil {
		return false, err
	}
	return cmp <= 0, nil
}

func isPartitionValueKeyword(value string) bool {
	return strings.EqualFold(value, partitionMaxValue) || strings.EqualFold(value, "DEFAULT")
}
