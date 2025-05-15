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
	"strings"

	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
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
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()

	var def model.StorageClassDef
	if err := decoder.Decode(&def); err == nil {
		def.Tier = strings.ToUpper(def.Tier)
		if err := checkStorageClassDef(&def); err != nil {
			return nil, err
		}

		for i, name := range def.NamesIn {
			def.NamesIn[i] = strings.ToLower(name)
		}

		return &model.StorageClassSettings{
			Defs: []*model.StorageClassDef{&def},
		}, nil
	}

	var typeError *json.UnmarshalTypeError
	if !errors.As(err, &typeError) {
		msg := fmt.Sprintf("invalid storage class def: '%-.192s'", input)
		return nil, dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
	}

	// Try parsing as a slice of objects
	var defs []*model.StorageClassDef
	if err := json.Unmarshal(input, &defs); err == nil {
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

func checkStorageClassDef(def *model.StorageClassDef) error {
	if err := checkTier(def.Tier); err != nil {
		return err
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

func setStorageClassTierForTable(tbInfo *model.TableInfo, tier string) {
	tbInfo.StorageClassTier = tier
	logutil.DDLLogger().Info("storage class: set table tier", zap.Int64("tableID", tbInfo.ID), zap.String("tier", tier))
}

// BuildStorageClassForTable builds storage class tier for a table.
func BuildStorageClassForTable(tbInfo *model.TableInfo, settings *model.StorageClassSettings) error {
	if settings == nil {
		return nil
	}

	for _, def := range settings.Defs {
		if def.HasNoScopeDef() {
			setStorageClassTierForTable(tbInfo, def.Tier)
			return nil
		}
	}

	setStorageClassTierForTable(tbInfo, model.StorageClassTierDefault)
	return nil
}

func setStorageClassTierForPartition(tbInfo *model.TableInfo, part *model.PartitionDefinition, tier string) {
	part.StorageClassTier = tier
	logutil.DDLLogger().Info("storage class: set partition tier",
		zap.Int64("tableID", tbInfo.ID), zap.String("partitionName", part.Name.L), zap.String("tier", tier))
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

PartitionLoop:
	for i := range partitions {
		part := &partitions[i]
		for _, def := range settings.Defs {
			if def.HasNoScopeDef() {
				setStorageClassTierForPartition(tbInfo, part, def.Tier)
				continue PartitionLoop
			}

			if len(def.NamesIn) > 0 {
				if isPartitionMatchNamesIn(part, def.NamesIn) {
					setStorageClassTierForPartition(tbInfo, part, def.Tier)
					continue PartitionLoop
				}
			}

			// TODO: handle less than
			// TODO: handle values in
		}

		setStorageClassTierForPartition(tbInfo, part, model.StorageClassTierDefault)
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
