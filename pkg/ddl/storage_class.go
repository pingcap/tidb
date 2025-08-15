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
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"go.uber.org/zap"
)

// BuildStorageClassSettingsFromJSON builds storage class settings from a JSON object.
func BuildStorageClassSettingsFromJSON(input json.RawMessage) (*model.StorageClassSettings, error) {
	settings, err := func() (*model.StorageClassSettings, error) {
		if input == nil {
			def := model.StorageClassDef{Tier: model.StorageClassTierDefault}
			return &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{&def},
			}, nil
		}

		// Try parsing as a string
		tier, err := getStorageClassTierAsString(input)
		if err == nil {
			def := model.StorageClassDef{Tier: tier}
			return &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{&def},
			}, nil
		}

		// Try parsing as a single object
		decoder := json.NewDecoder(bytes.NewReader(input))
		decoder.DisallowUnknownFields()
		var def model.StorageClassDef
		err = decoder.Decode(&def)
		if err == nil {
			return &model.StorageClassSettings{
				Defs: []*model.StorageClassDef{&def},
			}, nil
		}

		// Try parsing as a slice of objects
		var defs []*model.StorageClassDef
		err = json.Unmarshal(input, &defs)
		if err == nil {
			return &model.StorageClassSettings{
				Defs: defs,
			}, nil
		}

		// If all parsing attempts fail, return an error
		msg := fmt.Sprintf("invalid storage class def: '%-.192s'", input)
		return nil, dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
	}()
	if err != nil {
		return nil, err
	}

	// Validate the settings
	for _, def := range settings.Defs {
		if err := checkStorageClassDef(def); err != nil {
			return nil, err
		}
		def.Tier = model.StorageClassTier(strings.ToUpper(string(def.Tier)))
		for i, name := range def.NamesIn {
			def.NamesIn[i] = strings.ToLower(name)
		}
	}
	if err := checkDuplicatedNamesIn(settings.Defs); err != nil {
		return nil, err
	}

	return settings, nil
}

func getStorageClassTierAsString(msg json.RawMessage) (model.StorageClassTier, error) {
	var tier string
	if err := json.Unmarshal(msg, &tier); err != nil {
		return "", err
	}
	return model.StorageClassTier(strings.ToUpper(tier)), nil
}

func checkStorageClassDef(def *model.StorageClassDef) error {
	if !def.Tier.Valid() {
		msg := fmt.Sprintf("invalid storage class tier: %s", def.Tier)
		return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
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
		msg := "can only specify one of 'names_in', 'less_than', and 'values_in'"
		return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
	}

	return nil
}

func checkDuplicatedNamesIn(defs []*model.StorageClassDef) error {
	seen := make(map[string]struct{})
	for _, def := range defs {
		for _, name := range def.NamesIn {
			if _, ok := seen[name]; ok {
				msg := fmt.Sprintf("duplicated names_in: %s", name)
				return dbterror.ErrStorageClassInvalidSpec.GenWithStackByArgs(msg)
			}
			seen[name] = struct{}{}
		}
	}
	return nil
}

func setStorageClassTierForTable(tbInfo *model.TableInfo, tier model.StorageClassTier) {
	tbInfo.StorageClassTier = tier
	logutil.DDLStorageClassLogger().Info("set table tier",
		zap.Int64("tableID", tbInfo.ID), zap.Stringer("tier", tier))
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

func setStorageClassTierForPartition(tbInfo *model.TableInfo, part *model.PartitionDefinition, tier model.StorageClassTier) {
	part.StorageClassTier = tier
	logutil.DDLStorageClassLogger().Info("set partition tier",
		zap.Int64("tableID", tbInfo.ID), zap.String("partitionName", part.Name.L), zap.Stringer("tier", tier))
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

			if slices.Contains(def.NamesIn, part.Name.L) {
				setStorageClassTierForPartition(tbInfo, part, def.Tier)
				continue PartitionLoop
			}

			// TODO: handle less than
			// TODO: handle values in
		}

		setStorageClassTierForPartition(tbInfo, part, model.StorageClassTierDefault)
	}

	return nil
}
