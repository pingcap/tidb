// Copyright 2026 PingCAP, Inc.
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

package dxfutil

import (
	"context"
	"encoding/json"
	"path"
	"reflect"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const metaName = "meta.json"

// marshalWithOverride marshals the provided struct with the ability to override
func marshalWithOverride(src any, hideCond func(f reflect.StructField) bool) ([]byte, error) {
	v := reflect.ValueOf(src)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return json.Marshal(src)
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return json.Marshal(src)
	}
	t := v.Type()
	fields := make([]reflect.StructField, 0, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		newTag := f.Tag
		if hideCond(f) {
			newTag = `json:"-"`
		}
		fields = append(fields, reflect.StructField{
			Name:      f.Name,
			Type:      f.Type,
			Tag:       newTag,
			Offset:    f.Offset,
			Anonymous: f.Anonymous,
		})
	}
	newType := reflect.StructOf(fields)
	newVal := reflect.New(newType).Elem()
	j := 0
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		newVal.Field(j).Set(v.Field(i))
		j++
	}
	return json.Marshal(newVal.Interface())
}

// marshalInternalFields marshal all fields except those with external:"true" tag.
func marshalInternalFields(src any) ([]byte, error) {
	return marshalWithOverride(src, func(f reflect.StructField) bool {
		return f.Tag.Get("external") == "true"
	})
}

// marshalExternalFields marshal all fields with external:"true" tag.
func marshalExternalFields(src any) ([]byte, error) {
	return marshalWithOverride(src, func(f reflect.StructField) bool {
		return f.Tag.Get("external") != "true"
	})
}

// BaseExternalMeta is the base meta of external meta.
type BaseExternalMeta struct {
	// ExternalPath is the path to the external storage where the external meta is stored.
	ExternalPath string
}

// Marshal serializes the provided alias to JSON.
// Usage: If ExternalPath is set, marshals using internal meta; otherwise marshals the alias directly.
func (m BaseExternalMeta) Marshal(alias any) ([]byte, error) {
	if m.ExternalPath == "" {
		return json.Marshal(alias)
	}
	return marshalInternalFields(alias)
}

// WriteJSONToExternalStorage writes the serialized external meta JSON to external storage.
// Usage: Store external meta after appropriate modifications.
func (m BaseExternalMeta) WriteJSONToExternalStorage(ctx context.Context, store storeapi.Storage, a any) error {
	if m.ExternalPath == "" {
		return nil
	}
	data, err := marshalExternalFields(a)
	if err != nil {
		return errors.Trace(err)
	}
	return store.WriteFile(ctx, m.ExternalPath, data)
}

// ReadJSONFromExternalStorage reads and unmarshals JSON from external storage into the provided alias.
// Usage: Retrieve external meta for further processing.
func (m BaseExternalMeta) ReadJSONFromExternalStorage(ctx context.Context, store storeapi.Storage, a any) error {
	if m.ExternalPath == "" {
		return nil
	}
	data, err := store.ReadFile(ctx, m.ExternalPath)
	if err != nil {
		return errors.Trace(err)
	}
	return json.Unmarshal(data, a)
}

// PlanMetaPath returns the path of the plan meta file.
func PlanMetaPath(taskID int64, step string, idx int) string {
	return path.Join(strconv.FormatInt(taskID, 10), "plan", step, strconv.Itoa(idx), metaName)
}

// PreparedMetaPath returns the path of the prepared meta file.
func PreparedMetaPath(taskID int64) string {
	return path.Join(strconv.FormatInt(taskID, 10), "plan", "prepared", metaName)
}

// SubtaskMetaPath returns the path of the subtask meta file.
func SubtaskMetaPath(taskID int64, subtaskID int64) string {
	return path.Join(strconv.FormatInt(taskID, 10), strconv.FormatInt(subtaskID, 10), metaName)
}
