// Copyright 2026 PingCAP, Inc.

package model

import (
	"github.com/pingcap/errors"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
)

// CreateProcedureArgs is the arguments for create procedure/function job.
type CreateProcedureArgs struct {
	ProcedureInfo        *ProcedureInfo        `json:"procedure_info,omitempty"`
	LoadableFunctionInfo *LoadableFunctionInfo `json:"loadable_function_info,omitempty"`
}

func (a *CreateProcedureArgs) getArgsV1(*Job) []any {
	return []any{a.ProcedureInfo, a.LoadableFunctionInfo}
}

func (a *CreateProcedureArgs) decodeV1(job *Job) error {
	var procInfo *ProcedureInfo
	var loadableInfo *LoadableFunctionInfo
	if err := job.decodeArgs(&procInfo, &loadableInfo); err != nil {
		return errors.Trace(err)
	}
	a.ProcedureInfo = procInfo
	a.LoadableFunctionInfo = loadableInfo
	return nil
}

// GetCreateProcedureArgs gets the args for create procedure/function job.
func GetCreateProcedureArgs(job *Job) (*CreateProcedureArgs, error) {
	return getOrDecodeArgs[*CreateProcedureArgs](&CreateProcedureArgs{}, job)
}

// DropProcedureArgs is the arguments for drop procedure/function job.
type DropProcedureArgs struct {
	Schema   pmodel.CIStr `json:"schema,omitempty"`
	Name     pmodel.CIStr `json:"name,omitempty"`
	Type     string       `json:"type,omitempty"`
	IfExists bool         `json:"if_exists,omitempty"`
	// LoadableFunction indicates this drop is for a loadable UDF (mysql.func).
	LoadableFunction bool `json:"loadable_function,omitempty"`
}

func (a *DropProcedureArgs) getArgsV1(*Job) []any {
	return []any{a.Schema, a.Name, a.Type, a.IfExists, a.LoadableFunction}
}

func (a *DropProcedureArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.Schema, &a.Name, &a.Type, &a.IfExists, &a.LoadableFunction))
}

// GetDropProcedureArgs gets the args for drop procedure/function job.
func GetDropProcedureArgs(job *Job) (*DropProcedureArgs, error) {
	return getOrDecodeArgs[*DropProcedureArgs](&DropProcedureArgs{}, job)
}

// AlterProcedureArgs is the arguments for alter procedure/function job.
type AlterProcedureArgs struct {
	Schema pmodel.CIStr `json:"schema,omitempty"`
	Name   pmodel.CIStr `json:"name,omitempty"`
	Type   string       `json:"type,omitempty"`

	Comment      *string `json:"comment,omitempty"`
	SecurityType *string `json:"security_type,omitempty"`
}

func (a *AlterProcedureArgs) getArgsV1(*Job) []any {
	return []any{a.Schema, a.Name, a.Type, a.Comment, a.SecurityType}
}

func (a *AlterProcedureArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.Schema, &a.Name, &a.Type, &a.Comment, &a.SecurityType))
}

// GetAlterProcedureArgs gets the args for alter procedure/function job.
func GetAlterProcedureArgs(job *Job) (*AlterProcedureArgs, error) {
	return getOrDecodeArgs[*AlterProcedureArgs](&AlterProcedureArgs{}, job)
}

// CreateTriggerArgs is the arguments for create trigger job.
type CreateTriggerArgs struct {
	TriggerInfo *TriggerInfo `json:"trigger_info,omitempty"`
}

func (a *CreateTriggerArgs) getArgsV1(*Job) []any {
	return []any{a.TriggerInfo}
}

func (a *CreateTriggerArgs) decodeV1(job *Job) error {
	a.TriggerInfo = &TriggerInfo{}
	return errors.Trace(job.decodeArgs(a.TriggerInfo))
}

// GetCreateTriggerArgs gets the args for create trigger job.
func GetCreateTriggerArgs(job *Job) (*CreateTriggerArgs, error) {
	return getOrDecodeArgs[*CreateTriggerArgs](&CreateTriggerArgs{}, job)
}

// DropTriggerArgs is the arguments for drop trigger job.
type DropTriggerArgs struct {
	TriggerName pmodel.CIStr `json:"trigger_name,omitempty"`
	IfExists    bool         `json:"if_exists,omitempty"`
}

func (a *DropTriggerArgs) getArgsV1(*Job) []any {
	return []any{a.TriggerName, a.IfExists}
}

func (a *DropTriggerArgs) decodeV1(job *Job) error {
	return errors.Trace(job.decodeArgs(&a.TriggerName, &a.IfExists))
}

// GetDropTriggerArgs gets the args for drop trigger job.
func GetDropTriggerArgs(job *Job) (*DropTriggerArgs, error) {
	return getOrDecodeArgs[*DropTriggerArgs](&DropTriggerArgs{}, job)
}
