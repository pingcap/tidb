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
// See the License for the specific language governing permissions and
// limitations under the License.

package lbac

import (
	"github.com/pingcap/errors"
	mysql "github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// LBAC errors.
var (
	errLBACInvalid       = dbterror.ClassPrivilege.NewStd(mysql.ErrLBACInvalid)
	errLBACNotFound      = dbterror.ClassPrivilege.NewStd(mysql.ErrLBACNotFound)
	errLBACAlreadyExists = dbterror.ClassPrivilege.NewStd(mysql.ErrLBACAlreadyExists)

	// Component errors
	ErrInvalidComponentType    = errLBACInvalid.FastGenByArgs("invalid component type")
	ErrEmptyComponentValues    = errLBACInvalid.FastGenByArgs("empty component values")
	ErrInvalidComponentValue   = errLBACInvalid.FastGenByArgs("invalid component value")
	ErrDuplicateComponentValue = errLBACInvalid.FastGenByArgs("duplicate component value")
	ErrComponentNotFound       = errLBACNotFound.FastGenByArgs("component")
	ErrComponentAlreadyExists  = errLBACAlreadyExists.FastGenByArgs("component")
	ErrComponentInUse          = errLBACInvalid.FastGenByArgs("component is in use by policies")

	// Policy errors
	ErrInvalidPolicyName            = errLBACInvalid.FastGenByArgs("invalid policy name")
	ErrEmptyPolicyComponents        = errLBACInvalid.FastGenByArgs("empty policy components")
	ErrDuplicateComponentInPolicy   = errLBACInvalid.FastGenByArgs("duplicate component in policy")
	ErrPolicyNotFound               = errLBACNotFound.FastGenByArgs("policy")
	ErrPolicyAlreadyExists          = errLBACAlreadyExists.FastGenByArgs("policy")
	ErrPolicyInUse                  = errLBACInvalid.FastGenByArgs("policy is in use by labels")
	ErrPolicyInUseByTables          = errLBACInvalid.FastGenByArgs("policy is in use by tables")
	ErrPolicyInUseByExemptions      = errLBACInvalid.FastGenByArgs("policy is in use by exemptions")
	ErrMultipleSecurityLabelColumns = errLBACInvalid.FastGenByArgs("multiple SECURITYLABEL columns in table")

	// Label errors
	ErrEmptyLabelComponents     = errLBACInvalid.FastGenByArgs("empty label components")
	ErrLabelNotFound            = errLBACNotFound.FastGenByArgs("security label")
	ErrLabelAlreadyExists       = errLBACAlreadyExists.FastGenByArgs("label")
	ErrLabelInUse               = errLBACInvalid.FastGenByArgs("label is in use by users")
	ErrLabelInUseByColumns      = errLBACInvalid.FastGenByArgs("label is in use by columns")
	ErrInvalidComponentForLabel = errLBACInvalid.FastGenByArgs("invalid component for label")

	// User permission errors
	ErrUserLabelAlreadyExists     = errLBACAlreadyExists.FastGenByArgs("user label")
	ErrInvalidAccessType          = errLBACInvalid.FastGenByArgs("invalid access type")
	ErrUserExemptionAlreadyExists = errLBACAlreadyExists.FastGenByArgs("user exemption")
	ErrSecurityPolicyMissing      = errLBACInvalid.FastGenByArgs("security policy missing")
	ErrLBACCacheUnavailable       = errLBACInvalid.FastGenByArgs("lbac privilege cache unavailable")
)

// ComponentError creates a component-related error.
func ComponentError(err error, componentName string) error {
	return annotateObjectError(err, "component", componentName)
}

// PolicyError creates a policy-related error.
func PolicyError(err error, policyName string) error {
	return annotateObjectError(err, "policy", policyName)
}

// LabelError creates a label-related error.
func LabelError(err error, labelName string) error {
	return annotateObjectError(err, "label", labelName)
}

// UserError creates a user-related error.
func UserError(err error, userName, host string) error {
	if err == nil || (userName == "" && host == "") {
		return err
	}
	if userName == "" {
		return errors.Annotatef(err, "user @%s", host)
	}
	if host == "" {
		return errors.Annotatef(err, "user %s", userName)
	}
	return errors.Annotatef(err, "user %s@%s", userName, host)
}

func annotateObjectError(err error, kind, name string) error {
	if err == nil || name == "" {
		return err
	}
	return errors.Annotatef(err, "%s %s", kind, name)
}
