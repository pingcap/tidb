// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// See grunning.Supported() for an explanation behind this build tag.
//
//go:build (darwin && arm64) || freebsd

package grunning

func grunningnanos() int64 { return 0 }

func supported() bool { return false }
