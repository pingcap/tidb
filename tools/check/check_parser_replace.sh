#!/usr/bin/env bash
set -uo pipefail

grep "replace.*github.com/pingcap/parser" go.mod
grep_ret=$?

if [ $grep_ret -eq 0 ];then
  exit 1
else
  exit 0
fi
