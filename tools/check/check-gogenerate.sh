#!/usr/bin/env bash
set -euo pipefail

go generate ./...
if git status -s | awk '{print $2}' | xargs grep '^// Code generated .* DO NOT EDIT\.$' > /dev/null
then
  echo "Your commit is changed after running go generate ./..., it should not hanppen."
  exit 1
fi
