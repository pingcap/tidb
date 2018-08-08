#!/usr/bin/env bash
set -euo pipefail

# This script generates tools.json
# It helps record what releases/branches are being used
which retool >/dev/null || go get github.com/twitchtv/retool

# This tool can run other checks in a standardized way
retool add gopkg.in/alecthomas/gometalinter.v2 v2.0.5

# check spelling
# misspell works with gometalinter
retool add github.com/client9/misspell/cmd/misspell v0.3.4
# goword adds additional capability to check comments
retool add github.com/chzchzchz/goword a9744cb52b033fe5c269df48eeef2c954526cd79

# checks correctness
retool add github.com/gordonklaus/ineffassign 7bae11eba15a3285c75e388f77eb6357a2d73ee2
retool add honnef.co/go/tools/cmd/megacheck master
retool add github.com/dnephin/govet 4a96d43e39d340b63daa8bc5576985aa599885f6

# slow checks
retool add github.com/kisielk/errcheck v1.1.0
retool add github.com/securego/gosec/cmd/gosec 1.0.0

# linter
retool add github.com/mgechev/revive 7773f47324c2bf1c8f7a5500aff2b6c01d3ed73b
