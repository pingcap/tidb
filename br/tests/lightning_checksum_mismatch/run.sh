#!/bin/sh

set -eux

run_lightning 2>&1 | grep -q "checksum mismatched remote vs local"
