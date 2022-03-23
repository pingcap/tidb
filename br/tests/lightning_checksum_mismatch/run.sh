#!/bin/sh

set -eux

run_lightning 2>&1 | grep -q "Error: checksum mismatched remote vs local"
