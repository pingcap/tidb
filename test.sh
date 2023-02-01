#!/bin/bash

SELECTED_TEST_NAME="${TEST_NAME-$(find tests -mindepth 2 -maxdepth 2 -name run.sh | cut -d/ -f2 | sort)}"

echo $SELECTED_TEST_NAME
