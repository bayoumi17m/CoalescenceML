#!/usr/bin/env bash

set -e
set -x

# If only unittests or only integration
# then we just call test-coverage.sh unit
# or test-coverage.sh integration
TEST_SRC="tests/"${1:-""}

export COML_DEBUG=1
export EVIDENTLY_DISABLE_TELEMETRY=1

if [ -n "$1" ]; then
    coverage run -m pytest $TEST_SRC --color=yes
else
    coverage run -m pytest tests/unit --color=yes
    coverage run -m pytest tests/integration --color=yes
fi

coverage combine
coverage report --show-missing
coverage xml
