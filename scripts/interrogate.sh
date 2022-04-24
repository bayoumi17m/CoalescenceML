#!/usr/bin/env bash
set -e
set -x

SRC=${1:-"src/coalescenceml"}

export COML_DEBUG=1
interrogate $SRC -c pyproject.toml --generate-badge docs/interrogate.svg