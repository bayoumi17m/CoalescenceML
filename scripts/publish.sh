#!/usr/bin/env bash

set -e


export COML_DEBUG=1
python -m poetry publish --build --username $PYPI_USERNAME --password $PYPI_PASSWORD
