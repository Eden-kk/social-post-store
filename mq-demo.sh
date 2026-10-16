#!/bin/sh
set -eu
cd "$(dirname "$0")"
sh build.sh
if [ ! -x build/mq-venv/bin/python ]; then
    python3 -m venv build/mq-venv
fi
build/mq-venv/bin/python -m pip install -r adapters/view-mq/requirements.txt
build/mq-venv/bin/python -m unittest discover -s adapters/view-mq -p test_contract.py
exec build/mq-venv/bin/python adapters/view-mq/integration.py "$@"
