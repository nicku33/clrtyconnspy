#!/bin/bash
set -ex

echo "Running unit tests on connspy"
PYTHONPATH=connspy pytest --tb=short tests/*.py
