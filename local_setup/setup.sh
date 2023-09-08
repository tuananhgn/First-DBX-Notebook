#!/bin/bash
##
#   This script is used to setup the environment locally for this repository.
#
#   Sets up:
PYTHON_VERSION="3.7.13"
DBT_DATABRICK_VERSION=""

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_DIR=$( dirname $SCRIPT_DIR)

echo "${PYTHON_VERSION}-dbt-databrick"

# Virtual environment setup
pyenv virtualenv ${PYTHON_VERSION} ${PYTHON_VERSION}-dbt-databrick
pyenv local ${PYTHON_VERSION}-dbt-databrick
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
