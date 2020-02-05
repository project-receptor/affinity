#!/usr/bin/env bash
# coding=utf-8
set -euo pipefail

# Don't actually make changes on behalf of the dev. Just check to see if they
# forgot to do so.
poetry run black --check .
