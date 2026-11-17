set shell := ["bash", "-euo", "pipefail", "-c"]

default:
    @just --list

up:
    @bash scripts/up.sh

status:
    @bash scripts/status.sh

down:
    @bash scripts/down.sh

e2e:
    @bash scripts/e2e.sh
