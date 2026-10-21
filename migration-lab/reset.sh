#!/usr/bin/env bash
# Destroys the lab completely: every container, every volume and run/.
# Use it to start over, or after a step failed part-way.
set -euo pipefail
source "$(dirname "$0")/lib.sh"

say "Removing all lab containers and volumes (project kot-mig) and run/"
run composec down -v --remove-orphans
rm -rf "$RUN"
note "reset complete. Next: ./00-seed.sh"
