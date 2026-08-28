#!/bin/sh
set -eu

attempt=1
max_attempts=${DEPENDENCY_COMMAND_RETRY_ATTEMPTS:-8}
retry_delay=${DEPENDENCY_COMMAND_RETRY_DELAY_SECONDS:-5}
while :; do
  if "$@"; then
    exit 0
  else
    status=$?
  fi
  if [ "$attempt" -ge "$max_attempts" ]; then
    exit "$status"
  fi
  echo "dependency command failed (exit $status); retry $attempt/$max_attempts in ${retry_delay}s" >&2
  sleep "$retry_delay"
  attempt=$((attempt + 1))
done
