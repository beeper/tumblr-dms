#!/bin/sh
set -eu

if [ "${CGO_ENABLED:-1}" != "1" ]; then
	echo "CGO_ENABLED=1 is required to build Tumblr DMs with SQLite support." >&2
	exit 1
fi

export CGO_ENABLED=1
BINARY_NAME=tumblr-dms go tool maubuild -tags goolm "$@"
