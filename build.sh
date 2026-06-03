#!/bin/sh
set -eu

BINARY_NAME=tumblr-dms go tool maubuild "$@"
