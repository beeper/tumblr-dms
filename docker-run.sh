#!/bin/sh
set -eu

: "${UID:=1337}"
: "${GID:=$UID}"

BINARY_NAME="${BINARY_NAME:-/usr/bin/tumblr-dms}"
DATA_DIR="${DATA_DIR:-/data}"
CONFIG_PATH="${CONFIG_PATH:-$DATA_DIR/config.yaml}"
REGISTRATION_PATH="${REGISTRATION_PATH:-$DATA_DIR/registration.yaml}"

ensure_parent_dir() {
	parent=$(dirname "$1")
	if [ "$parent" != "." ]; then
		mkdir -p "$parent"
	fi
}

mkdir -p "$DATA_DIR"
ensure_parent_dir "$CONFIG_PATH"
ensure_parent_dir "$REGISTRATION_PATH"

fixperms() {
	chown -R "$UID:$GID" "$DATA_DIR"
}

if [ ! -f "$CONFIG_PATH" ]; then
	"$BINARY_NAME" -c "$CONFIG_PATH" -e
	fixperms
	echo "Didn't find a config file."
	echo "Copied default config file to $CONFIG_PATH"
	echo "Modify that config file to your liking."
	echo "Start the container again after that to generate the registration file."
	exit 0
fi

if [ ! -f "$REGISTRATION_PATH" ]; then
	"$BINARY_NAME" -g -c "$CONFIG_PATH" -r "$REGISTRATION_PATH"
	fixperms
	echo "Didn't find a registration file."
	echo "Generated one for you."
	echo "See https://docs.mau.fi/bridges/general/registering-appservices.html on how to use it."
	exit 0
fi

cd "$DATA_DIR"
fixperms
exec su-exec "$UID:$GID" "$BINARY_NAME" -c "$CONFIG_PATH" -r "$REGISTRATION_PATH"
