#!/bin/sh
set -eu
umask 077

BINARY_NAME="${BINARY_NAME:-/usr/bin/tumblr-dms}"
DATA_DIR="${DATA_DIR:-/data}"
CONFIG_PATH="${CONFIG_PATH:-$DATA_DIR/config.yaml}"
REGISTRATION_PATH="${REGISTRATION_PATH:-$DATA_DIR/registration.yaml}"

case "$DATA_DIR" in
	/*) ;;
	*)
		echo "DATA_DIR must be an absolute path." >&2
		exit 1
		;;
esac

case "$DATA_DIR" in
	/ | /bin | /boot | /dev | /etc | /home | /lib | /lib64 | /media | /mnt | /opt | /proc | /root | /run | /sbin | /srv | /sys | /tmp | /usr | /var)
		echo "Refusing to use broad system directory $DATA_DIR as DATA_DIR." >&2
		exit 1
		;;
esac

mkdir -p "$DATA_DIR"

if [ -L "$DATA_DIR" ]; then
	echo "Refusing to use a symbolic link as DATA_DIR." >&2
	exit 1
fi

resolved_data_dir=$(realpath "$DATA_DIR")
case "$resolved_data_dir" in
	/ | /bin | /boot | /dev | /etc | /home | /lib | /lib64 | /media | /mnt | /opt | /proc | /root | /run | /sbin | /srv | /sys | /tmp | /usr | /var)
		echo "Refusing to use broad system directory $resolved_data_dir as DATA_DIR." >&2
		exit 1
		;;
esac
DATA_DIR=$resolved_data_dir

# UID is the standard Mautrix container setting exported by the Dockerfile.
# shellcheck disable=SC3028
runtime_uid=${UID:-1337}
runtime_gid=${GID:-$runtime_uid}
case "$runtime_uid" in
	"" | 0 | 0* | *[!0-9]*)
		echo "UID must be a non-zero numeric ID." >&2
		exit 1
		;;
esac
case "$runtime_gid" in
	"" | 0 | 0* | *[!0-9]*)
		echo "GID must be a non-zero numeric ID." >&2
		exit 1
		;;
esac

resolve_runtime_file() {
	description=$1
	path=$2
	if [ -L "$path" ] || [ ! -f "$path" ]; then
		echo "Refusing to use $path: expected a regular, non-symbolic-link $description file." >&2
		exit 1
	fi
	resolved_path=$(realpath "$path")
	case "$resolved_path" in
		"$DATA_DIR"/*) ;;
		*)
			echo "Refusing to use $path: $description must be inside DATA_DIR." >&2
			exit 1
			;;
	esac
	printf '%s\n' "$resolved_path"
}

if [ ! -f "$CONFIG_PATH" ]; then
	echo "Didn't find $CONFIG_PATH." >&2
	echo "Generate it on the host with bbctl config before starting the container." >&2
	exit 1
fi

if [ ! -f "$REGISTRATION_PATH" ]; then
	echo "Didn't find $REGISTRATION_PATH." >&2
	echo "Generate and register it on the host with bbctl register before starting the container." >&2
	exit 1
fi

CONFIG_PATH=$(resolve_runtime_file config "$CONFIG_PATH")
REGISTRATION_PATH=$(resolve_runtime_file registration "$REGISTRATION_PATH")
chown -R "$runtime_uid:$runtime_gid" "$DATA_DIR"
chmod 0700 "$DATA_DIR"
chmod 0600 "$CONFIG_PATH" "$REGISTRATION_PATH"

cd "$DATA_DIR"
exec su-exec "$runtime_uid:$runtime_gid" "$BINARY_NAME" -c "$CONFIG_PATH" -r "$REGISTRATION_PATH"
