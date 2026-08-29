# Tumblr for Beeper

The Tumblr bridge connects Tumblr direct messages to Beeper. This code powers the
Beeper Cloud and On-Device variants and can also run as a self-hosted bridge.

This is an unofficial Tumblr integration. Tumblr changes may occasionally
require bridge updates.

## Features

- Email and password sign-in, including two-factor authentication.
- Multi-blog accounts: choose which messaging-enabled Tumblr inbox to connect.
- Existing history backfill and live message sync.
- Send and receive text, JPEG, PNG, WebP, GIF, and Tumblr post shares.
- Read state, contact names, and avatars.

Each Beeper connection represents one Tumblr blog. Add another Tumblr
connection to connect another inbox from the same account.

## Limitations

- New Tumblr conversations must be started in Tumblr before they appear in
  Beeper.
- Replies are sent as regular messages. Reply metadata, edits, reactions,
  per-message deletion, typing indicators, audio, files, and video are not
  supported.
- Image uploads are limited to 5 MiB.

## Self-hosting

You need [Beeper Desktop](https://www.beeper.com/download) and
[`bbctl`](https://github.com/beeper/bridge-manager). The feature list above
describes the current `main` branch, so build from source when tagged releases
lag behind it.

### Build

Requirements:

- Go 1.26 or newer.
- libolm development headers.

Install libolm:

```sh
# macOS
brew install libolm

# Debian or Ubuntu
sudo apt-get install libolm-dev libolm3
```

Build the bridge:

```sh
git clone https://github.com/beeper/tumblr-dms.git
cd tumblr-dms
./build.sh
```

Tagged binaries are published for `darwin-arm64`, `linux-amd64`, and
`linux-arm64`.

### Register and run

```sh
bbctl login

tumblr_bridge_dir="$HOME/.local/share/tumblr-dms"
mkdir -p "$tumblr_bridge_dir"
chmod 700 "$tumblr_bridge_dir"
cp tumblr-dms "$tumblr_bridge_dir/"
cd "$tumblr_bridge_dir"

bbctl config --type bridgev2 -o config.yaml sh-tumblrdms
bbctl register -g -o registration.yaml sh-tumblrdms
chmod 600 config.yaml registration.yaml

./tumblr-dms -c config.yaml -r registration.yaml
```

In Beeper Desktop, open **Settings → Bridges → Self-hosted Bridges**, add
`sh-tumblrdms`, and complete Tumblr sign-in.

The runtime directory contains the bridge database, configuration,
registration, and Tumblr session. Do not share those files or bridge logs.

## License

The Tumblr bridge is licensed under GNU AGPLv3 or later, with the Beeper and Element
exceptions in `LICENSE.exceptions`.
