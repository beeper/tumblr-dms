# Tumblr DMs for Beeper

Tumblr DMs is a self-hosted Beeper bridge for Tumblr direct messages. It runs
locally, signs in with your Tumblr web session, and brings Tumblr DM
conversations into Beeper.

This is not an official Tumblr API integration. Tumblr web changes may require
bridge updates.

## What Works

- Tumblr login through Beeper Desktop.
- Existing Tumblr DM history backfill.
- Live incoming message sync.
- Text send and receive.
- Basic media send and receive.
- Replies in existing Tumblr conversations from Beeper.
- Read receipts, redactions, contact names, and avatars.
- Backfill that preserves unread state.

## Known Limitations

- Starting brand-new Tumblr chats from Beeper may depend on deeper Desktop
  support.
- Group DMs may be incomplete.
- Windows release binaries are not currently published.

## Setup

Install and log in to `bbctl`:

```sh
bbctl login
```

Create a runtime directory:

```sh
export BRIDGE_DIR="$HOME/.local/share/tumblr-dms"
mkdir -p "$BRIDGE_DIR"
cd "$BRIDGE_DIR"
```

Register the bridge with Beeper:

```sh
bbctl config --type bridgev2 -o config.yaml sh-tumblrdms
bbctl register -g -o registration.yaml sh-tumblrdms
```

Download the bridge binary:

```sh
curl -L -o tumblr-dms \
  https://github.com/beeper/tumblr-dms/releases/latest/download/tumblr-dms-<os-arch>
chmod +x tumblr-dms
```

Use one of:

- `darwin-arm64`
- `linux-amd64`
- `linux-arm64`

Start the bridge:

```sh
./tumblr-dms -c config.yaml -r registration.yaml >> bridge.log 2>&1 &
```

Check that it is running:

```sh
bbctl whoami | grep tumblrdms
```

Before Tumblr login, the bridge should show as running. After login, it should
show a connected remote.

Open Beeper Desktop, go to Settings -> Bridges -> Self-hosted Bridges, find
`sh-tumblrdms`, and add an account. Complete the Tumblr login flow when Beeper
opens it.

## Assisted Setup

If you are using an AI coding assistant, it can do most of the setup for you.
The only step you need to do yourself is the Tumblr login inside Beeper
Desktop.

Suggested opening:

> This connects your Tumblr DMs to Beeper. Once it is running, your Tumblr
> conversations show up in Beeper like your other chats, and replies you send
> from Beeper go back through Tumblr.

What the assistant should do:

1. Confirm Beeper Desktop is installed.
2. Confirm `bbctl` is installed and logged in. If not, install it from
   [beeper/bridge-manager](https://github.com/beeper/bridge-manager) and run
   `bbctl login`.
3. Detect the user's OS and CPU:
   - macOS Apple Silicon: `darwin-arm64`
   - Linux x86_64: `linux-amd64`
   - Linux ARM64: `linux-arm64`
4. Create the runtime directory:

   ```sh
   export BRIDGE_DIR="$HOME/.local/share/tumblr-dms"
   mkdir -p "$BRIDGE_DIR"
   cd "$BRIDGE_DIR"
   ```

5. Register the bridge with Beeper:

   ```sh
   bbctl config --type bridgev2 -o config.yaml sh-tumblrdms
   bbctl register -g -o registration.yaml sh-tumblrdms
   ```

6. Download the matching binary:

   ```sh
   curl -L -o tumblr-dms \
     https://github.com/beeper/tumblr-dms/releases/latest/download/tumblr-dms-<os-arch>
   chmod +x tumblr-dms
   ```

7. Start the bridge:

   ```sh
   ./tumblr-dms -c config.yaml -r registration.yaml >> bridge.log 2>&1 &
   ```

8. Confirm Beeper can see it:

   ```sh
   bbctl whoami | grep tumblrdms
   ```

What the user should do:

1. Open Beeper Desktop.
2. Go to Settings -> Bridges.
3. Find `sh-tumblrdms` under Self-hosted Bridges.
4. Add an account.
5. Complete the Tumblr login flow when Beeper opens it.

After login, the assistant can run this again:

```sh
bbctl whoami | grep tumblrdms
```

The bridge should show a connected remote.

What the assistant should not do without asking:

- Delete or reset the bridge.
- Delete local runtime files.
- Share logs, configs, registrations, databases, cookies, tokens, or browser
  captures.
- Push commits or rewrite git history.

## Runtime Files

The runtime directory contains local state for the bridge:

- `config.yaml`
- `registration.yaml`
- `sh-tumblrdms.db`
- `bridge.log`

Do not share these files publicly. They may contain account or connection
details.

## Troubleshooting

If the bridge does not appear in Beeper, check that it is running:

```sh
bbctl whoami | grep tumblrdms
```

If login fails, re-run the account login from Beeper Desktop.

If messages are not syncing, confirm the bridge shows a connected remote and
check `bridge.log` for recent errors.

If the bridge was deleted and re-added, start from a clean runtime directory.
Mixing old local state with a new Beeper registration can cause stale room or
delivery issues.

## Build

Requirements:

- Go 1.25 or newer.
- libolm development headers.

On macOS:

```sh
brew install libolm
```

On Debian/Ubuntu:

```sh
sudo apt-get install libolm-dev libolm3
```

Build:

```sh
go build ./...
./build.sh
```

The `ld: warning: ignoring duplicate libraries: '-lc++', '-lolm'` linker
warning on macOS is harmless.

## Docker

Build the container:

```sh
docker build -t tumblr-dms .
```

Run it with a data directory:

```sh
mkdir -p ./data
docker run --rm -v "$PWD/data:/data" tumblr-dms
```

## Project Layout

```text
cmd/tumblr-dms/main.go       Bridge binary entrypoint
pkg/connector/               Beeper bridge connector
pkg/msgconv/                 Message conversion
pkg/tumblr/                  Tumblr web client and models
pkg/tumblrid/                Identifier helpers
```

## Releases

GitHub Actions builds release binaries when a `v*` tag is pushed:

- `tumblr-dms-linux-amd64`
- `tumblr-dms-linux-arm64`
- `tumblr-dms-darwin-arm64`
- `sha256sums.txt`

## License

Tumblr DMs is licensed under GNU AGPLv3 or later, with the Beeper and Element
exceptions in `LICENSE.exceptions`.
