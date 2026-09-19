# Bosch Smart Camera RTSP Proxy

Re-exposes Bosch Smart Camera System streams as plain `rtsp://` URLs, so any
standard RTSP client (VLC, Frigate, ffmpeg, a NAS, ...) can view them without
going through Bosch's mobile app.

It logs into your Bosch account once via OAuth2, talks to Bosch's REST API to
fetch short-lived per-camera credentials, and relays the camera's own TLS
video stream out as a local, unauthenticated RTSP feed — one path per camera.

## Quick start (Docker)

```bash
docker run --rm -it \
  -v "$(pwd)/data:/data" \
  ghcr.io/4e6963/bosch-smart-camera-rtsp-proxy:latest \
  login
```

This walks you through a one-time login (see below) and writes a session
file to `./data/tokens.json`. Once that exists, start the proxy for real:

```bash
docker run -d --name bosch-cam-proxy \
  --restart unless-stopped \
  -p 8554:8554 \
  -v "$(pwd)/data:/data" \
  ghcr.io/4e6963/bosch-smart-camera-rtsp-proxy:latest
```

Or with Compose (see [`docker-compose.yml`](docker-compose.yml) for the full
list of optional environment variables):

```yaml
services:
  bosch-cam-proxy:
    image: ghcr.io/4e6963/bosch-smart-camera-rtsp-proxy:latest
    restart: unless-stopped
    ports:
      - "8554:8554"
    volumes:
      - ./data:/data
```

```bash
docker compose run --rm bosch-cam-proxy login   # one-time
docker compose up -d
```

List your cameras and their proxy URLs at any point with:

```bash
docker compose run --rm bosch-cam-proxy list-cameras
```

Each camera is then reachable at `rtsp://<host>:8554/<camera-id>`.

## Getting started: logging in

There is no scripted/headless login — Bosch's identity provider gates its
login form behind a bot-protection challenge, so a human has to complete it
once in a real browser. `login` walks you through that:

```
$ bosch-cam-proxy login
Not logged in: no session found (no refresh token in ./tokens.json).

To obtain a session:

1. Open this URL in a real browser and log in with your Bosch account:

    https://smarthome.authz.bosch.com/auth/realms/home_auth_provider/protocol/openid-connect/auth?...

2. It ends in a redirect to 'https://www.bosch.com/boschcam?code=...&state=...'
   that fails to load — that's expected (no real page there). Copy the `code`
   value out of that URL's query string right away: it's a single-use
   authorization code, typically valid for well under a minute.
3. Paste it at the prompt below.

Paste the code here: <paste the code>
login: SUCCESS; session persisted to ./tokens.json
```

The resulting session (an OAuth refresh token) is saved to `TOKEN_STORE`
(`./tokens.json` by default) and kept alive automatically by `run` /
`list-cameras` — you only need to repeat `login` if that file is lost or the
refresh token gets revoked. Running `login` again when a session already
exists just verifies it (forces a refresh) instead of asking you to log in.

### Commands

| Command | Purpose |
|---|---|
| `run` (default) | Start the RTSP server. |
| `list-cameras` | Print each camera's id/name and its `rtsp://` URL. |
| `login` | Verify or (re-)establish a session, interactively. |

### Connecting a client

```
rtsp://<proxy-host>:8554/<camera-id>
```

Optional path/query overrides per request:

- Append `/local` or `/relay` to force a route regardless of
  `RTSP_DEFAULT_ROUTE`, e.g. `rtsp://<host>:8554/<camera-id>/relay`.
- Append `?quality=low` to request the camera's lower-resolution sub-stream
  instead of its main stream, e.g. `rtsp://<host>:8554/<camera-id>?quality=low`.

## Configuration

All settings are optional environment variables — every one already has a
working default. See also [`.env.example`](.env.example).

| Variable | Default | Description |
|---|---|---|
| `BOSCH_CLIENT_ID` | `residential_app` | OAuth client id used to talk to Bosch's identity provider. |
| `BOSCH_CLIENT_SECRET` | *(built-in)* | OAuth client secret paired with `BOSCH_CLIENT_ID`. This is the public mobile-app secret, not a per-user value — override only if Bosch ever rotates it. |
| `BOSCH_CA_BUNDLE` | *(embedded)* | Path to a PEM file overriding the embedded Bosch private-PKI root CA, used to validate both the REST API and the camera's own TLS. Only needed if Bosch rotates their root CA. |
| `BOSCH_TLS_INSECURE` | `0` (off) | Bring-up crutch: skip camera certificate verification entirely instead of validating against `BOSCH_CA_BUNDLE`. **Do not use in production.** |
| `TOKEN_STORE` | `./tokens.json` | Where the OAuth refresh token is persisted between runs. Seeded by `login`. |
| `RTSP_BIND` | `0.0.0.0:8554` | Local bind address for the RTSP server that consumers connect to. |
| `RTSP_DEFAULT_ROUTE` | `local` | Default upstream route when a request doesn't specify one: `local` (direct LAN connection, port 443, higher quality) or `relay` (Bosch's cloud relay). Override per-request with a `/local` or `/relay` path suffix. |
| `RTSP_AUDIO` | on | Relay the camera's audio track in addition to video. Set to `0` to disable (video only). |
| `RTSP_CONNECTING_PLACEHOLDER` | on | Serve a "Connecting to camera..." placeholder clip while the real upstream ingest is starting (cold start takes several seconds, mostly the camera's own TLS handshake), so subscribers get instant playback instead of a blocked `DESCRIBE`. Set to `0` to disable and block until the real stream is ready. |
| `RUST_LOG` | `bosch_cam_proxy=info` | Log filter, `tracing`'s `EnvFilter` syntax (e.g. `bosch_cam_proxy=debug`, or a specific module path). |

Flags (`BOSCH_TLS_INSECURE`, `RTSP_AUDIO`, `RTSP_CONNECTING_PLACEHOLDER`)
accept `1`/`true`/`yes` or `0`/`false`/`no` (case-insensitive).

## Building from source

Requires a Rust toolchain and OpenSSL (Linux/macOS) or nothing extra on
Windows (uses SChannel via `native-tls`).

```bash
cargo build --release
./target/release/bosch-cam-proxy login
./target/release/bosch-cam-proxy
```

## License

[0BSD](LICENSE).
