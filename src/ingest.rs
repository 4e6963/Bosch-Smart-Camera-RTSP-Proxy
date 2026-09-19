//! Upstream ingest: spawn/manage an `ffmpeg` subprocess that pulls one camera's
//! stream over the RTSP-over-HTTPS tunnel and republishes it into our local RTSP
//! server via ANNOUNCE/RECORD.
//!
//! The camera's TLS is not handed to ffmpeg directly: its cert can't be validated by
//! ffmpeg's GnuTLS backend (see [`crate::tls_relay`] for why), so a local
//! [`TlsRelay`] terminates it and ffmpeg only ever speaks a plain loopback HTTP
//! tunnel. The remaining flags are the outcome of the milestone-0 spike; they are
//! isolated in [`build_ffmpeg_args`] so they can be adjusted once verified against a
//! real camera.

use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tracing::{debug, warn};

use crate::camera::VideoTarget;
use crate::config::Config;
use crate::error::{ProxyError, Result};
use crate::tls_relay::TlsRelay;

/// Assemble the ffmpeg argument vector.
///
/// * `input_url` — the local [`TlsRelay`] loopback URL, with inline `user:password@`
///   userinfo.
/// * `publish_url` — the internal `rtsp://127.0.0.1:port/...` target on our own server.
pub fn build_ffmpeg_args(input_url: &str, publish_url: &str, include_audio: bool) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "-hide_banner".into(),
        "-loglevel".into(),
        "warning".into(),
        // Reconnect handling is done by us (re-spawn), so fail fast on the upstream.
        // `-timeout` is the rtsp demuxer's own control-channel AVOption; unlike
        // `-rw_timeout` (an AVIOContext/protocol option) it applies regardless of
        // which `-rtsp_transport` tunnel is in use.
        "-timeout".into(),
        "30000000".into(), // 30s in microseconds
    ];

    // --- Input options (RTSP-over-HTTP tunnel to our own TlsRelay), placed before -i ---
    // The relay already terminated the camera's TLS, so ffmpeg only ever sees a
    // plaintext HTTP tunnel on loopback here — no `-ca_file`/`-tls_verify` needed.
    args.push("-rtsp_transport".into());
    args.push("http".into());

    args.push("-i".into());
    args.push(input_url.to_string());

    // --- Output options: remux (no transcode) and publish over TCP-interleaved RTSP ---
    args.push("-map".into());
    args.push("0:v:0".into());
    if include_audio {
        // Best-effort audio; do not fail if the source has no audio track.
        args.push("-map".into());
        args.push("0:a:0?".into());
    }
    args.push("-c".into());
    args.push("copy".into());
    args.push("-f".into());
    args.push("rtsp".into());
    args.push("-rtsp_transport".into());
    args.push("tcp".into());
    args.push(publish_url.to_string());

    args
}

/// A running ffmpeg ingest process for one camera.
pub struct FfmpegIngest {
    child: Child,
    label: String,
    /// Kept alive for the ingest's lifetime; dropped (and its accept loop aborted)
    /// when this ingest stops. `None` if the upstream URL wasn't `rtsp://` (i.e. no
    /// TLS tunnel to terminate).
    _relay: Option<TlsRelay>,
}

impl FfmpegIngest {
    /// Spawn ffmpeg for `target`, publishing into `publish_url`.
    pub async fn spawn(
        cfg: &Config,
        target: &VideoTarget,
        publish_url: &str,
        include_audio: bool,
        label: impl Into<String>,
    ) -> Result<Self> {
        let label = label.into();
        let mut input_url = target.url_with_userinfo()?;

        let relay = if input_url.scheme() == "rtsp" {
            let host = input_url
                .host_str()
                .ok_or_else(|| ProxyError::Backend("video URL has no host".to_string()))?
                .to_string();
            let port = input_url.port_or_known_default().unwrap_or(443);
            let ca_pem = if cfg.tls_insecure { None } else { Some(cfg.ca_pem()?) };
            let relay = TlsRelay::spawn(host, port, ca_pem.as_deref()).await?;

            input_url.set_host(Some("127.0.0.1")).map_err(|_| {
                ProxyError::Backend("cannot rewrite video URL to loopback host".into())
            })?;
            input_url
                .set_port(Some(relay.local_addr().port()))
                .map_err(|_| {
                    ProxyError::Backend("cannot rewrite video URL to loopback port".into())
                })?;
            Some(relay)
        } else {
            None
        };

        let args = build_ffmpeg_args(input_url.as_str(), publish_url, include_audio);

        // Redacted form for logs (never print credentials).
        debug!(camera = %label, "spawning ffmpeg -> {publish_url}");

        let mut command = Command::new("ffmpeg");
        command
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command
            .spawn()
            .map_err(|e| ProxyError::Ingest(format!("failed to spawn ffmpeg: {e}")))?;

        // Forward ffmpeg's stderr into our logs.
        if let Some(stderr) = child.stderr.take() {
            let log_label = label.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    warn!(camera = %log_label, "ffmpeg: {line}");
                }
            });
        }

        Ok(Self {
            child,
            label,
            _relay: relay,
        })
    }

    /// Terminate the process (called when the last consumer disconnects).
    pub async fn stop(&mut self) {
        debug!(camera = %self.label, "stopping ffmpeg ingest");
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }

    /// Resolves when the ffmpeg process exits on its own (e.g. camera rejected the
    /// upstream credentials). Used by [`crate::stream_manager`] to detect a dead
    /// ingest without waiting out the full SDP timeout.
    pub async fn wait_exit(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.child.wait().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn args_use_local_http_tunnel_and_copy_codec() {
        let args = build_ffmpeg_args(
            "rtsp://u:p@127.0.0.1:54321/rtsp_tunnel?inst=1",
            "rtsp://127.0.0.1:8554/internal/cam1",
            true,
        );
        assert!(args.windows(2).any(|w| w == ["-rtsp_transport", "http"]));
        assert!(args.windows(2).any(|w| w == ["-rtsp_transport", "tcp"]));
        let joined = args.join(" ");
        assert!(joined.contains("-c copy"));
        assert!(joined.contains("0:a:0?"));
        assert!(args.last().unwrap().starts_with("rtsp://127.0.0.1:8554"));
    }

    #[test]
    fn audio_flag_omits_second_map_when_disabled() {
        let args = build_ffmpeg_args(
            "rtsp://u:p@127.0.0.1:54321/rtsp_tunnel?inst=1",
            "rtsp://127.0.0.1:8554/internal/cam1",
            false,
        );
        assert!(!args.join(" ").contains("0:a:0?"));
    }
}
