//! Synthetic "connecting to camera..." placeholder, published on the same stream
//! path while the real upstream ingest is starting so subscribers get instant
//! playback instead of a blocked DESCRIBE. Disable via `RTSP_CONNECTING_PLACEHOLDER=0`.
//!
//! Handed over to the real feed via [`crate::rtsp::Stream`]'s publisher-epoch
//! mechanism: both this and the real [`crate::ingest::FfmpegIngest`] publish
//! (ANNOUNCE) on the same path, and whichever announced most recently wins — see
//! `Stream::begin_publisher`/`is_active_epoch`. `StreamManager` stops this process as
//! soon as the real one takes over (or once it gives up retrying).

use std::path::Path;
use std::process::Stdio;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tracing::{debug, warn};

use crate::error::{ProxyError, Result};

/// System fonts to try for the placeholder's text overlay, in order of preference.
/// ffmpeg's `drawtext` needs a concrete font file — fontconfig discovery is
/// unreliable on Windows and no font ships in the minimal Docker image at all —
/// so this is checked at runtime rather than assumed; if none exist, the
/// placeholder falls back to a plain color card.
const FONT_CANDIDATES: &[&str] = &[
    "C:/Windows/Fonts/segoeui.ttf",
    "C:/Windows/Fonts/arial.ttf",
    "C:/Windows/Fonts/calibri.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
];

fn drawtext_filter() -> Option<String> {
    let font = FONT_CANDIDATES.iter().find(|p| Path::new(p).exists())?;
    Some(drawtext_filter_for_font(font))
}

fn drawtext_filter_for_font(font: &str) -> String {
    // ffmpeg filter-option syntax uses ':' as a separator, which collides with the
    // drive-letter colon in a Windows path -- escape it.
    let escaped = font.replace(':', "\\:");
    format!(
        "drawtext=fontfile='{escaped}':text='Connecting to camera...':fontcolor=white:fontsize=54:x=(w-text_w)/2:y=(h-text_h)/2"
    )
}

/// Assemble the ffmpeg argument vector. `vf` is the resolved `drawtext_filter()`
/// result (or `None` for a plain color card, e.g. when no system font was found).
fn build_args(publish_url: &str, include_audio: bool, vf: Option<&str>) -> Vec<String> {
    let mut args: Vec<String> = vec![
        "-hide_banner".into(),
        "-loglevel".into(),
        "warning".into(),
        "-f".into(),
        "lavfi".into(),
        "-re".into(),
        "-i".into(),
        "color=c=black:s=1280x720:r=25".into(),
    ];
    if include_audio {
        args.push("-f".into());
        args.push("lavfi".into());
        args.push("-re".into());
        args.push("-i".into());
        // Silent audio matching the real camera's observed AAC format (16kHz mono) so
        // the handover doesn't hand a decoder a new sample rate.
        args.push("anullsrc=r=16000:cl=mono".into());
    }

    if let Some(vf) = vf {
        args.push("-vf".into());
        args.push(vf.to_string());
    }

    args.push("-map".into());
    args.push("0:v:0".into());
    if include_audio {
        args.push("-map".into());
        args.push("1:a:0".into());
    }
    args.push("-c:v".into());
    args.push("libx264".into());
    args.push("-preset".into());
    args.push("ultrafast".into());
    args.push("-tune".into());
    args.push("zerolatency".into());
    args.push("-g".into());
    args.push("50".into());
    if include_audio {
        args.push("-c:a".into());
        args.push("aac".into());
    }
    args.push("-f".into());
    args.push("rtsp".into());
    args.push("-rtsp_transport".into());
    args.push("tcp".into());
    args.push(publish_url.to_string());

    args
}

/// A running placeholder ingest for one stream path.
pub struct PlaceholderIngest {
    child: Child,
    label: String,
}

impl PlaceholderIngest {
    /// Spawn ffmpeg generating a synthetic "connecting..." clip and publishing it
    /// into `publish_url` (our own local RTSP server). `include_audio` matches the
    /// real ingest's own flag so the placeholder's SDP has the same track shape the
    /// real stream is expected to (same track count/order -> ffmpeg's interleaved
    /// channel numbering for both ends up identical, so a subscriber's earlier SETUP
    /// keeps working once the real feed takes over).
    pub fn spawn(
        publish_url: &str,
        include_audio: bool,
        label: impl Into<String>,
    ) -> Result<Self> {
        let label = label.into();

        let vf = drawtext_filter();
        if vf.is_none() {
            warn!(
                "no system font found for connecting-placeholder text; showing a plain color card"
            );
        }
        let args = build_args(publish_url, include_audio, vf.as_deref());

        debug!(camera = %label, "spawning connecting-placeholder ffmpeg -> {publish_url}");

        let mut command = Command::new("ffmpeg");
        command
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().map_err(|e| {
            ProxyError::Ingest(format!("failed to spawn placeholder ffmpeg: {e}"))
        })?;

        if let Some(stderr) = child.stderr.take() {
            let log_label = label.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    debug!(camera = %log_label, "placeholder ffmpeg: {line}");
                }
            });
        }

        Ok(Self { child, label })
    }

    pub async fn stop(&mut self) {
        debug!(camera = %self.label, "stopping connecting-placeholder ingest");
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn includes_matching_audio_track_and_mapping() {
        let args = build_args("rtsp://127.0.0.1:8554/internal/cam1", true, None);
        assert!(args.windows(2).any(|w| w == ["-map", "0:v:0"]));
        assert!(args.windows(2).any(|w| w == ["-map", "1:a:0"]));
        let joined = args.join(" ");
        assert!(joined.contains("anullsrc=r=16000:cl=mono"));
        assert!(joined.contains("-c:a aac"));
        assert!(args.last().unwrap().starts_with("rtsp://127.0.0.1:8554"));
    }

    #[test]
    fn omits_audio_track_when_disabled() {
        let args = build_args("rtsp://127.0.0.1:8554/internal/cam1", false, None);
        assert!(!args.join(" ").contains("anullsrc"));
        assert!(!args.windows(2).any(|w| w == ["-map", "1:a:0"]));
    }

    #[test]
    fn includes_drawtext_filter_when_font_available() {
        let vf = drawtext_filter_for_font("C:/Windows/Fonts/arial.ttf");
        let args = build_args("rtsp://127.0.0.1:8554/internal/cam1", false, Some(&vf));
        assert!(args.windows(2).any(|w| w[0] == "-vf" && w[1].contains("Connecting to camera")));
    }

    #[test]
    fn escapes_drive_letter_colon_in_font_path() {
        let vf = drawtext_filter_for_font("C:/Windows/Fonts/arial.ttf");
        assert!(vf.contains("fontfile='C\\:/Windows/Fonts/arial.ttf'"));
    }
}
