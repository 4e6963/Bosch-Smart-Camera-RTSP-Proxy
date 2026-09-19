//! RTSP TCP server: accept loop and per-connection publisher/subscriber handling.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use url::Url;

use crate::camera::Route;
use crate::config::Config;
use crate::error::{ProxyError, Result};
use crate::stream_manager::StreamManager;

use super::message::{self, Frame, Request, Response};
use super::{Packet, Stream, StreamRegistry};

const SERVER_NAME: &str = "bosch-cam-proxy";
const SUPPORTED_METHODS: &str =
    "OPTIONS, DESCRIBE, ANNOUNCE, SETUP, PLAY, RECORD, PAUSE, TEARDOWN, GET_PARAMETER";
const SDP_WAIT: Duration = Duration::from_secs(20);

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

fn new_session_id() -> String {
    let n = SESSION_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{:016X}", n.wrapping_mul(0x9E3779B97F4A7C15))
}

/// The parsed target of an RTSP request URI.
struct StreamTarget {
    /// Registry/publish key. Usually just the path (leading slash and query
    /// stripped), but a non-default `high_quality` is folded in too (see below) so
    /// it can't collide with an already-running stream of the other quality.
    key: String,
    camera_id: String,
    route: Route,
    /// Request the camera's full-resolution main stream (default, `true`) or its
    /// lower-resolution sub-stream (`?quality=low`) -- independent of `route`, see
    /// `BackendClient::get_credentials`.
    high_quality: bool,
}

fn parse_target(uri: &str, default_route: Route) -> Result<StreamTarget> {
    // URIs may be absolute (`rtsp://host/cam1/relay`) or, for some clients, a path.
    let parsed = Url::parse(uri).or_else(|_| Url::parse(&format!("rtsp://placeholder/{}", uri.trim_start_matches('/'))));
    let parsed = parsed.map_err(|e| ProxyError::Rtsp(format!("bad request URI '{uri}': {e}")))?;

    let segments: Vec<String> = parsed
        .path_segments()
        .map(|s| s.filter(|seg| !seg.is_empty()).map(str::to_string).collect())
        .unwrap_or_default();

    let camera_id = segments
        .first()
        .cloned()
        .ok_or_else(|| ProxyError::Rtsp(format!("request URI '{uri}' has no camera id")))?;

    let route = match segments.get(1).map(String::as_str) {
        Some("relay") => Route::Relay,
        Some("local") => Route::Local,
        _ => default_route,
    };

    let high_quality = !parsed
        .query_pairs()
        .any(|(k, v)| k == "quality" && v.eq_ignore_ascii_case("low"));

    let path = segments.join("/");
    // Quality isn't part of the path, so fold a non-default value into the registry
    // key -- otherwise a `?quality=low` request could silently reuse (or collide
    // with) an already-running stream of the other quality on the same path.
    let key = if high_quality {
        path
    } else {
        format!("{path}?quality=low")
    };

    Ok(StreamTarget {
        key,
        camera_id,
        route,
        high_quality,
    })
}

/// The local RTSP server.
pub struct RtspServer {
    cfg: Arc<Config>,
    registry: Arc<StreamRegistry>,
    manager: Arc<StreamManager>,
}

impl RtspServer {
    pub fn new(
        cfg: Arc<Config>,
        registry: Arc<StreamRegistry>,
        manager: Arc<StreamManager>,
    ) -> Self {
        Self {
            cfg,
            registry,
            manager,
        }
    }

    /// Bind and serve until the process is stopped.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(self.cfg.rtsp_bind).await?;
        info!("RTSP server listening on rtsp://{}/<camera-id>", self.cfg.rtsp_bind);

        loop {
            let (socket, peer) = listener.accept().await?;
            let server = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = server.handle_connection(socket, peer).await {
                    debug!(%peer, "connection ended: {e}");
                }
            });
        }
    }

    async fn handle_connection(&self, socket: TcpStream, peer: SocketAddr) -> Result<()> {
        socket.set_nodelay(true).ok();
        let (mut reader, writer) = socket.into_split();

        // All writes funnel through one task so control responses and relayed media
        // never interleave mid-frame.
        let (out_tx, mut out_rx) = mpsc::channel::<Vec<u8>>(4096);
        let writer_task = tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let mut writer = writer;
            while let Some(bytes) = out_rx.recv().await {
                if writer.write_all(&bytes).await.is_err() {
                    break;
                }
                let _ = writer.flush().await;
            }
        });

        let mut conn = ConnState::new(peer);

        let result = loop {
            match message::read_frame(&mut reader).await {
                Ok(Some(Frame::Request(req))) => {
                    if let Err(e) = self.handle_request(&mut conn, &req, &out_tx).await {
                        // Report the failure to the client, then keep the session.
                        warn!(%peer, method = %req.method, "request error: {e}");
                        let resp = Response::error(req.cseq(), 500, "Internal Server Error");
                        let _ = out_tx.send(resp.encode()).await;
                    }
                    if conn.teardown {
                        break Ok(());
                    }
                }
                Ok(Some(Frame::Interleaved { channel, data })) => {
                    // Only meaningful from a publisher: relay to subscribers. Gated on
                    // epoch so a superseded publisher (e.g. a "connecting..."
                    // placeholder still shutting down after the real feed took over)
                    // can't corrupt the active one's packets.
                    if let Some(stream) = &conn.stream {
                        if conn.role == Role::Publisher {
                            if let Some(epoch) = conn.epoch {
                                if stream.is_active_epoch(epoch) {
                                    stream.publish(Packet { channel, data });
                                }
                            }
                        }
                    }
                }
                Ok(None) => break Ok(()),
                Err(e) => break Err(e),
            }
        };

        self.on_disconnect(&mut conn).await;
        drop(out_tx);
        let _ = writer_task.await;
        result
    }

    async fn handle_request(
        &self,
        conn: &mut ConnState,
        req: &Request,
        out_tx: &mpsc::Sender<Vec<u8>>,
    ) -> Result<()> {
        debug!(peer = %conn.peer, "{} {}", req.method, req.uri);
        match req.method.as_str() {
            "OPTIONS" => {
                let resp = Response::ok(req.cseq())
                    .header("Public", SUPPORTED_METHODS)
                    .header("Server", SERVER_NAME);
                out_tx.send(resp.encode()).await.ok();
            }
            "GET_PARAMETER" => {
                // Used as a keepalive; an empty 200 is sufficient.
                out_tx.send(Response::ok(req.cseq()).encode()).await.ok();
            }
            "ANNOUNCE" => self.handle_announce(conn, req, out_tx).await?,
            "DESCRIBE" => self.handle_describe(conn, req, out_tx).await?,
            "SETUP" => self.handle_setup(conn, req, out_tx).await?,
            "PLAY" => self.handle_play(conn, req, out_tx).await?,
            "RECORD" => {
                out_tx
                    .send(Response::ok(req.cseq()).session_opt(&conn.session).encode())
                    .await
                    .ok();
            }
            "PAUSE" => {
                out_tx.send(Response::ok(req.cseq()).encode()).await.ok();
            }
            "TEARDOWN" => {
                out_tx
                    .send(Response::ok(req.cseq()).session_opt(&conn.session).encode())
                    .await
                    .ok();
                conn.teardown = true;
            }
            other => {
                warn!(peer = %conn.peer, "unsupported method {other}");
                out_tx
                    .send(Response::error(req.cseq(), 501, "Not Implemented").encode())
                    .await
                    .ok();
            }
        }
        Ok(())
    }

    /// Publisher path: ffmpeg announces the stream's SDP.
    async fn handle_announce(
        &self,
        conn: &mut ConnState,
        req: &Request,
        out_tx: &mpsc::Sender<Vec<u8>>,
    ) -> Result<()> {
        let target = parse_target(&req.uri, self.cfg.default_route)?;
        let stream = self.registry.get_or_create(&target.key).await;
        let sdp = String::from_utf8_lossy(&req.body).to_string();
        let epoch = stream.begin_publisher(sdp).await;

        conn.role = Role::Publisher;
        conn.stream = Some(Arc::clone(&stream));
        conn.key = Some(target.key);
        conn.epoch = Some(epoch);
        info!(path = %stream.path, epoch, "publisher connected");

        out_tx.send(Response::ok(req.cseq()).encode()).await.ok();
        Ok(())
    }

    /// Subscriber path: trigger ingest if needed and return the SDP.
    async fn handle_describe(
        &self,
        conn: &mut ConnState,
        req: &Request,
        out_tx: &mpsc::Sender<Vec<u8>>,
    ) -> Result<()> {
        let target = parse_target(&req.uri, self.cfg.default_route)?;
        let stream = self.registry.get_or_create(&target.key).await;

        // Start the upstream ingest on demand if no publisher is present yet.
        if !stream.publisher_present().await {
            self.manager
                .ensure_publisher(&target.key, &target.camera_id, target.route, target.high_quality)
                .await?;
        }

        let sdp = stream.wait_for_sdp(SDP_WAIT).await.ok_or_else(|| {
            ProxyError::Rtsp(format!(
                "upstream did not produce an SDP for '{}' within {:?}",
                target.key, SDP_WAIT
            ))
        })?;

        conn.stream = Some(Arc::clone(&stream));
        conn.key = Some(target.key.clone());

        let content_base = format!("{}/", req.uri.split('?').next().unwrap_or(&req.uri));
        let resp = Response::ok(req.cseq())
            .header("Content-Base", content_base)
            .with_body("application/sdp", sdp.into_bytes());
        out_tx.send(resp.encode()).await.ok();
        Ok(())
    }

    /// Both roles negotiate TCP-interleaved transport here; we echo the requested
    /// channels back and hand out a session id.
    async fn handle_setup(
        &self,
        conn: &mut ConnState,
        req: &Request,
        out_tx: &mpsc::Sender<Vec<u8>>,
    ) -> Result<()> {
        let transport = req.header("transport").unwrap_or("");
        if !transport.contains("TCP") && !transport.contains("interleaved") {
            // We only support interleaved (TCP) transport.
            out_tx
                .send(Response::error(req.cseq(), 461, "Unsupported Transport").encode())
                .await
                .ok();
            return Ok(());
        }

        let interleaved = parse_interleaved(transport).unwrap_or_else(|| conn.next_channels());
        let session = conn
            .session
            .get_or_insert_with(new_session_id)
            .clone();

        let resp = Response::ok(req.cseq())
            .header(
                "Transport",
                format!(
                    "RTP/AVP/TCP;unicast;interleaved={}-{}",
                    interleaved.0, interleaved.1
                ),
            )
            .header("Session", format!("{session};timeout=60"));
        out_tx.send(resp.encode()).await.ok();
        Ok(())
    }

    /// Subscriber PLAY: begin relaying media to this connection.
    async fn handle_play(
        &self,
        conn: &mut ConnState,
        req: &Request,
        out_tx: &mpsc::Sender<Vec<u8>>,
    ) -> Result<()> {
        let stream = conn
            .stream
            .clone()
            .ok_or_else(|| ProxyError::Rtsp("PLAY before DESCRIBE/SETUP".into()))?;

        if !conn.playing {
            conn.playing = true;
            conn.role = Role::Subscriber;
            let count = stream.add_subscriber();
            conn.counted = true;
            info!(path = %stream.path, peer = %conn.peer, subscribers = count, "subscriber started PLAY");

            // Spawn the media relay for this subscriber.
            let mut rx = stream.subscribe();
            let media_tx = out_tx.clone();
            let handle = tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(pkt) => {
                            if let Some(bytes) = message::encode_interleaved(pkt.channel, &pkt.data)
                            {
                                if media_tx.send(bytes).await.is_err() {
                                    break; // consumer socket closed
                                }
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            warn!("subscriber lagged, dropped {n} packets");
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
            conn.media_task = Some(handle);
        }

        let session = conn.session.clone().unwrap_or_default();
        let resp = Response::ok(req.cseq()).header("Session", session);
        out_tx.send(resp.encode()).await.ok();
        Ok(())
    }

    async fn on_disconnect(&self, conn: &mut ConnState) {
        if let Some(handle) = conn.media_task.take() {
            handle.abort();
        }

        let Some(stream) = conn.stream.clone() else {
            return;
        };

        match conn.role {
            Role::Subscriber if conn.counted => {
                let remaining = stream.remove_subscriber();
                info!(path = %stream.path, subscribers = remaining, "subscriber disconnected");
                if remaining == 0 {
                    if let Some(key) = &conn.key {
                        self.manager.release(key).await;
                    }
                }
            }
            Role::Publisher => {
                info!(path = %stream.path, "publisher disconnected");
                if let Some(epoch) = conn.epoch {
                    stream.clear_publisher_if(epoch).await;
                }
            }
            _ => {}
        }
    }
}

#[derive(PartialEq, Eq, Clone, Copy)]
enum Role {
    Unknown,
    Publisher,
    Subscriber,
}

struct ConnState {
    peer: SocketAddr,
    role: Role,
    stream: Option<Arc<Stream>>,
    key: Option<String>,
    session: Option<String>,
    playing: bool,
    counted: bool,
    teardown: bool,
    channel_counter: u8,
    media_task: Option<tokio::task::JoinHandle<()>>,
    /// Set on ANNOUNCE (publisher only): this connection's publisher epoch, see
    /// [`Stream::begin_publisher`].
    epoch: Option<u64>,
}

impl ConnState {
    fn new(peer: SocketAddr) -> Self {
        Self {
            peer,
            role: Role::Unknown,
            stream: None,
            key: None,
            session: None,
            playing: false,
            counted: false,
            teardown: false,
            channel_counter: 0,
            media_task: None,
            epoch: None,
        }
    }

    /// Assign the next pair of interleaved channels when a client did not request one.
    fn next_channels(&mut self) -> (u8, u8) {
        let base = self.channel_counter;
        self.channel_counter = self.channel_counter.wrapping_add(2);
        (base, base + 1)
    }
}

/// Parse `interleaved=A-B` out of a Transport header.
fn parse_interleaved(transport: &str) -> Option<(u8, u8)> {
    let part = transport.split(';').find_map(|p| p.trim().strip_prefix("interleaved="))?;
    let (a, b) = part.split_once('-')?;
    Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
}

// Small ergonomic helper for optional Session headers.
trait SessionExt {
    fn session_opt(self, session: &Option<String>) -> Self;
}

impl SessionExt for Response {
    fn session_opt(self, session: &Option<String>) -> Self {
        match session {
            Some(s) => self.header("Session", s.clone()),
            None => self,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_interleaved_channels() {
        assert_eq!(
            parse_interleaved("RTP/AVP/TCP;unicast;interleaved=2-3"),
            Some((2, 3))
        );
        assert_eq!(parse_interleaved("RTP/AVP/TCP;unicast"), None);
    }

    #[test]
    fn parses_target_with_relay_route() {
        let t = parse_target("rtsp://host:8554/cam1/relay", Route::Local).unwrap();
        assert_eq!(t.camera_id, "cam1");
        assert_eq!(t.route, Route::Relay);
        assert_eq!(t.key, "cam1/relay");
        assert!(t.high_quality);
    }

    #[test]
    fn parses_bare_camera_path_with_default_route() {
        let t = parse_target("rtsp://host:8554/cam1", Route::Local).unwrap();
        assert_eq!(t.camera_id, "cam1");
        assert_eq!(t.route, Route::Local);
        assert_eq!(t.key, "cam1");
        assert!(t.high_quality);
    }

    #[test]
    fn quality_low_requests_low_quality_and_folds_into_key() {
        let t = parse_target("rtsp://host:8554/cam1?quality=low", Route::Local).unwrap();
        assert!(!t.high_quality);
        assert_eq!(t.key, "cam1?quality=low");
    }

    #[test]
    fn quality_low_is_case_insensitive_and_independent_of_route() {
        let t = parse_target("rtsp://host:8554/cam1/relay?quality=LOW", Route::Local).unwrap();
        assert_eq!(t.route, Route::Relay);
        assert!(!t.high_quality);
        assert_eq!(t.key, "cam1/relay?quality=low");
    }

    #[test]
    fn unrecognized_quality_value_defaults_to_high() {
        let t = parse_target("rtsp://host:8554/cam1?quality=hd", Route::Local).unwrap();
        assert!(t.high_quality);
        assert_eq!(t.key, "cam1");
    }
}
