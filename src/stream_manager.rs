//! On-demand orchestration: ties consumer demand to the upstream ingest lifecycle.
//!
//! The camera connection is held only while a consumer is attached (doc requirement):
//! the RTSP server calls [`StreamManager::ensure_publisher`] when a subscriber issues
//! DESCRIBE, and [`StreamManager::release`] when the last subscriber disconnects.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::backend::BackendClient;
use crate::camera::Route;
use crate::config::Config;
use crate::error::{ProxyError, Result};
use crate::ingest::FfmpegIngest;
use crate::placeholder::PlaceholderIngest;
use crate::rtsp::{Stream, StreamRegistry};

/// Grace period after the last subscriber leaves before the ingest is torn down,
/// so a client that briefly reconnects does not thrash ffmpeg.
const LINGER: Duration = Duration::from_secs(3);

/// The camera occasionally rejects a freshly-issued temporary credential on the very
/// first connection attempt (401 on the tunnel's initial OPTIONS) and accepts a
/// re-issued one moments later — observed in practice, cause unconfirmed (likely
/// propagation lag between the backend issuing credentials and the camera accepting
/// them). Retry a few times with fresh credentials before giving up, so a single
/// DESCRIBE self-heals instead of depending on the client reconnecting.
const MAX_INGEST_ATTEMPTS: u32 = 3;
/// How long to wait, per attempt, for either a publisher to appear or ffmpeg to exit.
const ATTEMPT_WAIT: Duration = Duration::from_secs(8);
/// How long to wait for the "connecting..." placeholder to actually announce before
/// starting the real ingest attempts. This establishes the correct baseline epoch for
/// `wait_for_new_publisher` below: spawning the placeholder process is non-blocking,
/// so without this wait its own (asynchronous) announce could land *during* the real
/// attempt's wait and be mistaken for the real ingest arriving.
const PLACEHOLDER_STARTUP_WAIT: Duration = Duration::from_secs(3);

pub struct StreamManager {
    cfg: Arc<Config>,
    backend: Arc<BackendClient>,
    registry: Arc<StreamRegistry>,
    /// Active (live) ingest processes keyed by stream path.
    ingests: Mutex<HashMap<String, FfmpegIngest>>,
    /// Keys with a real-ingest orchestration currently in flight (placeholder up,
    /// real attempts running in the background). Distinct from `ingests`, which only
    /// holds *completed* ingests: without this, a second concurrent DESCRIBE arriving
    /// after the placeholder announces but before the real ingest lands would see no
    /// tracked ingest yet and spawn a duplicate placeholder/attempt for the same key.
    in_flight: Mutex<HashSet<String>>,
}

impl StreamManager {
    pub fn new(
        cfg: Arc<Config>,
        backend: Arc<BackendClient>,
        registry: Arc<StreamRegistry>,
    ) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            backend,
            registry,
            ingests: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashSet::new()),
        })
    }

    /// The loopback URL ffmpeg publishes into (our own RTSP server).
    fn publish_url(&self, key: &str) -> String {
        format!("rtsp://127.0.0.1:{}/{}", self.cfg.rtsp_bind.port(), key)
    }

    /// Ensure an ingest is running for `key`, starting it if necessary. Idempotent.
    ///
    /// With the "connecting..." placeholder enabled, this returns as soon as the
    /// placeholder is live rather than waiting out the real ingest's full cold-start
    /// (the point of the feature: the caller's own SDP wait picks up the placeholder
    /// immediately, and the real ingest continues connecting in the background,
    /// handing over to it automatically once it publishes — see [`crate::rtsp::Stream`]'s
    /// publisher-epoch mechanism). Without a usable placeholder, this falls back to
    /// blocking until the real ingest is ready or exhausted, as before.
    pub async fn ensure_publisher(
        self: &Arc<Self>,
        key: &str,
        camera_id: &str,
        route: Route,
        high_quality: bool,
    ) -> Result<()> {
        let stream = self.registry.get_or_create(key).await;

        // Already something being served (placeholder or real, ours or a concurrent
        // caller's)? Nothing to do -- the caller's own SDP wait picks up whatever's
        // current.
        if stream.publisher_present().await {
            return Ok(());
        }

        // Claim this key so a concurrent DESCRIBE (arriving after our placeholder
        // announces but before the real ingest lands, when `publisher_present()` is
        // already true but nothing is in `ingests` yet) doesn't spawn a duplicate
        // orchestration. If someone else already claimed it, just defer to them.
        {
            let mut in_flight = self.in_flight.lock().await;
            if !in_flight.insert(key.to_string()) {
                return Ok(());
            }
        }

        // Clean up a stale (dead) tracked ingest, if any.
        if let Some(mut old) = self.ingests.lock().await.remove(key) {
            old.stop().await;
        }

        let epoch_before_placeholder = stream.active_epoch();
        let mut placeholder = self.spawn_placeholder(key, camera_id);

        // If it's running, wait for it to actually announce so *its* epoch becomes the
        // baseline the real ingest must beat below -- otherwise the placeholder's own
        // announce (asynchronous relative to spawning the process above) could land
        // during the real attempt's wait and be mistaken for the real ingest arriving.
        if placeholder.is_some() {
            stream
                .wait_for_new_publisher(epoch_before_placeholder, PLACEHOLDER_STARTUP_WAIT)
                .await;
        }
        let baseline_epoch = stream.active_epoch();
        let placeholder_live = placeholder.is_some() && baseline_epoch > epoch_before_placeholder;

        if placeholder_live {
            // Hand the real ingest off to a background task and return immediately so
            // the caller can serve the placeholder right away.
            let manager = Arc::clone(self);
            let key = key.to_string();
            let camera_id = camera_id.to_string();
            tokio::spawn(async move {
                let result = manager
                    .try_spawn_real_ingest(&key, &camera_id, route, high_quality, &stream, baseline_epoch)
                    .await;
                if let Some(mut p) = placeholder.take() {
                    p.stop().await;
                }
                match result {
                    Ok(ingest) => {
                        manager.ingests.lock().await.insert(key.clone(), ingest);
                    }
                    Err(e) => {
                        warn!(camera = %camera_id, "upstream ingest failed after connecting-placeholder: {e}");
                    }
                }
                manager.in_flight.lock().await.remove(&key);
            });
            return Ok(());
        }

        // No usable placeholder (disabled, or it failed to start/announce): block
        // until the real ingest is ready or exhausted, same as before this feature.
        let result = self
            .try_spawn_real_ingest(key, camera_id, route, high_quality, &stream, baseline_epoch)
            .await;

        if let Some(mut p) = placeholder.take() {
            p.stop().await;
        }
        self.in_flight.lock().await.remove(key);

        let ingest = result?;
        self.ingests.lock().await.insert(key.to_string(), ingest);
        Ok(())
    }

    /// Start the "connecting..." placeholder, or `None` if disabled or it failed to
    /// spawn (best-effort only — never blocks the real ingest attempt).
    fn spawn_placeholder(&self, key: &str, camera_id: &str) -> Option<PlaceholderIngest> {
        if !self.cfg.connecting_placeholder {
            return None;
        }
        let publish_url = self.publish_url(key);
        match PlaceholderIngest::spawn(&publish_url, self.cfg.include_audio, camera_id.to_string()) {
            Ok(p) => Some(p),
            Err(e) => {
                warn!(camera = camera_id, "failed to start connecting-placeholder: {e}");
                None
            }
        }
    }

    /// Try the real upstream ingest, retrying with fresh credentials up to
    /// `MAX_INGEST_ATTEMPTS` times (see `MAX_INGEST_ATTEMPTS` docs).
    async fn try_spawn_real_ingest(
        &self,
        key: &str,
        camera_id: &str,
        route: Route,
        high_quality: bool,
        stream: &Stream,
        baseline_epoch: u64,
    ) -> Result<FfmpegIngest> {
        let mut last_err = None;

        for attempt in 1..=MAX_INGEST_ATTEMPTS {
            let creds = self.backend.get_credentials(camera_id, route, high_quality).await?;
            let target = creds.video_target(route)?;
            let host = target.url.host_str().unwrap_or("?");
            let port = target.url.port_or_known_default().unwrap_or(0);
            info!(
                camera = camera_id,
                route = route.as_query(),
                high_quality,
                attempt,
                host,
                port,
                "starting upstream ingest"
            );
            let publish_url = self.publish_url(key);

            let mut ingest = FfmpegIngest::spawn(
                &self.cfg,
                &target,
                &publish_url,
                self.cfg.include_audio,
                camera_id.to_string(),
            )
            .await?;

            let published = tokio::select! {
                sdp = stream.wait_for_new_publisher(baseline_epoch, ATTEMPT_WAIT) => sdp.is_some(),
                _ = ingest.wait_exit() => false,
            };

            if published {
                return Ok(ingest);
            }

            warn!(camera = camera_id, attempt, "ingest attempt did not produce a publisher; retrying");
            ingest.stop().await;
            // The cached credential (if this attempt used one) may be what the
            // camera rejected; drop it so the next attempt fetches a fresh one
            // instead of retrying with the same one that just failed.
            self.backend
                .invalidate_credentials(camera_id, route, high_quality)
                .await;
            last_err = Some(ProxyError::Ingest(format!(
                "ffmpeg exited without publishing (attempt {attempt}/{MAX_INGEST_ATTEMPTS})"
            )));
        }

        Err(last_err.unwrap_or_else(|| ProxyError::Ingest("upstream ingest failed".to_string())))
    }

    /// Called when a subscriber leaves. After a linger, if no subscribers remain, the
    /// ingest is stopped.
    pub async fn release(self: &Arc<Self>, key: &str) {
        let manager = Arc::clone(self);
        let key = key.to_string();
        tokio::spawn(async move {
            tokio::time::sleep(LINGER).await;

            let still_active = manager
                .registry
                .get(&key)
                .await
                .map(|s| s.subscriber_count() > 0)
                .unwrap_or(false);
            if still_active {
                return;
            }

            let ingest = manager.ingests.lock().await.remove(&key);
            if let Some(mut ingest) = ingest {
                info!(path = %key, "no consumers left; stopping upstream ingest");
                ingest.stop().await;
            }
            if let Some(stream) = manager.registry.get(&key).await {
                stream.force_clear_publisher().await;
            }
        });
    }

    /// Stop every ingest (used on shutdown).
    pub async fn shutdown(&self) {
        let mut ingests = self.ingests.lock().await;
        for (key, mut ingest) in ingests.drain() {
            warn!(path = %key, "stopping ingest on shutdown");
            ingest.stop().await;
        }
    }
}
