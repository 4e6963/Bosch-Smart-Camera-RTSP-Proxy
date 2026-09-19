//! Local RTSP server: a minimal publish/subscribe relay (the MediaMTX model).
//!
//! An `ffmpeg` ingest process connects as a *publisher* (ANNOUNCE/RECORD) on a path;
//! consumers connect as *subscribers* (DESCRIBE/PLAY) on the same path and receive the
//! relayed interleaved RTP. All media is TCP-interleaved (no UDP port negotiation).

pub mod message;
pub mod server;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, Mutex, Notify};

pub use server::RtspServer;

/// One interleaved media packet as relayed from publisher to subscribers.
#[derive(Debug, Clone)]
pub struct Packet {
    pub channel: u8,
    pub data: Vec<u8>,
}

/// Shared state for a single stream path.
pub struct Stream {
    pub path: String,
    inner: Mutex<StreamInner>,
    /// Broadcast of relayed packets. Lagging subscribers drop packets rather than
    /// stalling the publisher — appropriate for live video.
    tx: broadcast::Sender<Arc<Packet>>,
    /// Notified whenever the SDP becomes available.
    sdp_ready: Notify,
    subscribers: AtomicUsize,
    /// Monotonic counter handed out to each publisher that announces (see
    /// [`Self::begin_publisher`]).
    epoch_counter: AtomicU64,
    /// The current publisher's epoch, or 0 if none. Lets a "connecting..." placeholder
    /// and the real upstream both publish on the same path without their packets
    /// colliding: only the most recent publisher's frames are actually relayed, and a
    /// stale publisher's disconnect can't clobber a newer one's state. Plain atomics
    /// (not behind `inner`'s lock) since [`Self::is_active_epoch`] is checked on the
    /// hot per-packet relay path.
    active_epoch: AtomicU64,
}

#[derive(Default)]
struct StreamInner {
    /// SDP announced by the publisher (served to subscribers on DESCRIBE).
    sdp: Option<String>,
    publisher_present: bool,
}

impl Stream {
    fn new(path: String) -> Self {
        let (tx, _rx) = broadcast::channel(2048);
        Self {
            path,
            inner: Mutex::new(StreamInner::default()),
            tx,
            sdp_ready: Notify::new(),
            subscribers: AtomicUsize::new(0),
            epoch_counter: AtomicU64::new(0),
            active_epoch: AtomicU64::new(0),
        }
    }

    /// Publisher: record the announced SDP, mark the publisher present, and become
    /// the stream's active publisher — superseding any previous one (e.g. a
    /// "connecting..." placeholder handing over to the real upstream). Returns this
    /// publisher's epoch, which the caller hands back to [`Self::is_active_epoch`] /
    /// [`Self::clear_publisher_if`] to recognize its own packets/disconnect later
    /// without racing a newer publisher.
    pub async fn begin_publisher(&self, sdp: String) -> u64 {
        let epoch = self.epoch_counter.fetch_add(1, Ordering::SeqCst) + 1;
        self.active_epoch.store(epoch, Ordering::SeqCst);
        let mut inner = self.inner.lock().await;
        inner.sdp = Some(sdp);
        inner.publisher_present = true;
        drop(inner);
        self.sdp_ready.notify_waiters();
        epoch
    }

    /// True if `epoch` is still the stream's current publisher.
    pub fn is_active_epoch(&self, epoch: u64) -> bool {
        self.active_epoch.load(Ordering::SeqCst) == epoch
    }

    /// The stream's current publisher epoch, or 0 if none.
    pub fn active_epoch(&self) -> u64 {
        self.active_epoch.load(Ordering::SeqCst)
    }

    /// Publisher gone: clear SDP so the next subscriber triggers a fresh ingest — but
    /// only if `epoch` is still the active one. A disconnect from a publisher that has
    /// already been superseded (e.g. a placeholder stopped after the real feed took
    /// over) must not clobber the newer publisher's state.
    pub async fn clear_publisher_if(&self, epoch: u64) {
        if !self.is_active_epoch(epoch) {
            return;
        }
        self.active_epoch.store(0, Ordering::SeqCst);
        let mut inner = self.inner.lock().await;
        inner.publisher_present = false;
        inner.sdp = None;
    }

    /// Unconditionally clear the publisher state, regardless of epoch — used when we
    /// ourselves tore down the tracked ingest (e.g. the no-subscribers linger).
    pub async fn force_clear_publisher(&self) {
        self.active_epoch.store(0, Ordering::SeqCst);
        let mut inner = self.inner.lock().await;
        inner.publisher_present = false;
        inner.sdp = None;
    }

    pub async fn publisher_present(&self) -> bool {
        self.inner.lock().await.publisher_present
    }

    pub async fn sdp(&self) -> Option<String> {
        self.inner.lock().await.sdp.clone()
    }

    /// Wait until the publisher has announced its SDP, or time out.
    pub async fn wait_for_sdp(&self, timeout: Duration) -> Option<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            // Register for notification BEFORE checking, so a set_sdp that races the
            // check cannot be lost.
            let notified = self.sdp_ready.notified();
            if let Some(sdp) = self.sdp().await {
                return Some(sdp);
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            if tokio::time::timeout(remaining, notified).await.is_err() {
                return self.sdp().await;
            }
        }
    }

    /// Wait until a publisher *newer* than `after_epoch` has announced (i.e. the
    /// active epoch advances past it), or time out. Unlike [`Self::wait_for_sdp`],
    /// this isn't fooled by an SDP that's already set from an older publisher (e.g. a
    /// "connecting..." placeholder) — used to detect the real upstream specifically
    /// taking over.
    pub async fn wait_for_new_publisher(&self, after_epoch: u64, timeout: Duration) -> Option<String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.sdp_ready.notified();
            if self.active_epoch() > after_epoch {
                return self.sdp().await;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            if tokio::time::timeout(remaining, notified).await.is_err() {
                return if self.active_epoch() > after_epoch {
                    self.sdp().await
                } else {
                    None
                };
            }
        }
    }

    /// Publisher: broadcast a relayed packet to all subscribers.
    pub fn publish(&self, packet: Packet) {
        // Err only means there are currently no subscribers; that is fine.
        let _ = self.tx.send(Arc::new(packet));
    }

    /// Subscriber: obtain a receiver for relayed packets.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Packet>> {
        self.tx.subscribe()
    }

    pub fn add_subscriber(&self) -> usize {
        self.subscribers.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn remove_subscriber(&self) -> usize {
        self.subscribers.fetch_sub(1, Ordering::SeqCst).saturating_sub(1)
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscribers.load(Ordering::SeqCst)
    }
}

/// Registry of live streams keyed by path.
#[derive(Default)]
pub struct StreamRegistry {
    streams: Mutex<HashMap<String, Arc<Stream>>>,
}

impl StreamRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn get_or_create(&self, path: &str) -> Arc<Stream> {
        let mut map = self.streams.lock().await;
        map.entry(path.to_string())
            .or_insert_with(|| Arc::new(Stream::new(path.to_string())))
            .clone()
    }

    pub async fn get(&self, path: &str) -> Option<Arc<Stream>> {
        self.streams.lock().await.get(path).cloned()
    }
}
