//! Local TLS-terminating relay for the camera's RTSP-over-HTTPS tunnel.
//!
//! ffmpeg's GnuTLS backend refuses the camera's leaf certificate outright: it has no
//! Subject Alternative Name, only `CN=<mac-address>`. Neither `-verifyhost` nor
//! `-tls_verify 0` gets ffmpeg's `-rtsp_transport https` past it (GnuTLS, unlike
//! OpenSSL/SChannel, doesn't fall back to CN matching, and `-tls_verify` isn't even
//! forwarded into that transport's nested TLS context) — see
//! `credentials-and-ffmpeg-findings` project notes. `native-tls` (Windows SChannel)
//! handles this cert fine once hostname checking is disabled while chain-of-trust is
//! still validated against our pinned root CA, so this relay does that handshake
//! itself and hands ffmpeg a plain loopback TCP socket (`-rtsp_transport http`)
//! instead of talking TLS to the camera directly.

use std::net::SocketAddr;

use native_tls::{Certificate, TlsConnector as NativeTlsConnector};
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio_native_tls::TlsConnector;
use tracing::{debug, warn};

use crate::error::{ProxyError, Result};

/// Accepts plaintext TCP on a loopback port and forwards each connection, TLS-wrapped,
/// to the real upstream host. Dropping it stops accepting new connections.
pub struct TlsRelay {
    local_addr: SocketAddr,
    accept_task: tokio::task::JoinHandle<()>,
}

impl TlsRelay {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Start relaying to `(host, port)`. `ca_pem` is the pinned Bosch root CA PEM
    /// bytes to validate the chain against (see [`crate::config::Config::ca_pem`]);
    /// `None` also waives chain validation (mirrors `cfg.tls_insecure`, which
    /// previously mapped to ffmpeg's `-tls_verify 0`).
    pub async fn spawn(host: String, port: u16, ca_pem: Option<&[u8]>) -> Result<Self> {
        let mut builder = NativeTlsConnector::builder();
        // The camera cert has no SAN and we connect by IP, so hostname matching can
        // never succeed; chain-of-trust is still enforced below unless waived.
        builder.danger_accept_invalid_hostnames(true);
        match ca_pem {
            Some(pem) => {
                let cert = Certificate::from_pem(pem)
                    .map_err(|e| ProxyError::Config(format!("parsing CA bundle: {e}")))?;
                builder.add_root_certificate(cert);
            }
            None => {
                builder.danger_accept_invalid_certs(true);
            }
        }
        let connector: TlsConnector = builder
            .build()
            .map_err(|e| ProxyError::Config(format!("building TLS connector: {e}")))?
            .into();

        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let local_addr = listener.local_addr()?;

        let accept_task = tokio::spawn(async move {
            loop {
                let (inbound, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(e) => {
                        warn!("tls relay accept failed: {e}");
                        break;
                    }
                };
                let connector = connector.clone();
                let host = host.clone();
                tokio::spawn(async move {
                    if let Err(e) = relay_one(inbound, &host, port, &connector).await {
                        warn!(target = %host, "tls relay connection failed: {e}");
                    }
                });
            }
        });

        Ok(Self {
            local_addr,
            accept_task,
        })
    }
}

impl Drop for TlsRelay {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

async fn relay_one(
    mut inbound: TcpStream,
    host: &str,
    port: u16,
    connector: &TlsConnector,
) -> Result<()> {
    let outbound = TcpStream::connect((host, port)).await?;
    let mut tls = connector
        .connect(host, outbound)
        .await
        .map_err(|e| ProxyError::Ingest(format!("tls relay handshake to {host}:{port}: {e}")))?;

    debug!(target = %host, "tls relay: connection established");
    io::copy_bidirectional(&mut inbound, &mut tls).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn binds_a_loopback_port() {
        // Target host is never dialed until a client connects, so this doesn't need
        // a live camera to verify the listener side of the relay.
        let relay = TlsRelay::spawn("203.0.113.1".to_string(), 443, None)
            .await
            .unwrap();
        assert!(relay.local_addr().ip().is_loopback());
        assert_ne!(relay.local_addr().port(), 0);
    }
}
