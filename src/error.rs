//! Crate-wide error type.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("authentication failed: {0}")]
    Auth(String),

    #[error("the stored/refresh session is no longer valid (server-forced logout or expired refresh token)")]
    SessionInvalidated,

    #[error("backend request failed: {0}")]
    Backend(String),

    #[allow(dead_code)] // returned once camera-id lookup/validation is wired
    #[error("camera {0} not found")]
    CameraNotFound(String),

    #[error("upstream ffmpeg error: {0}")]
    Ingest(String),

    #[error("rtsp protocol error: {0}")]
    Rtsp(String),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, ProxyError>;
