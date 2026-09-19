//! Bosch cloud backend client: lists cameras and fetches short-lived per-camera
//! credentials, authenticating every call with a Bearer token from the
//! [`AuthManager`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tracing::debug;

use crate::auth::AuthManager;
use crate::camera::{Camera, CameraCredentials, Route};
use crate::config::Config;
use crate::error::{ProxyError, Result};

pub struct BackendClient {
    cfg: Arc<Config>,
    auth: Arc<AuthManager>,
    http: Client,
    /// Cached per-(camera, route, quality) credentials from `PUT .../connection`,
    /// reused across ingest attempts/sessions until the camera rejects them — the
    /// endpoint issues a *new* credential (and implicitly rotates out the old one)
    /// on every call, so re-fetching on every single connect is both an unnecessary
    /// backend round-trip and needless credential churn.
    credentials_cache: Mutex<HashMap<(String, Route, bool), CameraCredentials>>,
}

impl BackendClient {
    pub fn new(cfg: Arc<Config>, auth: Arc<AuthManager>) -> Result<Self> {
        let mut builder = Client::builder().timeout(Duration::from_secs(30));
        if cfg.tls_insecure {
            builder = builder.danger_accept_invalid_certs(true);
        } else {
            // residential.cbs.boschsecurity.com (and friends) chain to a private
            // Bosch PKI root that isn't in the system trust store; trust it directly.
            let pem = cfg.ca_pem()?;
            builder = builder.add_root_certificate(reqwest::Certificate::from_pem(&pem)?);
        }
        let http = builder.build()?;
        Ok(Self {
            cfg,
            auth,
            http,
            credentials_cache: Mutex::new(HashMap::new()),
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.cfg.environment.backend_base, path.trim_start_matches('/'))
    }

    /// GET with a Bearer token, retrying once after a forced refresh on 401.
    async fn get_json(&self, path: &str, query: &[(&str, &str)]) -> Result<Value> {
        let url = self.url(path);
        for attempt in 0..2 {
            let token = self.auth.valid_access_token().await?;
            let resp = self
                .http
                .get(url.as_str())
                .bearer_auth(&token)
                .query(query)
                .send()
                .await?;

            let status = resp.status();
            if status.is_success() {
                return Ok(resp.json().await?);
            }
            if status == StatusCode::UNAUTHORIZED && attempt == 0 {
                debug!("backend returned 401 for {path}; forcing token refresh and retrying");
                self.auth.force_refresh().await?;
                continue;
            }
            let body = resp.text().await.unwrap_or_default();
            return Err(ProxyError::Backend(format!("GET {path} -> {status}: {body}")));
        }
        unreachable!("retry loop always returns")
    }

    /// PUT a JSON body with a Bearer token, retrying once after a forced refresh on 401.
    async fn put_json(&self, path: &str, body: &Value) -> Result<Value> {
        let url = self.url(path);
        for attempt in 0..2 {
            let token = self.auth.valid_access_token().await?;
            let resp = self
                .http
                .put(url.as_str())
                .bearer_auth(&token)
                .json(body)
                .send()
                .await?;

            let status = resp.status();
            if status.is_success() {
                return Ok(resp.json().await?);
            }
            if status == StatusCode::UNAUTHORIZED && attempt == 0 {
                debug!("backend returned 401 for {path}; forcing token refresh and retrying");
                self.auth.force_refresh().await?;
                continue;
            }
            let text = resp.text().await.unwrap_or_default();
            return Err(ProxyError::Backend(format!("PUT {path} -> {status}: {text}")));
        }
        unreachable!("retry loop always returns")
    }

    /// List the user's cameras (`GET /video_inputs`).
    pub async fn list_video_inputs(&self) -> Result<Vec<Camera>> {
        let value = self.get_json("video_inputs", &[]).await?;
        let array = extract_array(&value).ok_or_else(|| {
            ProxyError::Backend("unexpected /video_inputs response shape".to_string())
        })?;
        let cameras: Vec<Camera> = serde_json::from_value(Value::Array(array.clone()))?;
        Ok(cameras)
    }

    /// Fetch credentials for one camera/route/quality, from cache if we already
    /// have a live one (`PUT /video_inputs/{id}/connection`, body `{"type",
    /// "highQualityVideo"}`).
    ///
    /// Reverse-engineered from the live backend: a `GET .../credentials` endpoint
    /// also exists but returns an unrelated `{"userToken": ...}` shape — the real
    /// endpoint is a `PUT` to `.../connection` with a JSON body, and the connection
    /// `type` enum is upper-cased (`LOCAL`/`REMOTE`), confirmed via the server's own
    /// 400 response when given the wrong casing. `highQualityVideo` is honored
    /// independently of `type` (confirmed live: `REMOTE` + `true` returns the full
    /// main stream, not clamped down), so we let the caller choose both separately
    /// rather than pairing them (`LOCAL`+`true`, `REMOTE`+`false`).
    ///
    /// The response has no expiry field to key an active cache invalidation off of,
    /// so this caches optimistically and relies on the caller telling us when a
    /// cached credential turned out to be rejected (see [`Self::invalidate_credentials`]).
    pub async fn get_credentials(
        &self,
        camera_id: &str,
        route: Route,
        high_quality: bool,
    ) -> Result<CameraCredentials> {
        let key = (camera_id.to_string(), route, high_quality);
        if let Some(cached) = self.credentials_cache.lock().await.get(&key) {
            return Ok(cached.clone());
        }

        let path = format!("video_inputs/{camera_id}/connection");
        let body = json!({
            "type": route.connection_type(),
            "highQualityVideo": high_quality,
        });
        let value = self.put_json(&path, &body).await?;
        let creds: CameraCredentials = serde_json::from_value(value)?;
        self.credentials_cache.lock().await.insert(key, creds.clone());
        Ok(creds)
    }

    /// Drop the cached credentials for `(camera_id, route, high_quality)`, if any,
    /// so the next [`Self::get_credentials`] call fetches a fresh one. Call this
    /// when a cached credential turns out to have been rejected by the camera.
    pub async fn invalidate_credentials(&self, camera_id: &str, route: Route, high_quality: bool) {
        self.credentials_cache
            .lock()
            .await
            .remove(&(camera_id.to_string(), route, high_quality));
    }

    /// Validate the session (`GET /registration/check`); maps a server-forced logout to
    /// [`ProxyError::SessionInvalidated`]. Available for a periodic session
    /// health-check; not yet wired into the run loop.
    #[allow(dead_code)]
    pub async fn registration_check(&self) -> Result<()> {
        let value = self.get_json("registration/check", &[]).await?;
        if let Some(problems) = value.get("loginProblems").or_else(|| value.get("LoginProblems")) {
            let text = problems.to_string();
            if text.contains("OAUTH_TOKEN_INVALIDATED") {
                return Err(ProxyError::SessionInvalidated);
            }
        }
        Ok(())
    }
}

/// Accept either a bare array or an object wrapping the array under a common key.
fn extract_array(value: &Value) -> Option<Vec<Value>> {
    if let Some(arr) = value.as_array() {
        return Some(arr.clone());
    }
    for key in ["video_inputs", "videoInputs", "items", "data"] {
        if let Some(arr) = value.get(key).and_then(Value::as_array) {
            return Some(arr.clone());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_bare_array() {
        let v = json!([{"id": "a"}, {"id": "b"}]);
        assert_eq!(extract_array(&v).unwrap().len(), 2);
    }

    #[test]
    fn extracts_wrapped_array() {
        let v = json!({"video_inputs": [{"id": "a"}]});
        assert_eq!(extract_array(&v).unwrap().len(), 1);
    }

    #[test]
    fn parses_camera_ignoring_extra_fields() {
        let v = json!([{"id": "cam1", "name": "Front", "extra": 42}]);
        let arr = extract_array(&v).unwrap();
        let cams: Vec<Camera> = serde_json::from_value(Value::Array(arr)).unwrap();
        assert_eq!(cams[0].id, "cam1");
        assert_eq!(cams[0].name.as_deref(), Some("Front"));
    }
}
