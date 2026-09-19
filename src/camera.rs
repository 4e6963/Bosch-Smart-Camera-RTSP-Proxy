//! Camera domain models and upstream video-URL construction: temporary
//! Basic-Auth credentials plus URL schemes, with the Local route forced to
//! port 443 for the RTSP-over-HTTPS tunnel.

use serde::Deserialize;
use url::Url;

use crate::error::{ProxyError, Result};

/// Connection route to a camera. `Local` is a direct LAN connection (port 443);
/// `Relay` goes through Bosch's cloud relay. Independent of stream quality (the
/// `highQualityVideo` flag on the credentials request, see
/// [`crate::backend::BackendClient::get_credentials`]) -- confirmed live that the
/// backend honors any route/quality combination, it isn't clamped by route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Route {
    Local,
    Relay,
}

impl Route {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "local" | "lan" => Ok(Route::Local),
            "relay" | "cloud" => Ok(Route::Relay),
            other => Err(ProxyError::Config(format!(
                "invalid route '{other}' (expected: local|relay)"
            ))),
        }
    }

    /// Value used as the `route` query parameter on the credentials endpoint.
    pub fn as_query(self) -> &'static str {
        match self {
            Route::Local => "local",
            Route::Relay => "relay",
        }
    }

    /// The `type` value in the `PUT /video_inputs/{id}/connection` request body.
    /// Confirmed against the live backend: it's an upper-cased Java enum name,
    /// not the lower-case `local`/`relay` used in our own config/URLs.
    pub fn connection_type(self) -> &'static str {
        match self {
            Route::Local => "LOCAL",
            Route::Relay => "REMOTE",
        }
    }

}

/// A camera as returned by `GET /video_inputs`.
///
/// The backend payload carries more fields than we need; unknown ones are ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct Camera {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    // Reported by the backend; used to tune player config once ingest is wired.
    #[allow(dead_code)]
    #[serde(default, rename = "hwVersion", alias = "hw_version")]
    pub hw_version: Option<String>,
}

impl Camera {
    /// Human-friendly label for logs.
    pub fn label(&self) -> String {
        match &self.name {
            Some(n) if !n.is_empty() => format!("{} ({})", n, self.id),
            _ => self.id.clone(),
        }
    }
}

/// Short-lived, cloud-issued credentials for one camera/route, from
/// `PUT /video_inputs/{id}/connection`.
///
/// `user`/`password` are only present for the `Local` route: `Relay` connections
/// embed a one-time token in the URL path instead and return both as `null`.
/// `video_url_scheme` (and the other `*UrlScheme` fields) are *templates*
/// containing a literal `{url}` placeholder, substituted with an entry from
/// `urls` (a bare `host:port`, e.g. `10.1.0.33:443` or a relay
/// `host:port/token`) to get the final URL.
#[derive(Debug, Clone, Deserialize)]
pub struct CameraCredentials {
    #[serde(default, alias = "User")]
    pub user: Option<String>,
    #[serde(default, alias = "Password")]
    pub password: Option<String>,
    #[serde(default, alias = "Urls")]
    pub urls: Vec<String>,
    #[serde(default, rename = "videoUrlScheme", alias = "VideoUrlScheme")]
    pub video_url_scheme: Option<String>,
    // Snapshot / control-plane URL templates; kept for when those features are wired.
    #[allow(dead_code)]
    #[serde(default, rename = "imageUrlScheme", alias = "ImageUrlScheme")]
    pub image_url_scheme: Option<String>,
    #[allow(dead_code)]
    #[serde(default, rename = "httpsUrlScheme", alias = "baseUrlScheme")]
    pub base_url_scheme: Option<String>,
}

impl CameraCredentials {
    /// Build the upstream video URL by substituting `{url}` in `video_url_scheme`
    /// with the first entry of `urls`, as `CameraUrlBuilderService` does.
    ///
    /// Returns the URL *without* embedded credentials plus the userinfo separately,
    /// so the caller can decide whether to inline them or pass an `Authorization`
    /// header (both forms are tried in the ffmpeg spike, milestone 0).
    pub fn video_target(&self, _route: Route) -> Result<VideoTarget> {
        let host = self.urls.first().ok_or_else(|| {
            ProxyError::Backend("credentials response has no connection url".to_string())
        })?;
        let scheme = self.video_url_scheme.as_deref().ok_or_else(|| {
            ProxyError::Backend("credentials response has no video URL scheme".to_string())
        })?;
        let resolved = scheme.replace("{url}", host);

        let url = Url::parse(&resolved)
            .map_err(|e| ProxyError::Backend(format!("invalid video URL '{resolved}': {e}")))?;

        Ok(VideoTarget {
            url,
            user: self.user.clone(),
            password: self.password.clone(),
        })
    }
}

/// A resolved upstream video endpoint plus its optional Basic-Auth userinfo
/// (`None` for the Relay route, whose auth is embedded in the URL path).
#[derive(Debug, Clone)]
pub struct VideoTarget {
    pub url: Url,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl VideoTarget {
    /// The URL with `user:password@` inline userinfo — the form handed to the
    /// native player (`ConnectionUrl`) — or the bare URL if there are no
    /// credentials to embed.
    pub fn url_with_userinfo(&self) -> Result<Url> {
        let mut u = self.url.clone();
        let (Some(user), Some(password)) = (&self.user, &self.password) else {
            return Ok(u);
        };
        u.set_username(user)
            .map_err(|_| ProxyError::Backend("cannot set username on video URL".into()))?;
        u.set_password(Some(password))
            .map_err(|_| ProxyError::Backend("cannot set password on video URL".into()))?;
        Ok(u)
    }

    /// `Basic base64(user:password)` header value, as an alternative to inline userinfo.
    /// Retained for the milestone-0 spike (header-based auth ffmpeg variant).
    #[allow(dead_code)]
    pub fn basic_auth_header(&self) -> Option<String> {
        use base64::Engine as _;
        let raw = format!("{}:{}", self.user.as_deref()?, self.password.as_deref()?);
        Some(format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(raw)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_url_placeholder_from_urls_list() {
        let creds = CameraCredentials {
            user: Some("u".into()),
            password: Some("p".into()),
            urls: vec!["10.1.0.33:443".into()],
            video_url_scheme: Some("rtsp://{url}/rtsp_tunnel?inst=1".into()),
            image_url_scheme: None,
            base_url_scheme: None,
        };
        let target = creds.video_target(Route::Local).unwrap();
        assert_eq!(target.url.as_str(), "rtsp://10.1.0.33:443/rtsp_tunnel?inst=1");
    }

    #[test]
    fn relay_route_has_no_credentials() {
        let creds = CameraCredentials {
            user: None,
            password: None,
            urls: vec!["proxy-1.live.cbs.boschsecurity.com:42090/tok".into()],
            video_url_scheme: Some("rtsp://{url}/rtsp_tunnel?inst=2".into()),
            image_url_scheme: None,
            base_url_scheme: None,
        };
        let target = creds.video_target(Route::Relay).unwrap();
        let with = target.url_with_userinfo().unwrap();
        assert_eq!(with.username(), "");
        assert_eq!(with.password(), None);
        assert!(target.basic_auth_header().is_none());
    }

    #[test]
    fn userinfo_and_basic_header_match_credentials() {
        let creds = CameraCredentials {
            user: Some("alice".into()),
            password: Some("s3cret".into()),
            urls: vec!["cam.local:443".into()],
            video_url_scheme: Some("https://{url}/stream1".into()),
            image_url_scheme: None,
            base_url_scheme: None,
        };
        let target = creds.video_target(Route::Local).unwrap();
        let with = target.url_with_userinfo().unwrap();
        assert_eq!(with.username(), "alice");
        assert_eq!(with.password(), Some("s3cret"));
        // base64("alice:s3cret")
        assert_eq!(target.basic_auth_header().as_deref(), Some("Basic YWxpY2U6czNjcmV0"));
    }
}
