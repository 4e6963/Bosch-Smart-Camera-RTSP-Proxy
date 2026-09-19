use std::net::SocketAddr;
use std::path::PathBuf;

use crate::camera::Route;
use crate::error::{ProxyError, Result};

/// Bosch's private-PKI root CA (`Bosch ST Root CA`), embedded at compile time so
/// the binary trusts the camera stream and REST API out of the box with no
/// external file needed. This is a public root certificate, not a secret.
/// Override with `BOSCH_CA_BUNDLE` (a path to a different PEM file) if Bosch ever
/// rotates it.
const DEFAULT_CA_PEM: &str = include_str!("../bosch-ca.pem");

/// A Bosch backend/identity environment preset.
#[derive(Debug, Clone)]
pub struct Environment {
    /// Keycloak host, e.g. `smarthome.authz.bosch.com`.
    pub authz_host: String,
    /// Keycloak realm, e.g. `home_auth_provider`.
    pub realm: String,
    /// Default OAuth client id for this environment.
    pub default_client_id: &'static str,
    /// Backend REST base, without trailing slash, e.g.
    /// `https://residential.cbs.boschsecurity.com/v11`.
    pub backend_base: String,
}

impl Environment {
    pub fn preset(name: &str) -> Result<Self> {
        let (authz_host, realm, client, backend_host) = match name.to_ascii_lowercase().as_str() {
            "prod" => (
                "smarthome.authz.bosch.com",
                "home_auth_provider",
                "residential_app",
                "residential.cbs.boschsecurity.com",
            ),
            "test" => (
                "smarthome.authz.bosch.com",
                "home_auth_provider",
                "resitest_app",
                "resitest.cbs.boschsecurity.com",
            ),
            "dev" => (
                "p14.authz.bosch.com",
                "home_auth_server",
                "residev_app",
                "residev.cbs.boschsecurity.com",
            ),
            "sand" => (
                "p14.authz.bosch.com",
                "home_auth_server",
                "resisand_app",
                "resisand.cbs.boschsecurity.com",
            ),
            "pen" => (
                "p14.authz.bosch.com",
                "home_auth_server",
                "resipen_app",
                "resipen.cbs.boschsecurity.com",
            ),
            other => {
                return Err(ProxyError::Config(format!(
                    "unknown environment preset '{other}' (expected: prod|test|dev|sand|pen)"
                )))
            }
        };

        Ok(Self {
            authz_host: authz_host.to_string(),
            realm: realm.to_string(),
            default_client_id: client,
            backend_base: format!("https://{backend_host}/v11"),
        })
    }

    /// The Keycloak base URL for this realm's OpenID Connect endpoints.
    pub fn oidc_base(&self) -> String {
        format!(
            "https://{}/auth/realms/{}/protocol/openid-connect",
            self.authz_host, self.realm
        )
    }

    pub fn token_url(&self) -> String {
        format!("{}/token", self.oidc_base())
    }

    /// The authorize URL to open in a real browser to obtain a refresh token
    /// out-of-band (see `AuthManager::login_instructions`).
    pub fn authorize_url(&self) -> String {
        format!("{}/auth", self.oidc_base())
    }
}

/// Fully resolved configuration for a run.
#[derive(Debug, Clone)]
pub struct Config {
    pub client_id: String,
    pub client_secret: String,
    pub environment: Environment,

    /// Path to a PEM file overriding the embedded [`DEFAULT_CA_PEM`], if
    /// `BOSCH_CA_BUNDLE` was set. `None` means use the embedded default -- see
    /// [`Self::ca_pem`].
    pub ca_bundle_override: Option<PathBuf>,
    pub tls_insecure: bool,
    pub token_store: PathBuf,

    pub rtsp_bind: SocketAddr,
    pub default_route: Route,
    /// Relay the camera's audio track in addition to video.
    pub include_audio: bool,
    /// Publish a synthetic "connecting to camera..." placeholder while the real
    /// upstream ingest is starting, so subscribers get instant playback instead of a
    /// blocked DESCRIBE. Disable via `RTSP_CONNECTING_PLACEHOLDER=0`.
    pub connecting_placeholder: bool,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let environment = Environment::preset("prod")?;

        let client_id = std::env::var("BOSCH_CLIENT_ID")
            .unwrap_or_else(|_| environment.default_client_id.to_string());

        let rtsp_bind = env_or("RTSP_BIND", "0.0.0.0:8554")
            .parse::<SocketAddr>()
            .map_err(|e| ProxyError::Config(format!("invalid RTSP_BIND: {e}")))?;

        let default_route = Route::parse(&env_or("RTSP_DEFAULT_ROUTE", "local"))?;

        Ok(Self {
            client_id,
            client_secret: env_or("BOSCH_CLIENT_SECRET", "yUmjfFutWfKbYOOficWFrcFeD14oFW0C"),
            environment,
            ca_bundle_override: std::env::var("BOSCH_CA_BUNDLE")
                .ok()
                .filter(|v| !v.is_empty())
                .map(PathBuf::from),
            tls_insecure: env_flag("BOSCH_TLS_INSECURE"),
            token_store: PathBuf::from(env_or("TOKEN_STORE", "./tokens.json")),
            rtsp_bind,
            default_route,
            include_audio: !env_flag_off("RTSP_AUDIO"),
            connecting_placeholder: !env_flag_off("RTSP_CONNECTING_PLACEHOLDER"),
        })
    }

    /// The Bosch private-PKI root CA PEM bytes to trust: from `BOSCH_CA_BUNDLE` if
    /// set, otherwise the copy embedded in the binary at compile time.
    pub fn ca_pem(&self) -> Result<Vec<u8>> {
        match &self.ca_bundle_override {
            Some(path) => std::fs::read(path).map_err(|e| {
                ProxyError::Config(format!("reading BOSCH_CA_BUNDLE {}: {e}", path.display()))
            }),
            None => Ok(DEFAULT_CA_PEM.as_bytes().to_vec()),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_flag(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes")
    )
}

/// True if the env var is explicitly set to a falsy value (used for flags that
/// default to on).
fn env_flag_off(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("0") | Some("false") | Some("FALSE") | Some("no")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_config(ca_bundle_override: Option<PathBuf>) -> Config {
        Config {
            client_id: "id".into(),
            client_secret: "secret".into(),
            environment: Environment::preset("prod").unwrap(),
            ca_bundle_override,
            tls_insecure: false,
            token_store: PathBuf::from("./tokens.json"),
            rtsp_bind: "0.0.0.0:8554".parse().unwrap(),
            default_route: Route::Local,
            include_audio: true,
            connecting_placeholder: true,
        }
    }

    #[test]
    fn ca_pem_defaults_to_embedded_bundle() {
        let cfg = dummy_config(None);
        assert_eq!(cfg.ca_pem().unwrap(), DEFAULT_CA_PEM.as_bytes());
    }

    #[test]
    fn ca_pem_reads_override_file_when_set() {
        let path = std::env::temp_dir().join("bosch-cam-proxy-test-ca.pem");
        std::fs::write(&path, "custom pem content").unwrap();
        let cfg = dummy_config(Some(path.clone()));
        assert_eq!(cfg.ca_pem().unwrap(), b"custom pem content");
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn ca_pem_errors_clearly_when_override_missing() {
        let cfg = dummy_config(Some(PathBuf::from(
            "./definitely-does-not-exist-bosch-ca.pem",
        )));
        assert!(matches!(cfg.ca_pem(), Err(ProxyError::Config(_))));
    }
}
