//! OAuth2 token model, token-endpoint calls, and the [`AuthManager`] that keeps a
//! valid access token available at all times.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::redirect::Policy;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use url::Url;

use crate::config::Config;
use crate::error::{ProxyError, Result};

/// Refresh this many seconds before the access token's `exp` to avoid racing expiry.
const EXPIRY_SKEW: u64 = 60;

/// `redirect_uri` a real browser's authorization-code flow would hit. No real
/// resource is ever fetched there. Only used to build the login URL printed by
/// [`login_instructions`] — no code here ever follows it.
const REDIRECT_URI: &str = "https://www.bosch.com/boschcam";
/// OAuth scopes requested at the authorize endpoint.
const SCOPE: &str = "email offline_access profile openid";

/// Build the authorize URL and step-by-step instructions for obtaining a session
/// out-of-band. There is no scripted login in this codebase — the identity
/// provider's login form is gated behind a bot-protection challenge — so a human
/// logs in through a real browser once, and we tell them exactly where to look
/// for the code. Used by the `login` subcommand itself; other commands just point
/// the user at `login` rather than repeating this (see [`AuthManager::bootstrap`]'s
/// deliberately short error messages).
pub(crate) fn login_instructions(cfg: &Config) -> String {
    let url = Url::parse_with_params(
        &cfg.environment.authorize_url(),
        &[
            ("response_type", "code"),
            ("client_id", cfg.client_id.as_str()),
            ("redirect_uri", REDIRECT_URI),
            ("scope", SCOPE),
        ],
    )
    .map(|u| u.to_string())
    .unwrap_or_else(|_| cfg.environment.authorize_url());

    format!(
        "\nTo obtain a session:\n\n\
         1. Open this URL in a real browser and log in with your Bosch account:\n\n    {url}\n\n\
         2. It ends in a redirect to '{REDIRECT_URI}?code=...&state=...' that fails to load — \
         that's expected (no real page there). Copy the `code` value out of that URL's query \
         string right away: it's a single-use authorization code, typically valid for well \
         under a minute.\n\
         3. Paste it at the prompt below.\n"
    )
}

/// The three tokens tracked for a session (no `expires_in`/`token_type`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuth2Token {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub id_token: Option<String>,
}

/// Build the token-endpoint client (follows no redirects; the token endpoint is a
/// plain JSON POST).
fn token_client() -> Result<Client> {
    Ok(Client::builder()
        .redirect(Policy::none())
        .timeout(Duration::from_secs(30))
        .build()?)
}

/// Renew tokens with a refresh token (`grant_type=refresh_token`).
pub async fn refresh(cfg: &Config, refresh_token: &str) -> Result<OAuth2Token> {
    let params = [
        ("grant_type", "refresh_token"),
        ("client_id", cfg.client_id.as_str()),
        ("client_secret", cfg.client_secret.as_str()),
        ("refresh_token", refresh_token),
    ];
    post_token(cfg, &params).await
}

/// Exchange a single-use authorization `code` for tokens (`grant_type=authorization_code`).
/// The code comes from a real, human-completed login (see [`login_instructions`]) —
/// this is just the final leg of the standard OAuth2 flow, not a scripted login.
async fn exchange_code(cfg: &Config, code: &str) -> Result<OAuth2Token> {
    let params = [
        ("grant_type", "authorization_code"),
        ("client_id", cfg.client_id.as_str()),
        ("client_secret", cfg.client_secret.as_str()),
        ("code", code),
        ("redirect_uri", REDIRECT_URI),
    ];
    post_token(cfg, &params).await
}

async fn post_token(cfg: &Config, params: &[(&str, &str)]) -> Result<OAuth2Token> {
    let client = token_client()?;
    let resp = client
        .post(cfg.environment.token_url())
        .form(params)
        .send()
        .await?;

    let status = resp.status();
    if status.is_success() {
        let token: OAuth2Token = resp.json().await?;
        return Ok(token);
    }

    // A 400 on a refresh means the refresh token is no longer usable -> the whole
    // session must be re-bootstrapped.
    let body = resp.text().await.unwrap_or_default();
    match status {
        StatusCode::BAD_REQUEST => Err(ProxyError::SessionInvalidated),
        StatusCode::INTERNAL_SERVER_ERROR => {
            Err(ProxyError::Auth(format!("token endpoint 500: {body}")))
        }
        other => Err(ProxyError::Auth(format!("token endpoint {other}: {body}"))),
    }
}

/// Read the unverified `exp` claim (seconds since epoch) from a JWT access token.
///
/// We do not verify the signature — we only need the expiry to schedule refreshes.
pub fn jwt_exp(token: &str) -> Option<u64> {
    use base64::Engine as _;
    let payload_b64 = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .ok()?;
    #[derive(Deserialize)]
    struct Claims {
        exp: Option<u64>,
    }
    let claims: Claims = serde_json::from_slice(&bytes).ok()?;
    claims.exp
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

struct TokenState {
    token: OAuth2Token,
    /// Unverified `exp` of the current access token, if decodable.
    access_exp: Option<u64>,
}

/// Keeps a valid access token available, refreshing proactively. If the refresh
/// token is rejected, the session must be re-obtained (`login`).
pub struct AuthManager {
    cfg: Arc<Config>,
    state: Mutex<TokenState>,
}

impl AuthManager {
    /// Load a persisted refresh token and refresh it into a live session.
    ///
    /// There is no scripted login here — the identity provider gates its login form
    /// behind a bot-protection challenge, so a refresh token must be obtained once
    /// out-of-band (a real browser) and seeded via `bosch-cam-proxy login`. The
    /// error messages here are deliberately short (no embedded instructions) since
    /// most callers just need to tell the user to run `login` and exit; `login`
    /// itself prints the full instructions.
    pub async fn bootstrap(cfg: Arc<Config>) -> Result<Arc<Self>> {
        let stored = load_store(&cfg.token_store).and_then(|s| s.refresh_token.clone());
        let Some(rt) = stored else {
            return Err(ProxyError::Config(format!(
                "no session found (no refresh token in {})",
                cfg.token_store.display()
            )));
        };

        let token = refresh(&cfg, &rt).await.map_err(|e| match e {
            ProxyError::SessionInvalidated => ProxyError::Config(format!(
                "the refresh token in {} was rejected (revoked/expired)",
                cfg.token_store.display()
            )),
            other => other,
        })?;
        info!("resumed session from persisted refresh token");

        let manager = Arc::new(Self {
            state: Mutex::new(TokenState {
                access_exp: jwt_exp(&token.access_token),
                token,
            }),
            cfg,
        });
        manager.persist().await;
        Ok(manager)
    }

    /// Exchange a freshly obtained authorization code (from a real, human login —
    /// see [`login_instructions`]) for a session, then persist it to `TOKEN_STORE`.
    /// Used by the interactive `login` subcommand once no existing session is found.
    pub async fn bootstrap_from_code(cfg: Arc<Config>, code: &str) -> Result<Arc<Self>> {
        let token = exchange_code(&cfg, code).await.map_err(|e| match e {
            ProxyError::SessionInvalidated => ProxyError::Config(
                "that authorization code was rejected (expired, already used, or the redirect_uri \
                 didn't match) — codes are single-use and short-lived, get a fresh one with \
                 `bosch-cam-proxy login`"
                    .into(),
            ),
            other => other,
        })?;
        Self::finish_bootstrap(cfg, token).await
    }

    async fn finish_bootstrap(cfg: Arc<Config>, token: OAuth2Token) -> Result<Arc<Self>> {
        let manager = Arc::new(Self {
            state: Mutex::new(TokenState {
                access_exp: jwt_exp(&token.access_token),
                token,
            }),
            cfg,
        });
        manager.persist().await;
        Ok(manager)
    }

    /// Return a currently-valid access token, refreshing (or re-logging in) if needed.
    /// Refreshes are serialized by the state mutex to prevent concurrent
    /// single-use refresh-token races.
    pub async fn valid_access_token(&self) -> Result<String> {
        let mut state = self.state.lock().await;

        let fresh = match state.access_exp {
            Some(exp) => now_secs() + EXPIRY_SKEW < exp,
            None => false, // could not decode expiry -> refresh to be safe
        };
        if fresh {
            return Ok(state.token.access_token.clone());
        }

        debug!("access token expired or near expiry; renewing");
        let new_token = self.renew(&state.token).await?;
        state.access_exp = jwt_exp(&new_token.access_token);
        state.token = new_token;
        let access = state.token.access_token.clone();
        drop(state);
        self.persist().await;
        Ok(access)
    }

    /// Force a renewal irrespective of expiry (used by `login`).
    pub async fn force_refresh(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        let new_token = self.renew(&state.token).await?;
        state.access_exp = jwt_exp(&new_token.access_token);
        state.token = new_token;
        drop(state);
        self.persist().await;
        Ok(())
    }

    async fn renew(&self, current: &OAuth2Token) -> Result<OAuth2Token> {
        let rt = current.refresh_token.as_ref().ok_or_else(|| {
            ProxyError::Config(format!(
                "no refresh token available to renew the session.{}",
                login_instructions(&self.cfg)
            ))
        })?;
        refresh(&self.cfg, rt).await.map_err(|e| match e {
            ProxyError::SessionInvalidated => ProxyError::Config(format!(
                "the refresh token in {} was rejected (revoked/expired).{}",
                self.cfg.token_store.display(),
                login_instructions(&self.cfg)
            )),
            other => other,
        })
    }

    async fn persist(&self) {
        let state = self.state.lock().await;
        if let Err(e) = save_store(&self.cfg.token_store, &state.token) {
            warn!("could not persist token store: {e}");
        }
    }
}

fn load_store(path: &Path) -> Option<OAuth2Token> {
    let data = std::fs::read(path).ok()?;
    serde_json::from_slice(&data).ok()
}

fn save_store(path: &Path, token: &OAuth2Token) -> Result<()> {
    let json = serde_json::to_vec_pretty(token)?;
    std::fs::write(path, json)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    #[test]
    fn decodes_jwt_exp() {
        // payload = {"exp":1700000000}
        let payload =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"exp":1700000000}"#);
        let token = format!("h.{payload}.s");
        assert_eq!(jwt_exp(&token), Some(1700000000));
    }

    #[test]
    fn missing_exp_is_none() {
        let payload =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"sub":"x"}"#);
        let token = format!("h.{payload}.s");
        assert_eq!(jwt_exp(&token), None);
    }
}
