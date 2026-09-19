//! Bosch Smart Camera RTSP proxy.
//!
//! Re-exposes Bosch cloud cameras as plain `rtsp://` URLs.

mod auth;
mod backend;
mod camera;
mod config;
mod error;
mod ingest;
mod placeholder;
mod rtsp;
mod stream_manager;
mod tls_relay;

use std::io::{self, Write};
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::auth::tokens::login_instructions;
use crate::auth::AuthManager;
use crate::backend::BackendClient;
use crate::config::Config;
use crate::error::{ProxyError, Result};
use crate::rtsp::{RtspServer, StreamRegistry};
use crate::stream_manager::StreamManager;

#[derive(Parser)]
#[command(name = "bosch-cam-proxy", about = "Proxy Bosch Smart Camera streams as plain RTSP")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the RTSP proxy server (default).
    Run,
    /// List the cameras available to the account and their proxy URLs.
    ListCameras,
    /// Check the login session: if TOKEN_STORE holds a usable refresh token, verify
    /// it with a forced refresh; otherwise print the login URL and step-by-step
    /// instructions for obtaining one out-of-band (there's no scripted login —
    /// the identity provider's login form is gated behind a bot-protection
    /// challenge), then prompt for the resulting authorization code and complete
    /// the exchange right here.
    Login,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("bosch_cam_proxy=info")),
        )
        .init();

    if let Err(e) = run().await {
        error!("fatal: {}", describe_error(&e));
        std::process::exit(1);
    }
}

/// Render an error together with its full `source()` chain, since the
/// top-level `Display` of a `reqwest::Error` hides the underlying cause
/// (e.g. a TLS trust failure) that's usually what you actually need to see.
fn describe_error(e: &(dyn std::error::Error + 'static)) -> String {
    let mut msg = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        source = s.source();
    }
    msg
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let cfg = Arc::new(Config::from_env()?);

    match cli.command.unwrap_or(Command::Run) {
        Command::Login => login(cfg).await,
        Command::ListCameras => list_cameras(cfg).await,
        Command::Run => serve(cfg).await,
    }
}

/// Verify an existing session, or walk the user through obtaining one right here:
/// print the login URL/instructions, prompt for the resulting authorization code,
/// and exchange it immediately.
async fn login(cfg: Arc<Config>) -> Result<()> {
    match AuthManager::bootstrap(Arc::clone(&cfg)).await {
        Ok(auth) => {
            info!("session OK; forcing a token refresh...");
            auth.force_refresh().await?;
            println!(
                "login: SUCCESS; session verified and persisted to {}",
                cfg.token_store.display()
            );
            Ok(())
        }
        // `bootstrap` only returns `Config` for a missing/rejected refresh token
        // (env vars were already validated), with a deliberately short message
        // (see its docs) — print it plus the full login-URL instructions, then
        // continue interactively instead of a scary "fatal: configuration error"
        // log line.
        Err(ProxyError::Config(msg)) => {
            println!("Not logged in: {msg}.");
            println!("{}", login_instructions(&cfg));
            print!("\nPaste the code here: ");
            io::stdout().flush().ok();
            let mut code = String::new();
            io::stdin().read_line(&mut code)?;
            let code = code.trim();
            if code.is_empty() {
                return Err(ProxyError::Config("no authorization code entered".into()));
            }

            AuthManager::bootstrap_from_code(Arc::clone(&cfg), code).await?;
            println!(
                "login: SUCCESS; session persisted to {}",
                cfg.token_store.display()
            );
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Bootstrap a session for a non-interactive command (`serve`/`list-cameras`): on
/// a missing/invalid session, print a short hint pointing at `login` (the one
/// place that actually walks through obtaining one) and exit, rather than
/// surfacing a raw "fatal: configuration error" log line.
async fn require_session(cfg: &Arc<Config>) -> Result<Arc<AuthManager>> {
    match AuthManager::bootstrap(Arc::clone(cfg)).await {
        Ok(auth) => Ok(auth),
        Err(ProxyError::Config(msg)) => {
            println!("Not logged in: {msg}.\nRun `bosch-cam-proxy login` to create a session.");
            std::process::exit(1);
        }
        Err(e) => Err(e),
    }
}

async fn list_cameras(cfg: Arc<Config>) -> Result<()> {
    let auth = require_session(&cfg).await?;
    let backend = Arc::new(BackendClient::new(Arc::clone(&cfg), auth)?);
    let cameras = backend.list_video_inputs().await?;

    if cameras.is_empty() {
        println!("No cameras found for this account.");
        return Ok(());
    }
    println!("Cameras ({}):", cameras.len());
    for cam in &cameras {
        println!(
            "  {:<40} rtsp://{}/{}",
            cam.label(),
            cfg.rtsp_bind,
            cam.id
        );
    }
    Ok(())
}

async fn serve(cfg: Arc<Config>) -> Result<()> {
    let auth = require_session(&cfg).await?;
    let backend = Arc::new(BackendClient::new(Arc::clone(&cfg), auth)?);

    // Log the available camera paths at startup (best-effort).
    match backend.list_video_inputs().await {
        Ok(cameras) => {
            info!("{} camera(s) available:", cameras.len());
            for cam in &cameras {
                info!("  rtsp://{}/{}   [{}]", cfg.rtsp_bind, cam.id, cam.label());
            }
        }
        Err(e) => error!("could not list cameras at startup: {}", describe_error(&e)),
    }

    let registry = StreamRegistry::new();
    let manager = StreamManager::new(Arc::clone(&cfg), backend, Arc::clone(&registry));
    let server = Arc::new(RtspServer::new(
        Arc::clone(&cfg),
        registry,
        Arc::clone(&manager),
    ));

    let serve = tokio::spawn({
        let server = Arc::clone(&server);
        async move { server.run().await }
    });

    tokio::select! {
        res = serve => {
            match res {
                Ok(inner) => inner?,
                Err(e) => error!("server task panicked: {e}"),
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("shutting down; stopping all ingests");
            manager.shutdown().await;
        }
    }
    Ok(())
}
