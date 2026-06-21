//! `recall` binary entry point. Loads configuration (X6), initialises logging (X3) and the tracing
//! seam (X5), builds the application state, and serves until shutdown (X13). Only `main` may panic on
//! unrecoverable bootstrap (X1 / C5 rule); library code never panics.

use std::process::ExitCode;

use recall::config::Config;
use recall::{build_state, serve};

#[tokio::main]
async fn main() -> ExitCode {
    // X6: load and validate configuration. A failure exits non-zero; the key is named, never the
    // value (so a secret is never logged).
    let config = match Config::load() {
        Ok(config) => config,
        Err(err) => {
            // Logging is not yet initialised; emit to stderr directly with the key only.
            eprintln!("{err}");
            return ExitCode::FAILURE;
        }
    };

    // X3: initialise structured logging from the validated config.
    recall::obs::log::init_logging(&config);
    tracing::info!(target: "recall", env = ?config.env, "config.loaded");

    let state = match build_state(config).await {
        Ok(state) => state,
        Err(err) => {
            tracing::error!(target: "recall", error = %err, "failed to build application state");
            return ExitCode::FAILURE;
        }
    };

    if let Err(err) = serve(state).await {
        tracing::error!(target: "recall", error = %err, "server exited with error");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
