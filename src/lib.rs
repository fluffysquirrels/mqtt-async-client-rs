//! An MQTT 3.1.1 client written in Rust.
//!
//! For example usage see the command-line test app at
//! `src/bin/mqttc.rs`, and integration tests at `tests/*.rs`.

#![deny(warnings)]
#![deny(missing_docs)]

// The futures_util::select! macro needs a higher recursion_limit
#![recursion_limit="1024"]

pub mod client;
mod error;
pub mod util;

pub use error::{Error, Result};
