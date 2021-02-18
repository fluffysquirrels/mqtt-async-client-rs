//! An MQTT 3.1.1 client written in Rust.
//!
//! For example usage see the command-line test app at
//! `examples/mqttc.rs`, and integration tests at `tests/*.rs`.
//!
//! This crate uses the log crate. To enable extra, potentially
//! sensitive logging (including passwords) enable the
//! "unsafe-logging" Cargo feature. With "unsafe-logging" enabled at
//! the "trace" log level every packet is logged.
//!
//! The "tls" feature is enabled by default and allows connections
//! over TLS using [rustls](https://crates.io/crates/rustls).
//! If TLS is not required you can opt out by specifying
//! `default-features = false`.
//! E.g. `mqtt-async-client = { version = "0.1", default-features = false }`
//!
//! The "serde" feature is disabled by default, but when enabled
//! `client::ClientBuilder` will implement `serde::Deserialize`. For example,
//! you can then `Deserialize` your `ClientBuilder` with options from a config
//! file.
#![deny(warnings)]
#![deny(missing_docs)]

// The futures_util::select! macro needs a higher recursion_limit
#![recursion_limit="1024"]

pub mod client;
mod error;
pub mod util;

pub use error::{Error, Result};
