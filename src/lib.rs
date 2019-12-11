#![deny(warnings)]

// The futures_util::select! macro needs a higher recursion_limit
#![recursion_limit="1024"]

pub mod client;
mod error;
mod util;

pub use error::{Error, Result};
