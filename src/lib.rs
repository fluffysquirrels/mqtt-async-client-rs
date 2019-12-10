#![deny(warnings)]
#![recursion_limit="1024"]

pub mod client;
mod util;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
