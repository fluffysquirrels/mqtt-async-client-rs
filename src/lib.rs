#![deny(warnings)]

pub mod client;
mod util;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
