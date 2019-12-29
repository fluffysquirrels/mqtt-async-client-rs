use std::future::Future;
use tokio::{
    self,
    task::JoinHandle
};

/// Represents a tokio runtime on which to spawn tasks.
#[derive(Clone, Debug)]
pub enum TokioRuntime {
    /// Represents the default global tokio runtime, i.e. to use [tokio::spawn](https://docs.rs/tokio/0.2.6/tokio/fn.spawn.html)
    Default,

    /// Encapsulates a [tokio::runtime::Handle](https://docs.rs/tokio/0.2.6/tokio/runtime/struct.Handle.html) to use to spawn tasks.
    Handle(tokio::runtime::Handle),
}

impl TokioRuntime {
    /// Spawn a task onto the selected tokio runtime.
    pub fn spawn<F>(&self, f: F) -> JoinHandle<F::Output>
        where
            F: Future + Send + 'static,
            F::Output: Send + 'static, {
        match self {
            TokioRuntime::Default => tokio::spawn(f),
            TokioRuntime::Handle(h) => h.spawn(f),
        }
    }
}

impl Default for TokioRuntime {
    fn default() -> TokioRuntime {
        TokioRuntime::Default
    }
}
