use std::future::Future;
use tokio::{
    self,
    task::JoinHandle
};

#[derive(Clone, Debug)]
pub enum TokioRuntime {
    Default,
    Handle(tokio::runtime::Handle),
}

impl TokioRuntime {
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
