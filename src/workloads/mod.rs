use anyhow::Result;
use async_trait::async_trait;

use crate::pool::ClientHandle;

pub mod counter;
pub mod onekib;
pub mod tiny;

pub struct Inserts {
    pub rows_inserted: usize,
    pub logical_bytes_written: usize,
}

#[async_trait]
pub trait Workload {
    type T;

    async fn setup(&self, client: ClientHandle) -> Result<()>;
    async fn transaction(&self, client: ClientHandle) -> Result<Self::T>;
}
