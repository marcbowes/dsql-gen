use std::time::Duration;

use async_trait::async_trait;
use extend::ext;
use tokio::sync::mpsc::{self, error::SendError};

use crate::{
    pool::{self},
    usage::Usage,
};

#[derive(Debug, Clone)]
pub enum QueryResult {
    Ok(QueryOk),
    Err(QueryErr),
}

#[derive(Debug, Clone)]
pub struct QueryOk {
    pub duration: Duration,
    pub rows_inserted: usize,
    pub logical_bytes_written: usize,
}

#[derive(Debug, Clone)]
pub struct QueryErr {
    pub duration: Duration,
    pub msg: String,
}

/// Messages sent from the runner to the UI
#[derive(Debug)]
pub enum Message {
    /// A batch completed successfully or failed
    QueryResult(QueryResult),
    /// Initial usage
    InitialUsage(Usage),
    /// Usage information was updated
    UsageUpdated(Usage),
    TableDropping(String),
    TableDropped(String),
    TableCreating(String),
    TableCreated(String),
    TableLoading(String, usize),
    TableLoaded(String, usize),
    /// All batches have completed
    WorkloadComplete,
    /// Pool telemetry
    PoolTelemetry(pool::Telemetry),
}

#[ext]
#[async_trait]
pub impl mpsc::Sender<Message> {
    async fn table_dropping(&self, t: impl Into<String> + Send) -> Result<(), SendError<Message>> {
        self.send(Message::TableDropping(t.into())).await
    }

    async fn table_dropped(&self, t: impl Into<String> + Send) -> Result<(), SendError<Message>> {
        self.send(Message::TableDropped(t.into())).await
    }

    async fn table_creating(&self, t: impl Into<String> + Send) -> Result<(), SendError<Message>> {
        self.send(Message::TableCreating(t.into())).await
    }

    async fn table_created(&self, t: impl Into<String> + Send) -> Result<(), SendError<Message>> {
        self.send(Message::TableCreated(t.into())).await
    }

    async fn table_loading(
        &self,
        t: impl Into<String> + Send,
        rows: usize,
    ) -> Result<(), SendError<Message>> {
        self.send(Message::TableLoading(t.into(), rows)).await
    }

    async fn table_loaded(
        &self,
        t: impl Into<String> + Send,
        rows: usize,
    ) -> Result<(), SendError<Message>> {
        self.send(Message::TableLoaded(t.into(), rows)).await
    }
}
