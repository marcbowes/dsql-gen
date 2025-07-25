use std::num::NonZero;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::sleep;
use tokio_retry::strategy::{jitter, ExponentialBackoff};

use crate::events::{Message, QueryErr, QueryOk, QueryResult};
use crate::pool::ConnectionPool;
use crate::workloads::{Inserts, Workload};

pub type SharedWorkload = Arc<dyn Workload<T = Inserts> + Send + Sync + 'static>;
pub type SharedExecutor = Arc<dyn BatchExecutor + Send + Sync + 'static>;

pub struct WorkloadRunner {
    pub pool: ConnectionPool,
    executor: SharedExecutor,
    concurrency: Arc<AtomicUsize>,
    batches: Arc<AtomicUsize>,
    tx: mpsc::Sender<Message>,
    always_rollback: bool,
}

impl WorkloadRunner {
    /// Create a new workload runner with the given configuration
    pub async fn new(
        pool: ConnectionPool,
        executor: SharedExecutor,
        concurrency: NonZero<usize>,
        batches: usize,
        tx: mpsc::Sender<Message>,
        always_rollback: bool,
    ) -> Result<Self> {
        let concurrency = Arc::new(AtomicUsize::new(concurrency.get()));
        let batches = Arc::new(AtomicUsize::new(batches));

        Ok(Self {
            pool,
            executor,
            concurrency,
            batches,
            tx,
            always_rollback,
        })
    }

    pub fn spawn(&self) -> JoinHandle<Result<()>> {
        let concurrency = self.concurrency.clone();
        let batches = self.batches.clone();
        let pool = self.pool.clone();
        let executor = self.executor.clone();
        let tx = self.tx.clone();
        let always_rollback = self.always_rollback;

        tokio::spawn(async move {
            let mut set = JoinSet::new();
            let mut complete = 0;
            let mut spawned = 0;

            loop {
                let total_batches = batches.load(Ordering::Relaxed);

                if complete >= total_batches {
                    break;
                }

                // Maintain desired concurrency
                let current_concurrency = concurrency.load(Ordering::Relaxed);

                // Spawn new tasks to maintain concurrency level
                while set.len() < current_concurrency && spawned < total_batches {
                    set.spawn(executor.clone().execute_batch_with_retry(
                        pool.clone(),
                        tx.clone(),
                        always_rollback,
                    ));
                    spawned += 1;
                }

                // Wait for at least one task to complete before continuing
                if !set.is_empty() {
                    if let Some(Ok(true)) = set.join_next().await {
                        complete += 1;
                    }
                } else if spawned >= total_batches {
                    // All tasks spawned and completed
                    break;
                } else {
                    // Avoid busy-waiting when no tasks are running
                    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                }
            }

            // Wait for any remaining tasks
            while let Some(result) = set.join_next().await {
                if let Ok(true) = result {
                    complete += 1;
                }
            }

            tx.send(Message::WorkloadComplete).await?;
            Ok(())
        })
    }

    pub fn set_concurrency(&self, value: usize) {
        self.concurrency.store(value, Ordering::SeqCst);
    }

    pub fn concurrency(&self) -> usize {
        self.batches.load(Ordering::Relaxed)
    }

    pub fn set_batches(&self, value: usize) {
        self.batches.store(value, Ordering::SeqCst);
    }

    pub fn batches(&self) -> usize {
        self.batches.load(Ordering::Relaxed)
    }
}

#[async_trait]
pub trait BatchExecutor {
    async fn execute_batch_with_retry(
        self: Arc<Self>,
        pool: ConnectionPool,
        tx: mpsc::Sender<Message>,
        always_rollback: bool,
    ) -> bool;
}

#[derive(Clone)]
pub struct InsertsExecutor(pub SharedWorkload);

impl InsertsExecutor {
    async fn attempt(
        &self,
        pool: ConnectionPool,
        tx: mpsc::Sender<Message>,
        always_rollback: bool,
    ) -> Result<Inserts> {
        let mut client = pool.borrow().await?;
        let transaction = client.transaction().await?;

        let result = match self.0.transaction(&transaction, tx).await {
            Ok(inserts) => {
                // Transaction succeeded, commit or rollback based on flag
                if always_rollback {
                    transaction.rollback().await?;
                } else {
                    transaction.commit().await?;
                }
                Ok(inserts)
            }
            Err(e) => {
                _ = transaction.rollback().await;
                Err(e)
            }
        };

        result
    }
}

#[async_trait]
impl BatchExecutor for InsertsExecutor {
    /// Execute a batch with retry logic
    async fn execute_batch_with_retry(
        self: Arc<Self>,
        pool: ConnectionPool,
        tx: mpsc::Sender<Message>,
        always_rollback: bool,
    ) -> bool {
        let retry_strategy = ExponentialBackoff::from_millis(10).map(jitter);
        for backoff in retry_strategy {
            let start = Instant::now();

            match self
                .attempt(pool.clone(), tx.clone(), always_rollback)
                .await
            {
                Ok(inserts) => {
                    _ = tx
                        .send(Message::QueryResult(QueryResult::Ok(QueryOk {
                            duration: start.elapsed(),
                            rows_inserted: inserts.rows_inserted,
                            logical_bytes_written: inserts.logical_bytes_written,
                        })))
                        .await;
                    return true;
                }
                Err(err) => {
                    if tx
                        .send(Message::QueryResult(QueryResult::Err(QueryErr {
                            duration: start.elapsed(),
                            msg: err.to_string(),
                        })))
                        .await
                        .is_err()
                    {
                        return false;
                    }
                }
            }
            sleep(backoff).await;
        }
        false
    }
}
