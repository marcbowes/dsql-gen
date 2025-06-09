use std::num::NonZero;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use aws_config::SdkConfig;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::sleep;
use tokio_retry::strategy::{jitter, ExponentialBackoff};

use crate::events::{Message, QueryErr, QueryOk, QueryResult};
use crate::pool::{Bundle, ConnectionPool, PoolConfig};
use crate::workload::{self, Workload};

pub struct WorkloadRunner {
    pub pool: ConnectionPool,
    workload: Workload,
    concurrency: Arc<AtomicUsize>,
    batches: Arc<AtomicUsize>,
    tx: mpsc::Sender<Message>,
}

impl WorkloadRunner {
    /// Create a new workload runner with the given configuration
    pub async fn new(
        endpoint: String,
        user: String,
        sdk_config: SdkConfig,
        workload_name: String,
        rows: usize,
        concurrency: NonZero<usize>,
        batches: usize,
        tx: mpsc::Sender<Message>,
    ) -> Result<Self> {
        let workloads = workload::load_all(rows);
        let workload = workloads
            .get(&workload_name)
            .ok_or_else(|| anyhow::anyhow!("unknown workload"))?
            .clone();

        let mut config = tokio_postgres::Config::new();
        config.host(endpoint);
        config.user(user);
        config.dbname("postgres");
        config.ssl_mode(tokio_postgres::config::SslMode::Require);
        config.ssl_negotiation(tokio_postgres::config::SslNegotiation::Direct);

        tracing::info!("launching pool");
        let (pool, mut telemetry) = ConnectionPool::launch(
            Bundle::new_with_sdk_config(config, sdk_config)?,
            PoolConfig {
                desired: concurrency,
                concurrent: concurrency,
            },
        )
        .await?;

        let _telem = tx.clone();
        tokio::spawn(async move {
            while let Some(t) = telemetry.recv().await {
                _telem.send(Message::PoolTelemetry(t)).await?;
            }
            anyhow::Ok(())
        });

        {
            tracing::info!(setup = %workload.setup, "will setup schema");
            let conn = pool.borrow().await;
            tracing::info!("connection acquired");
            _ = conn.execute(&workload.setup, &[]).await?;
            tracing::info!("schema ready");
        }

        let concurrency = Arc::new(AtomicUsize::new(concurrency.get()));
        let batches = Arc::new(AtomicUsize::new(batches));

        Ok(Self {
            pool,
            workload,
            concurrency,
            batches,
            tx,
        })
    }

    pub fn spawn(&self) -> JoinHandle<Result<()>> {
        let concurrency = self.concurrency.clone();
        let batches = self.batches.clone();
        let pool = self.pool.clone();
        let workload = self.workload.clone();
        let tx = self.tx.clone();

        tokio::spawn(async move {
            let mut set = JoinSet::new();
            let mut complete = 0;

            loop {
                if complete >= batches.load(Ordering::SeqCst) {
                    break;
                }

                // Maintain max connections.
                {
                    let concurrency = concurrency.load(Ordering::Relaxed) as u32;
                    while set.len() >= concurrency as usize {
                        if let Some(Ok(true)) = set.join_next().await {
                            complete += 1;
                        }
                    }

                    // TODO: replace sqlx pool.
                    // if pool.options().get_min_connections() != concurrency {
                    //     pool.options().min_connections(concurrency);
                    //     pool.options().max_connections(concurrency);
                    // }
                }

                // FIXME: This loop is buggy - it doesn't complete inflight

                let remaining = batches
                    .load(Ordering::SeqCst)
                    .saturating_sub(complete + set.len());
                if remaining > 0 {
                    set.spawn(execute_batch_with_retry(
                        pool.clone(),
                        workload.clone(),
                        tx.clone(),
                    ));
                }
            }

            while set.join_next().await.is_some() {}
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

/// Execute a batch with retry logic
async fn execute_batch_with_retry(
    pool: ConnectionPool,
    workload: Workload,
    tx: mpsc::Sender<Message>,
) -> bool {
    let retry_strategy = ExponentialBackoff::from_millis(10).map(jitter);
    for backoff in retry_strategy {
        let start = Instant::now();

        match attempt(pool.clone(), workload.clone()).await {
            Ok(_) => {
                _ = tx
                    .send(Message::QueryResult(QueryResult::Ok(QueryOk {
                        duration: start.elapsed(),
                        rows_inserted: workload.rows_inserted,
                        per_row_logical_bytes_written: workload.per_row_logical_bytes_written,
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

async fn attempt(pool: ConnectionPool, workload: Workload) -> Result<()> {
    let client = pool.borrow().await;
    let statement = client
        .statement("payload", workload.single_statement)
        .await?;
    _ = client.execute(&statement, &[]).await?;

    Ok(())
}
