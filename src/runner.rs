use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use aws_config::SdkConfig;
use aws_sdk_dsql::auth_token::{AuthToken, AuthTokenGenerator, Config as AuthConfig};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Pool, Postgres};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{self, sleep};
use tokio_retry::strategy::{jitter, ExponentialBackoff};

use crate::events::{Message, QueryErr, QueryOk, QueryResult};
use crate::workload::{self, Workload};

pub struct WorkloadRunner {
    pub pool: Pool<Postgres>,
    workload: Workload,
    concurrency: Arc<AtomicUsize>,
    batches: Arc<AtomicUsize>,
    tx: mpsc::Sender<Message>,
}

impl WorkloadRunner {
    /// Create a new workload runner with the given configuration
    pub async fn new(
        endpoint: String,
        sdk_config: SdkConfig,
        workload_name: String,
        rows: usize,
        concurrency: usize,
        batches: usize,
        tx: mpsc::Sender<Message>,
    ) -> Result<Self> {
        let workloads = workload::load_all(rows);
        let workload = workloads
            .get(&workload_name)
            .ok_or_else(|| anyhow::anyhow!("unknown workload"))?
            .clone();

        tracing::info!(%endpoint, %concurrency, "opening connection pool");
        let pool =
            establish_connection_pool(endpoint.clone(), sdk_config.clone(), concurrency as u32)
                .await?;
        tracing::info!("pool ready");

        tracing::info!(setup = %workload.setup, "running workload setup");
        sqlx::query(&workload.setup).execute(&pool).await?;
        tracing::info!("schema ready");

        let concurrency = Arc::new(AtomicUsize::new(concurrency));
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

                let _pool = pool.clone();
                let _workload = workload.clone();
                let _tx = tx.clone();

                set.spawn(async move { execute_batch_with_retry(_pool, _workload, _tx).await });
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

impl Drop for WorkloadRunner {
    fn drop(&mut self) {
        let pool = self.pool.clone();
        tokio::spawn(async move {
            pool.close().await;
        });
    }
}

/// Execute a batch with retry logic
async fn execute_batch_with_retry(
    pool: Pool<Postgres>,
    workload: Workload,
    tx: mpsc::Sender<Message>,
) -> bool {
    let retry_strategy = ExponentialBackoff::from_millis(10).map(jitter);
    for backoff in retry_strategy {
        let start = Instant::now();
        match sqlx::query(&workload.single_statement).execute(&pool).await {
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

/// Generate a password token for connection to the database
async fn generate_password_token(
    signer: &AuthTokenGenerator,
    sdk_config: &SdkConfig,
) -> Result<AuthToken> {
    signer
        .db_connect_admin_auth_token(sdk_config)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to generate auth token: {}", e))
}

/// Establish a connection pool to the database
async fn establish_connection_pool(
    endpoint: String,
    sdk_config: SdkConfig,
    connections: u32,
) -> Result<Pool<Postgres>> {
    let signer = AuthTokenGenerator::new(
        AuthConfig::builder()
            .hostname(&endpoint)
            .expires_in(900)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build auth config: {}", e))?,
    );

    let password_token = generate_password_token(&signer, &sdk_config).await?;

    let connection_options = PgConnectOptions::new()
        .host(&endpoint)
        .port(5432)
        .database("postgres")
        .username("admin")
        .password(password_token.as_str())
        .ssl_mode(sqlx::postgres::PgSslMode::VerifyFull);

    let pool = PgPoolOptions::new()
        // .min_connections(connections) // Makes it too slow to start.
        .max_connections(connections)
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(3600))
        .max_lifetime(Duration::from_secs(3600))
        .connect_with(connection_options.clone())
        .await?;

    // Periodically refresh the password by regenerating the token.
    let _pool = pool.clone(); // Pool uses an Arc internally
    tokio::spawn(async move {
        loop {
            time::sleep(Duration::from_secs(600)).await;

            match generate_password_token(&signer, &sdk_config).await {
                Ok(password_token) => {
                    let connect_options_with_new_token =
                        connection_options.clone().password(password_token.as_str());
                    _pool.set_connect_options(connect_options_with_new_token);
                }
                Err(err) => {
                    eprintln!("Failed to refresh authentication token: {err}");
                }
            }
        }
    });

    Ok(pool)
}
