use anyhow::Result;
use aws_config::SdkConfig;
use aws_sdk_dsql::auth_token::{AuthToken, AuthTokenGenerator, Config as AuthConfig};
use hdrhistogram::Histogram;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Pool, Postgres};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{self, sleep};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};

use crate::usage::{calculate_costs, Usage, UsageCalculator};
use crate::workload::{self, Workload};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone)]
pub struct ErrorEntry {
    pub timestamp: SystemTime,
    pub message: String,
}

#[derive(Clone)]
pub struct MetricsInner {
    pub completed_batches: usize,
    pub completed_since_last_tick: usize,
    pub error_count: usize,
    pub errors_since_last_tick: usize,
    pub last_errors: VecDeque<ErrorEntry>,
    pub latency_histogram: Histogram<u64>,
}

impl Default for MetricsInner {
    fn default() -> Self {
        Self {
            completed_batches: 0,
            completed_since_last_tick: 0,
            error_count: 0,
            errors_since_last_tick: 0,
            last_errors: VecDeque::with_capacity(5),
            latency_histogram: Histogram::<u64>::new_with_bounds(1, 60_000 * 10, 3).unwrap(),
        }
    }
}

#[derive(Clone)]
pub struct Metrics {
    pub inner: Arc<Mutex<MetricsInner>>,
    pub total_batches: usize,
}

impl Metrics {
    pub fn new(total_batches: usize) -> Self {
        let inner = MetricsInner::default();

        Self {
            inner: Arc::new(Mutex::new(inner)),
            total_batches,
        }
    }

    pub fn record_success(&self, duration_ms: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.completed_batches += 1;
        inner.completed_since_last_tick += 1;

        inner
            .latency_histogram
            .record(duration_ms)
            .expect("histogram is correctly configured");
    }

    pub fn record_error(&self, error: String) {
        let mut inner = self.inner.lock().unwrap();
        inner.error_count += 1;
        inner.errors_since_last_tick += 1;

        if inner.last_errors.len() >= 5 {
            inner.last_errors.pop_front();
        }
        inner.last_errors.push_back(ErrorEntry {
            timestamp: SystemTime::now(),
            message: error,
        });
    }

    pub fn read(&self) -> MetricsInner {
        self.inner.lock().unwrap().clone()
    }

    pub fn get_and_reset(&self) -> (usize, usize, Histogram<u64>) {
        let mut inner = self.inner.lock().unwrap();
        let completed = inner.completed_since_last_tick;
        let errors = inner.errors_since_last_tick;
        inner.completed_since_last_tick = 0;
        inner.errors_since_last_tick = 0;
        let histogram = inner.latency_histogram.clone();
        inner.latency_histogram.reset();
        (completed, errors, histogram)
    }
}

#[derive(Clone)]
pub struct WorkloadRunner {
    pub identifier: String,
    pub endpoint: String,
    pub pool: Pool<Postgres>,
    pub workload: Workload,
    pub metrics: Metrics,
    pub usage_rx: watch::Receiver<Usage>,
    usage_task_abort: Arc<AtomicBool>,
    pub initial_usage: Usage,
}

impl WorkloadRunner {
    pub async fn new(
        identifier: String,
        endpoint: String,
        sdk_config: SdkConfig,
        workload_name: String,
        rows: usize,
        concurrency: u32,
        batches: usize,
    ) -> Result<Self> {
        let workloads = workload::load_all(rows);
        let workload = workloads
            .get(&workload_name)
            .ok_or_else(|| anyhow::anyhow!("unknown workload"))?
            .clone();

        println!("Establishing connection pool...");
        let pool =
            establish_connection_pool(endpoint.clone(), sdk_config.clone(), concurrency).await?;
        println!("Connection pool established successfully");

        sqlx::query(&workload.setup).execute(&pool).await?;

        let cal = UsageCalculator::new(identifier.clone(), &sdk_config);
        let initial_usage = get_initial_usage(&cal).await?;

        let (tx, rx) = watch::channel(initial_usage.clone());

        let abort_flag = Arc::new(AtomicBool::new(false));
        let abort_flag_clone = abort_flag.clone();

        tokio::spawn(async move {
            loop {
                if abort_flag_clone.load(Ordering::Relaxed) {
                    break;
                }

                if let Ok(it) = cal.dpus_this_month().await {
                    tx.send_if_modified(move |usage| usage.set_dpu_metrics(it));
                }

                if let Ok(it) = cal.current_storage_usage().await {
                    tx.send_if_modified(move |usage| usage.set_storage_metrics(it));
                }

                sleep(Duration::from_secs(30)).await;
            }
        });

        let metrics = Metrics::new(batches);

        Ok(Self {
            identifier,
            endpoint,
            pool,
            workload,
            metrics,
            usage_rx: rx,
            usage_task_abort: abort_flag,
            initial_usage,
        })
    }

    pub async fn run(&self, concurrency: u32, batches: usize) -> Result<()> {
        let mut set = JoinSet::new();

        for i in 0..batches {
            while set.len() >= concurrency as usize {
                set.join_next().await;
            }
            set.spawn(execute_batch_with_retry(
                self.pool.clone(),
                self.workload.single_statement.clone(),
                i,
                self.metrics.clone(),
            ));
        }

        while set.join_next().await.is_some() {}

        Ok(())
    }

    pub async fn cleanup(self) -> Result<(Usage, Usage)> {
        // Wait for final stats
        sleep(Duration::from_millis(100)).await;

        println!("\nCompleted {} batches", self.metrics.total_batches);
        let metrics = self.metrics.read();
        println!("Total errors: {}", metrics.error_count);

        self.usage_task_abort.store(true, Ordering::Relaxed);

        println!("waiting for final usage");
        sleep(Duration::from_secs(90)).await;
        let final_usage = self.usage_rx.borrow().clone();

        self.pool.close().await;

        Ok((self.initial_usage, final_usage))
    }
}

async fn generate_password_token(
    signer: &AuthTokenGenerator,
    sdk_config: &SdkConfig,
) -> Result<AuthToken> {
    signer
        .db_connect_admin_auth_token(sdk_config)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to generate auth token: {}", e))
}

async fn establish_connection_pool(
    endpoint: String,
    sdk_config: SdkConfig,
    max_connections: u32,
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
        .max_connections(max_connections)
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

async fn execute_batch_with_retry(
    pool: Pool<Postgres>,
    sql: String,
    batch_id: usize,
    metrics: Metrics,
) -> Result<()> {
    let retry_strategy = ExponentialBackoff::from_millis(10).map(jitter);
    let start = Instant::now();

    let result = Retry::spawn(retry_strategy, || {
        execute_batch(&pool, sql.clone(), batch_id)
    })
    .await;

    let duration_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok(()) => {
            metrics.record_success(duration_ms);
            Ok(())
        }
        Err(err) => {
            metrics.record_error(format!("Batch {batch_id}: {err}"));
            Err(err)
        }
    }
}

async fn execute_batch(pool: &Pool<Postgres>, sql: String, _batch_id: usize) -> Result<()> {
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

async fn get_initial_usage(cal: &UsageCalculator) -> Result<Usage> {
    let mut dpus = None;
    let mut storage = None;

    loop {
        if let Ok(it) = cal.dpus_this_month().await {
            dpus = Some(it)
        }
        if let Ok(it) = cal.current_storage_usage().await {
            storage = Some(it)
        };

        if dpus.is_some() && storage.is_some() {
            break;
        }

        // XXX: New clusters will always have metrics due to the initial table
        // creation. Wait for them to come through.
        println!("no metrics yet, assuming this is a new cluster, retrying..");
        sleep(Duration::from_secs(30)).await;
    }

    let dpu_metrics = dpus.unwrap();
    let storage_metrics = storage.unwrap();
    let cost_estimate = calculate_costs(&dpu_metrics, &storage_metrics);
    Ok(Usage {
        dpu_metrics,
        storage_metrics,
        cost_estimate,
    })
}

pub fn print_usage(latest_usage: &Usage) {
    let storage = byte_unit::Byte::from_u64(latest_usage.storage_metrics.size_bytes as u64)
        .get_appropriate_unit(byte_unit::UnitType::Decimal);
    let total_cost = latest_usage.cost_estimate.total_dpus.total
        + latest_usage.cost_estimate.latest_storage.gb_month;

    println!("\n{:=^70}", " Usage & Cost ");
    println!("{:<15} {:>15} {:>15}", "", "Usage", "Cost");
    println!("{:-<15} {:-^15} {:-^15}", "", "", "");
    println!(
        "{:<15} {:>15.2} {:>15}",
        "Total DPUs:",
        latest_usage.dpu_metrics.total,
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.total)
    );
    println!(
        "{:<15} {:>15.2} {:>15}",
        "  Compute:",
        latest_usage.dpu_metrics.compute,
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.compute)
    );
    println!(
        "{:<15} {:>15.2} {:>15}",
        "  Read:",
        latest_usage.dpu_metrics.read,
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.read)
    );
    println!(
        "{:<15} {:>15.2} {:>15}",
        "  Write:",
        latest_usage.dpu_metrics.write,
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.write)
    );
    println!(
        "{:<15} {:>15} {:>15}",
        "Storage:",
        storage.to_string(),
        format!("${:.2}", latest_usage.cost_estimate.latest_storage.gb_month)
    );
    println!("{:═<15} {:═^15} {:═^15}", "", "", "");
    println!(
        "{:<15} {:>15} {:>15}",
        "TOTAL COST:",
        "",
        format!("${:.2}", total_cost)
    );
    println!("{:=^70}", "");
}

pub fn print_usage_and_diff(latest_usage: &Usage, usage_diff: &Usage) {
    let storage_current = byte_unit::Byte::from_u64(latest_usage.storage_metrics.size_bytes as u64)
        .get_appropriate_unit(byte_unit::UnitType::Decimal);
    let storage_diff = byte_unit::Byte::from_u64(usage_diff.storage_metrics.size_bytes as u64)
        .get_appropriate_unit(byte_unit::UnitType::Decimal);

    let total_cost_current = latest_usage.cost_estimate.total_dpus.total
        + latest_usage.cost_estimate.latest_storage.gb_month;
    let total_cost_diff = usage_diff.cost_estimate.total_dpus.total
        + usage_diff.cost_estimate.latest_storage.gb_month;

    println!("\n{:=^90}", " Usage & Cost ");
    println!(
        "{:<15} {:>15} {:>15} {:>15} {:>15}",
        "", "Usage", "Delta", "Cost", "Delta"
    );
    println!(
        "{:-<15} {:-^15} {:-^15} {:-^15} {:-^15}",
        "", "", "", "", ""
    );
    println!(
        "{:<15} {:>15.2} {:>15} {:>15} {:>15}",
        "Total DPUs:",
        latest_usage.dpu_metrics.total,
        format!("(+{:.2})", usage_diff.dpu_metrics.total),
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.total),
        format!("(+${:.2})", usage_diff.cost_estimate.total_dpus.total)
    );
    println!(
        "{:<15} {:>15.2} {:>15} {:>15} {:>15}",
        "  Compute:",
        latest_usage.dpu_metrics.compute,
        format!("(+{:.2})", usage_diff.dpu_metrics.compute),
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.compute),
        format!("(+${:.2})", usage_diff.cost_estimate.total_dpus.compute)
    );
    println!(
        "{:<15} {:>15.2} {:>15} {:>15} {:>15}",
        "  Read:",
        latest_usage.dpu_metrics.read,
        format!("(+{:.2})", usage_diff.dpu_metrics.read),
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.read),
        format!("(+${:.2})", usage_diff.cost_estimate.total_dpus.read)
    );
    println!(
        "{:<15} {:>15.2} {:>15} {:>15} {:>15}",
        "  Write:",
        latest_usage.dpu_metrics.write,
        format!("(+{:.2})", usage_diff.dpu_metrics.write),
        format!("${:.2}", latest_usage.cost_estimate.total_dpus.write),
        format!("(+${:.2})", usage_diff.cost_estimate.total_dpus.write)
    );
    println!(
        "{:<15} {:>15} {:>15} {:>15} {:>15}",
        "Storage:",
        storage_current.to_string(),
        format!("(+{})", storage_diff),
        format!("${:.2}", latest_usage.cost_estimate.latest_storage.gb_month),
        format!(
            "(+${:.2})",
            usage_diff.cost_estimate.latest_storage.gb_month
        )
    );
    println!(
        "{:═<15} {:═^15} {:═^15} {:═^15} {:═^15}",
        "", "", "", "", ""
    );
    println!(
        "{:<15} {:>15} {:>15} {:>15} {:>15}",
        "TOTAL COST:",
        "",
        "",
        format!("${:.2}", total_cost_current),
        format!("(+${:.2})", total_cost_diff)
    );
    println!("{:=^90}", "");
}
