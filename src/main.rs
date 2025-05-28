use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_dsql::auth_token::{AuthToken, AuthTokenGenerator, Config};
use chrono::Local;
use clap::Parser;
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Pool, Postgres};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;
use tokio::time;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// AWS DSQL cluster endpoint
    #[arg(short, long)]
    endpoint: String,

    /// AWS region
    #[arg(short, long, env = "AWS_REGION")]
    region: String,

    /// Number of concurrent connections
    #[arg(short, long, default_value_t = 10)]
    concurrency: u32,

    /// SQL query to execute per batch
    #[arg(
        short,
        long,
        default_value = "INSERT INTO test (content) SELECT md5(random()::text) FROM generate_series(1, 1000)"
    )]
    sql: String,

    /// Total number of batches to execute
    #[arg(short, long, default_value_t = 2000)]
    batches: usize,
}

#[derive(Clone)]
struct MetricsInner {
    completed_batches: usize,
    completed_since_last_tick: usize,
    error_count: usize,
    last_errors: VecDeque<String>,
    latency_histogram: Histogram<u64>,
}

#[derive(Clone)]
struct Metrics {
    inner: Arc<Mutex<MetricsInner>>,
    total_batches: usize,
}

impl Metrics {
    fn new(total_batches: usize) -> Self {
        let inner = MetricsInner {
            completed_batches: 0,
            completed_since_last_tick: 0,
            error_count: 0,
            last_errors: VecDeque::with_capacity(3),
            latency_histogram: Histogram::<u64>::new_with_bounds(1, 60_000, 3).unwrap(),
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
            total_batches,
        }
    }

    fn record_success(&self, duration_ms: u64) {
        let mut inner = self.inner.lock().unwrap();
        inner.completed_batches += 1;
        inner.completed_since_last_tick += 1;

        inner
            .latency_histogram
            .record(duration_ms)
            .unwrap_or_else(|_| {
                // If value is out of range, record the max value
                inner.latency_histogram.record(60_000).unwrap();
            });
    }

    fn record_error(&self, error: String) {
        let mut inner = self.inner.lock().unwrap();
        inner.error_count += 1;

        if inner.last_errors.len() >= 3 {
            inner.last_errors.pop_front();
        }
        inner.last_errors.push_back(error);
    }

    fn read(&self) -> MetricsInner {
        self.inner.lock().unwrap().clone()
    }

    fn get_and_reset_tps(&self, interval_secs: f64) -> f64 {
        let mut inner = self.inner.lock().unwrap();
        let completed = inner.completed_since_last_tick;
        inner.completed_since_last_tick = 0;
        completed as f64 / interval_secs
    }
}

async fn generate_password_token(
    signer: &AuthTokenGenerator,
    sdk_config: &SdkConfig,
) -> anyhow::Result<AuthToken> {
    Ok(signer
        .db_connect_admin_auth_token(sdk_config)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to generate auth token: {}", e))?)
}

/// Establish a pooled connection with periodic credential refresh.
async fn establish_connection_pool(
    endpoint: String,
    region: String,
    max_connections: u32,
) -> anyhow::Result<Pool<Postgres>> {
    let sdk_config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let signer = AuthTokenGenerator::new(
        Config::builder()
            .hostname(&endpoint)
            .region(Region::new(region))
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
) -> anyhow::Result<()> {
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

async fn execute_batch(pool: &Pool<Postgres>, sql: String, _batch_id: usize) -> anyhow::Result<()> {
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

async fn monitor_progress(metrics: Metrics, pool: Pool<Postgres>, progress_bar: ProgressBar) {
    let mut interval = time::interval(Duration::from_secs(5));
    let interval_secs = 5.0;

    loop {
        interval.tick().await;

        // Get TPS and reset counter
        let tps = metrics.get_and_reset_tps(interval_secs);

        let m = metrics.read();

        // Update progress bar
        progress_bar.set_position(m.completed_batches as u64);

        // Print stats
        println!(
            "\n========== {} ==========",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        let progress_pct = (m.completed_batches as f64 / metrics.total_batches as f64) * 100.0;
        println!(
            "Progress: {}/{} ({:.1}%)",
            m.completed_batches, metrics.total_batches, progress_pct
        );
        println!("TPS: {:.1} batches/sec", tps);
        println!("Errors: {} total", m.error_count);
        println!("Pool: {} open, {} idle", pool.size(), pool.num_idle());

        if !m.last_errors.is_empty() {
            println!("Last {} errors:", m.last_errors.len());
            for (i, error) in m.last_errors.iter().enumerate() {
                println!("  {}: {}", i + 1, error);
            }
        }

        let p50 = m.latency_histogram.value_at_quantile(0.50) as f64;
        let p90 = m.latency_histogram.value_at_quantile(0.90) as f64;
        let p99 = m.latency_histogram.value_at_quantile(0.99) as f64;
        let p999 = m.latency_histogram.value_at_quantile(0.999) as f64;

        println!("Latency (ms):");
        println!("  p50: {:.1}", p50);
        println!("  p90: {:.1}", p90);
        println!("  p99: {:.1}", p99);
        println!("  p99.9: {:.1}", p999);

        if m.completed_batches >= metrics.total_batches {
            break;
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Establishing connection pool...");
    let pool =
        establish_connection_pool(args.endpoint.clone(), args.region.clone(), args.concurrency)
            .await?;
    println!("Connection pool established successfully");

    // Setup metrics and progress bar
    let metrics = Metrics::new(args.batches);
    let progress_bar = ProgressBar::new(args.batches as u64);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40} {pos:>7}/{len:7} {msg}")
            .unwrap()
            .progress_chars("##-"),
    );

    // Start monitoring task
    let monitor_metrics = metrics.clone();
    let monitor_progress_bar = progress_bar.clone();
    let _pool = pool.clone();
    let monitor_handle = tokio::spawn(async move {
        monitor_progress(monitor_metrics, _pool, monitor_progress_bar).await;
    });

    let mut set = JoinSet::new();

    for i in 0..args.batches {
        while set.len() >= args.concurrency as usize {
            set.join_next().await;
        }
        set.spawn(execute_batch_with_retry(
            pool.clone(),
            args.sql.clone(),
            i,
            metrics.clone(),
        ));
    }

    while set.join_next().await.is_some() {}

    progress_bar.finish_with_message("Complete");

    // Wait for final stats
    time::sleep(Duration::from_millis(100)).await;

    println!("\nCompleted {} batches", args.batches);
    let metrics = metrics.read();
    println!("Total errors: {}", metrics.error_count);

    // Cancel the monitor task
    monitor_handle.abort();

    // Close the connection pool
    pool.close().await;

    Ok(())
}
