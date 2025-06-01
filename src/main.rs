use anyhow::{anyhow, Result};
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_sdk_dsql::auth_token::{AuthToken, AuthTokenGenerator, Config};
use byte_unit::Byte;
use chrono::Local;
use clap::Parser;
use dsql_gen::usage::{self, Usage, UsageCalculator};
use dsql_gen::workload::{self, Workload};
use hdrhistogram::Histogram;
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Pool, Postgres};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{self, sleep};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
enum Commands {
    /// Run the load generator
    Run(RunArgs),
    /// Print usage information for a cluster
    Usage(UsageArgs),
}

#[derive(Parser, Debug)]
struct RunArgs {
    /// AWS DSQL cluster ID
    #[arg(short, long)]
    identifier: String,

    /// AWS region
    #[arg(short, long, env = "AWS_REGION")]
    region: Option<String>,

    /// Number of concurrent connections
    #[arg(short, long, default_value_t = 10)]
    concurrency: u32,

    /// Total number of batches to execute
    #[arg(short, long, default_value_t = 2000)]
    batches: usize,

    /// Workload name
    #[arg(short, long)]
    workload: String,

    /// Workload rows
    #[arg(short = 'n', long, default_value_t = 0)]
    rows: usize,
}

impl RunArgs {
    fn endpoint(&self, sdk_config: &SdkConfig) -> Result<String> {
        let region = sdk_config
            .region()
            .ok_or_else(|| anyhow!("no region set"))?;
        Ok(format!(
            "{}.dsql.{}.on.aws",
            self.identifier,
            region.as_ref()
        ))
    }

    async fn sdk_config(&self) -> SdkConfig {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(r) = &self.region {
            loader = loader.region(Region::new(r.clone()));
        }
        loader.load().await
    }
}

impl UsageArgs {
    async fn sdk_config(&self) -> SdkConfig {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(r) = &self.region {
            loader = loader.region(Region::new(r.clone()));
        }
        loader.load().await
    }
}

#[derive(Parser, Debug)]
struct UsageArgs {
    /// AWS DSQL cluster ID
    #[arg(short, long)]
    identifier: String,

    /// AWS region
    #[arg(short, long, env = "AWS_REGION")]
    region: Option<String>,
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
            latency_histogram: Histogram::<u64>::new_with_bounds(1, 60_000 * 10, 3).unwrap(),
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
            .expect("histogram is correctly configured");
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

    fn get_and_reset(&self) -> (usize, Histogram<u64>) {
        let mut inner = self.inner.lock().unwrap();
        let completed = inner.completed_since_last_tick;
        inner.completed_since_last_tick = 0;
        let histogram = inner.latency_histogram.clone();
        inner.latency_histogram.reset();
        (completed, histogram)
    }
}

async fn generate_password_token(
    signer: &AuthTokenGenerator,
    sdk_config: &SdkConfig,
) -> Result<AuthToken> {
    signer
        .db_connect_admin_auth_token(sdk_config)
        .await
        .map_err(|e| anyhow!("Failed to generate auth token: {}", e))
}

/// Establish a pooled connection with periodic credential refresh.
async fn establish_connection_pool(
    endpoint: String,
    sdk_config: SdkConfig,
    max_connections: u32,
) -> Result<Pool<Postgres>> {
    let signer = AuthTokenGenerator::new(
        Config::builder()
            .hostname(&endpoint)
            .expires_in(900)
            .build()
            .map_err(|e| anyhow!("Failed to build auth config: {}", e))?,
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

async fn monitor_progress(
    metrics: Metrics,
    pool: Pool<Postgres>,
    workload: Workload,
    progress_bar: ProgressBar,
    usage: watch::Receiver<Usage>,
) {
    let mut interval = time::interval(Duration::from_secs(5));
    let interval_secs = 5.0;

    let mut prev_usage = (*usage.borrow()).clone();

    loop {
        interval.tick().await;

        // Get TPS and reset counter
        let (completed, latency_histogram) = metrics.get_and_reset();
        let tps = completed as f64 / interval_secs;

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
        let rps = tps * workload.rows_inserted as f64;
        let bps = (rps * workload.per_row_logical_bytes_written as f64) as u64;
        let bps = Byte::from_u64(bps as u64).get_appropriate_unit(byte_unit::UnitType::Binary);
        println!(
            "Throughput: {} transactions/sec, {} rows/sec, {:.2} {}/sec",
            tps as u64,
            rps as u64,
            bps.get_value(),
            bps.get_unit()
        );
        println!("Errors: {} total", m.error_count);
        println!("Pool: {} open, {} idle", pool.size(), pool.num_idle());

        if !m.last_errors.is_empty() {
            println!("Last {} errors:", m.last_errors.len());
            for (i, error) in m.last_errors.iter().enumerate() {
                println!("  {}: {}", i + 1, error);
            }
        }

        let p50 = latency_histogram.value_at_quantile(0.50) as f64;
        let p90 = latency_histogram.value_at_quantile(0.90) as f64;
        let p99 = latency_histogram.value_at_quantile(0.99) as f64;
        let p999 = latency_histogram.value_at_quantile(0.999) as f64;

        println!("Latency (ms):");
        println!("  p50: {:.1}", p50);
        println!("  p90: {:.1}", p90);
        println!("  p99: {:.1}", p99);
        println!("  p99.9: {:.1}", p999);

        if usage.has_changed().unwrap() {
            let latest_usage = usage.borrow().clone();
            let usage_diff = latest_usage - prev_usage;
            print_usage_and_diff(&latest_usage, &usage_diff);
            prev_usage = latest_usage;
        }

        if m.completed_batches >= metrics.total_batches {
            break;
        }
    }
}

fn print_usage(latest_usage: &Usage) {
    println!("================= Usage ==================");
    println!(
        "Total DPUS: {:.2} = ${:.2}",
        latest_usage.dpu_metrics.total, latest_usage.cost_estimate.total_dpus.total,
    );
    println!(
        "    - Compute DPUS: {:.2} = ${:.2}",
        latest_usage.dpu_metrics.compute, latest_usage.cost_estimate.total_dpus.compute,
    );
    println!(
        "    - Read DPUS: {:.2} = ${:.2}",
        latest_usage.dpu_metrics.read, latest_usage.cost_estimate.total_dpus.read,
    );
    println!(
        "    - Write DPUS: {:.2} = ${:.2}",
        latest_usage.dpu_metrics.write, latest_usage.cost_estimate.total_dpus.write,
    );
    println!(
        "Storage: {} = ${:.2}",
        Byte::from_u64(latest_usage.storage_metrics.size_bytes as u64)
            .get_appropriate_unit(byte_unit::UnitType::Decimal),
        latest_usage.cost_estimate.latest_storage.gb_month,
    );
}

fn print_usage_and_diff(latest_usage: &Usage, usage_diff: &Usage) {
    println!("================= Usage ==================");
    println!(
        "Total DPUS: {:.2} (+{:.2}) = ${:.2} (+ ${:.2})",
        latest_usage.dpu_metrics.total,
        usage_diff.dpu_metrics.total,
        latest_usage.cost_estimate.total_dpus.total,
        usage_diff.cost_estimate.total_dpus.total,
    );
    println!(
        "    - Compute DPUS: {:.2} (+{:.2}) = ${:.2} (+ ${:.2})",
        latest_usage.dpu_metrics.compute,
        usage_diff.dpu_metrics.compute,
        latest_usage.cost_estimate.total_dpus.compute,
        usage_diff.cost_estimate.total_dpus.compute,
    );
    println!(
        "    - Read DPUS: {:.2} (+{:.2}) = ${:.2} (+ ${:.2})",
        latest_usage.dpu_metrics.read,
        usage_diff.dpu_metrics.read,
        latest_usage.cost_estimate.total_dpus.read,
        usage_diff.cost_estimate.total_dpus.read,
    );
    println!(
        "    - Write DPUS: {:.2} (+{:.2}) = ${:.2} (+ ${:.2})",
        latest_usage.dpu_metrics.write,
        usage_diff.dpu_metrics.write,
        latest_usage.cost_estimate.total_dpus.write,
        usage_diff.cost_estimate.total_dpus.write,
    );
    println!(
        "Storage: {} (+{}) = ${:.2} (+ ${:.2})",
        Byte::from_u64(latest_usage.storage_metrics.size_bytes as u64)
            .get_appropriate_unit(byte_unit::UnitType::Decimal),
        Byte::from_u64(usage_diff.storage_metrics.size_bytes as u64)
            .get_appropriate_unit(byte_unit::UnitType::Decimal),
        latest_usage.cost_estimate.latest_storage.gb_month,
        usage_diff.cost_estimate.latest_storage.gb_month,
    );
}

async fn run_load_generator(args: RunArgs) -> Result<()> {
    let sdk_config = args.sdk_config().await;

    let workloads = workload::load_all(args.rows);
    let workload = workloads
        .get(&args.workload)
        .ok_or_else(|| anyhow!("unknown workload"))?;

    println!("Establishing connection pool...");
    let pool = establish_connection_pool(
        args.endpoint(&sdk_config)?,
        sdk_config.clone(),
        args.concurrency,
    )
    .await?;
    println!("Connection pool established successfully");

    sqlx::query(&workload.setup).execute(&pool).await?;

    if args.rows == 0 {
        println!("setup complete, no rows requested");
        return Ok(());
    }

    let cal = UsageCalculator::new(args.identifier, &sdk_config);
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
    let cost_estimate = usage::calculate_costs(&dpu_metrics, &storage_metrics);
    let initial_usage = Usage {
        dpu_metrics,
        storage_metrics,
        cost_estimate,
    };
    print_usage(&initial_usage);

    let (tx, rx) = watch::channel(initial_usage.clone());

    let usage_task = tokio::spawn(async move {
        loop {
            if let Ok(it) = cal.dpus_this_month().await {
                tx.send_if_modified(move |usage| usage.set_dpu_metrics(it));
            }

            if let Ok(it) = cal.current_storage_usage().await {
                tx.send_if_modified(move |usage| usage.set_storage_metrics(it));
            }

            sleep(Duration::from_secs(30)).await;
        }
    });

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
    let _workload = workload.clone();
    let watch_usage = rx.clone();
    let monitor_handle = tokio::spawn(async move {
        monitor_progress(
            monitor_metrics,
            _pool,
            _workload,
            monitor_progress_bar,
            watch_usage,
        )
        .await;
    });

    let mut set = JoinSet::new();

    for i in 0..args.batches {
        while set.len() >= args.concurrency as usize {
            set.join_next().await;
        }
        set.spawn(execute_batch_with_retry(
            pool.clone(),
            workload.single_statement.clone(),
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

    monitor_handle.abort();

    println!("waiting for final usage");
    sleep(Duration::from_secs(90)).await;
    let final_usage = rx.borrow().clone();
    print_usage_and_diff(&initial_usage, &final_usage);

    usage_task.abort();

    // Close the connection pool
    pool.close().await;

    Ok(())
}

async fn print_cluster_usage(args: UsageArgs) -> anyhow::Result<()> {
    let sdk_config = args.sdk_config().await;
    let cal = UsageCalculator::new(args.identifier.clone(), &sdk_config);

    println!(
        "Fetching usage information for cluster: {}",
        args.identifier
    );

    let dpu_metrics = cal.dpus_this_month().await?;
    let storage_metrics = cal.current_storage_usage().await?;
    let cost_estimate = usage::calculate_costs(&dpu_metrics, &storage_metrics);
    let usage = Usage {
        dpu_metrics,
        storage_metrics,
        cost_estimate,
    };

    print_usage(&usage);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Run(run_args) => run_load_generator(run_args).await?,
        Commands::Usage(print_args) => print_cluster_usage(print_args).await?,
    }

    Ok(())
}
