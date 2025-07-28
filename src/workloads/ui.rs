use std::{
    fmt::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use chrono::{DateTime, Utc};
use console::Emoji;
use hdrhistogram::Histogram;
use indexmap::IndexMap;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressState, ProgressStyle};
use serde_json::json;
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};

use crate::{
    cli::{WorkloadArgs, WorkloadCommands},
    events::{Message, QueryResult},
    runner::WorkloadRunner,
};

pub struct Ui {
    state: Arc<State>,
    events: JoinHandle<()>,
}

impl Drop for Ui {
    fn drop(&mut self) {
        self.events.abort();
    }
}

struct State {
    pool: ProgressBar,
    setup: SetupState,
    workload: Mutex<WorkloadState>,
}

impl Ui {
    pub fn new(rx: mpsc::Receiver<Message>) -> Self {
        let state = Arc::new(State {
            pool: ProgressBar::hidden(),
            setup: SetupState::default(),
            workload: Mutex::new(WorkloadState::default()),
        });

        let events = tokio::spawn(process_events(rx, state.clone()));

        Ui { state, events }
    }

    pub fn on_before_pool_create(&mut self) {
        let pool = &self.state.pool;
        pool.set_draw_target(ProgressDrawTarget::stderr());
        pool.set_style(spinner_template());
        pool.set_message("connecting");
        pool.set_prefix(phased(1));
        pool.enable_steady_tick(Duration::from_millis(100));
    }

    pub fn on_after_pool_create(&mut self) {
        let pool = &self.state.pool;
        self.state.pool.disable_steady_tick();
        pool.set_message("connected");
        pool.finish_and_clear();
    }

    pub fn on_before_setup(&mut self) {
        let SetupState { schema, load, .. } = &self.state.setup;
        schema.set_draw_target(ProgressDrawTarget::stderr());
        load.set_draw_target(ProgressDrawTarget::stderr());
    }

    pub fn on_after_setup(&mut self) {
        let SetupState {
            schema,
            schemas,
            load,
            loads,
        } = &self.state.setup;
        let mut schemas = schemas.lock().unwrap();
        for (_, pb) in schemas.iter_mut() {
            pb.disable_steady_tick();
            pb.finish_and_clear();
        }
        _ = schema.clear();

        let mut loads = loads.lock().unwrap();
        for (_, pb) in loads.iter_mut() {
            pb.disable_steady_tick();
            pb.finish_and_clear();
        }
        _ = load.clear();
    }

    pub async fn run(self, runner: WorkloadRunner, args: WorkloadArgs) -> Result<()> {
        let rows_per_tx = match &args.workload {
            WorkloadCommands::Tiny(tiny_args) => Some(tiny_args.rows_per_transaction),
            WorkloadCommands::OneKib(onekib_args) => Some(onekib_args.rows_per_transaction),
            WorkloadCommands::Counter(_) => None,
            WorkloadCommands::Tpcb(_) => Some(4), // FIXME: Make this automatically correctly
        };

        let workload_name = match &args.workload {
            WorkloadCommands::Tiny(_) => "tiny".to_string(),
            WorkloadCommands::OneKib(_) => "onekib".to_string(),
            WorkloadCommands::Counter(_) => "counter".to_string(),
            WorkloadCommands::Tpcb(_) => "tcpb".to_string(),
        };

        {
            let mut wl = self.state.workload.lock().unwrap();
            wl.starting_now();
            wl.multi.set_draw_target(ProgressDrawTarget::stderr());
            wl.pb.set_length(runner.batches() as u64);
            wl.pb.enable_steady_tick(Duration::from_millis(100));

            // Output initial configuration as JSON
            let start_config = json!({
                "phase": "start",
                "start_time_utc": wl.start_time_utc.to_rfc3339(),
                "workload": workload_name,
                "total_batches": runner.batches(),
                "concurrency": runner.concurrency(),
                "always_rollback": args.always_rollback,
                "rows_per_transaction": rows_per_tx,
            });
            _ = wl.multi.println(serde_json::to_string(&start_config)?);
        }

        let running = runner.spawn();
        running.await??;

        let wl = self.state.workload.lock().unwrap();
        wl.pb.disable_steady_tick();
        wl.print_final_stats();

        Ok(())
    }
}

struct SetupState {
    schema: MultiProgress,
    schemas: Mutex<IndexMap<String, ProgressBar>>,
    load: MultiProgress,
    loads: Mutex<IndexMap<String, ProgressBar>>,
}

impl Default for SetupState {
    fn default() -> Self {
        let schema = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let schemas = Mutex::new(IndexMap::new());
        let load = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let loads = Mutex::new(IndexMap::new());

        Self {
            schema,
            schemas,
            load,
            loads,
        }
    }
}

impl SetupState {
    fn schema_progressbar(&self, name: impl Into<String>) -> ProgressBar {
        let mut schemas = self.loads.lock().expect("never poisoned");

        match schemas.entry(name.into()) {
            indexmap::map::Entry::Occupied(occupied_entry) => occupied_entry.get().clone(),
            indexmap::map::Entry::Vacant(vacant_entry) => {
                let pb = self.schema.add(ProgressBar::no_length());
                pb.set_style(spinner_template());
                vacant_entry.insert(pb.clone());
                pb
            }
        }
    }

    fn load_progressbar(&self, name: impl Into<String>) -> ProgressBar {
        let mut loads = self.loads.lock().expect("never poisoned");

        match loads.entry(name.into()) {
            indexmap::map::Entry::Occupied(occupied_entry) => occupied_entry.get().clone(),
            indexmap::map::Entry::Vacant(vacant_entry) => {
                let pb = self.load.add(ProgressBar::new(0));
                pb.set_style(
                    ProgressStyle::with_template(
                        "{prefix:.bold.dim} {spinner} {wide_msg} [{elapsed_precise}] [{bar:.cyan/blue}] {pos}/{len} rows ({eta})",
                    )
                    .unwrap()
                    .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                         write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
                    })
                    .progress_chars("#>-")
                );
                vacant_entry.insert(pb.clone());
                pb
            }
        }
    }
}

struct WorkloadState {
    multi: MultiProgress,
    pb: ProgressBar,
    latency_histogram: Histogram<u64>,
    completed_batches: usize,
    error_count: usize,
    start_time: Instant,
    start_time_utc: DateTime<Utc>,
    errors: Vec<String>,
}

impl Default for WorkloadState {
    fn default() -> Self {
        let multi = MultiProgress::with_draw_target(ProgressDrawTarget::hidden());
        let pb = multi.add(ProgressBar::hidden());
        pb.set_style(
            ProgressStyle::with_template(
                "{prefix:.bold.dim} {spinner} {wide_msg} [{elapsed_precise}] [{bar:.cyan/blue}] {pos}/{len} rows ({per_sec}), eta {eta}",
            )
                .unwrap()
                .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
                    write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
                })
                .progress_chars("#>-")
        );

        let now = Instant::now();
        let now_utc = Utc::now();
        Self {
            multi,
            pb,
            latency_histogram: Histogram::<u64>::new_with_bounds(1, 60_000 * 10, 3).unwrap(),
            completed_batches: 0,
            error_count: 0,
            start_time: now,
            start_time_utc: now_utc,
            errors: Vec::new(),
        }
    }
}

impl WorkloadState {
    fn starting_now(&mut self) {
        self.start_time = Instant::now();
        self.start_time_utc = Utc::now();
    }
}

impl WorkloadState {
    pub fn print_final_stats(&self) {
        let duration = self.start_time.elapsed();
        let duration_secs = duration.as_secs_f64();
        let end_time_utc = Utc::now();

        let mut results = json!({
            "phase": "results",
            "end_time_utc": end_time_utc.to_rfc3339(),
            "duration_s": format!("{:.3}", duration_secs),
            "completed_batches": self.completed_batches,
            "error_count": self.error_count,
        });

        if self.completed_batches > 0 {
            results["throughput_batches_per_sec"] = json!(format!(
                "{:.1}",
                self.completed_batches as f64 / duration_secs
            ));

            if self.latency_histogram.len() > 0 {
                let latency_stats = json!({
                    "p50_ms": format!("{:.1}", self.latency_histogram.value_at_quantile(0.5) as f64),
                    "p95_ms": format!("{:.1}", self.latency_histogram.value_at_quantile(0.95) as f64),
                    "p99_ms": format!("{:.1}", self.latency_histogram.value_at_quantile(0.99) as f64),
                    "p999_ms": format!("{:.1}", self.latency_histogram.value_at_quantile(0.999) as f64),
                    "max_ms": format!("{:.1}", self.latency_histogram.max() as f64),
                    "mean_ms": format!("{:.1}", self.latency_histogram.mean()),
                    "stddev_ms": format!("{:.1}", self.latency_histogram.stdev()),
                });
                results["latency"] = latency_stats;
            }
        }

        // Include errors in the results
        if self.error_count > 0 && !self.errors.is_empty() {
            let errors: Vec<String> = self
                .errors
                .iter()
                .take(5)
                .map(|e| e.replace('\t', " ").replace('\n', " "))
                .collect();
            results["errors"] = json!(errors);
        }

        // Output the final results as JSON
        if let Ok(json_string) = serde_json::to_string(&results) {
            _ = self.multi.println(json_string);
        }
    }
}

fn phased(phase: usize) -> String {
    let icon = match phase {
        1 => Emoji("🔗", ""),
        2 => Emoji("🚧", ""),
        3 => Emoji("💾", ""),
        _ => unimplemented!(),
    };

    format!("{icon}")
}

fn spinner_template() -> ProgressStyle {
    ProgressStyle::with_template("{prefix:.bold.dim} {spinner} {wide_msg}").unwrap()
}

async fn process_events(mut rx: mpsc::Receiver<Message>, state: Arc<State>) {
    while let Some(event) = rx.recv().await {
        match event {
            Message::QueryResult(result) => match result {
                QueryResult::Ok(ok) => {
                    let mut wl = state.workload.lock().unwrap();
                    wl.latency_histogram
                        .record(ok.duration.as_millis() as u64)
                        .unwrap_or(());
                    wl.completed_batches += 1;
                    wl.pb.inc(1);
                }
                QueryResult::Err(err) => {
                    let mut wl = state.workload.lock().unwrap();
                    wl.error_count += 1;
                    wl.errors.push(err.msg);
                }
            },
            Message::InitialUsage(_usage) => {}
            Message::UsageUpdated(_usage) => {}
            Message::TableDropping(t) => {
                let pb = &state.setup.schema_progressbar(&t);
                pb.enable_steady_tick(Duration::from_millis(100));
                pb.set_prefix(phased(2));
                pb.set_message(format!("dropping: {t}"));
            }
            Message::TableDropped(t) => {
                let pb = &state.setup.schema_progressbar(&t);
                pb.disable_steady_tick();
                pb.set_message(format!("dropped: {t}"));
            }
            Message::TableCreating(t) => {
                let pb = &state.setup.schema_progressbar(&t);
                pb.enable_steady_tick(Duration::from_millis(100));
                pb.set_prefix(phased(2));
                pb.set_message(format!("creating table: {t}"));
            }
            Message::TableCreated(t) => {
                let pb = &state.setup.schema_progressbar(&t);
                pb.disable_steady_tick();
                pb.set_message(format!("created table: {t}"));
            }
            Message::TableLoading(t, rows) => {
                let pb = &state.setup.load_progressbar(&t);
                pb.reset();
                pb.enable_steady_tick(Duration::from_millis(100));
                pb.set_prefix(phased(3));
                pb.set_length(rows as u64);
                pb.set_message(format!("loading: {t}"));
            }
            Message::TableLoaded(t, rows) => {
                let pb = &state.setup.load_progressbar(&t);
                pb.inc(rows as u64);

                if Some(pb.position()) == pb.length() {
                    pb.disable_steady_tick();
                    pb.set_message(format!("loaded: {t}"));
                }
            }
            Message::LoadComplete => {
                let load = &state.setup.load;
                _ = load.clear();
            }
            Message::WorkloadComplete => {
                let wl = state.workload.lock().unwrap();
                wl.pb.finish_with_message("workload complete");
            }
            Message::PoolTelemetry(telemetry) => match telemetry {
                crate::pool::Telemetry::Connected(_) => {}
                crate::pool::Telemetry::Disconnected(_) => {}
                crate::pool::Telemetry::Err { .. } => {}
            },
        }
    }
}
