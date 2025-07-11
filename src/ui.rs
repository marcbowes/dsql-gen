use std::io::stdout;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::{DateTime, Utc};
use crossterm::{
    ExecutableCommand,
    event::{self, Event, KeyCode, KeyModifiers},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use hdrhistogram::Histogram;
use ratatui::prelude::*;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time;

use crate::events::{EventListener, Message, QueryResult};
use crate::runner::WorkloadRunner;
use crate::tui::{self, Model};

pub struct MonitorUI {
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
    listener: EventListener,
}

impl MonitorUI {
    pub fn new(rx: mpsc::Receiver<Message>) -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = stdout();
        stdout.execute(EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        let listener = EventListener::new(rx);

        Ok(Self { terminal, listener })
    }

    pub async fn run(&mut self, runner: WorkloadRunner) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(1));

        // Setup state
        let mut model = Model::new(runner);
        let running = model.runner.spawn();

        loop {
            // Check for key events
            if event::poll(Duration::from_millis(0))? {
                if let Event::Key(key) = event::read()? {
                    match key.code {
                        _ => {
                            // Handle quit keys: 'q', ESC, or Ctrl-C
                            let should_quit = key.code == KeyCode::Char('q')
                                || key.code == KeyCode::Esc
                                || (key.code == KeyCode::Char('c')
                                    && key.modifiers.contains(KeyModifiers::CONTROL));

                            if should_quit {
                                running.abort();
                                break;
                            }
                        }
                    }
                }
            }

            // Wait for tick
            interval.tick().await;

            // Process available events
            self.listener.process_available_messages(&mut model).await;

            if running.is_finished() || self.listener.completed {
                break;
            }

            // Draw UI using the component architecture
            self.terminal.draw(|f| {
                tui::draw(f, &mut model);
            })?;

            // Don't auto-exit when load gen completes - wait for user input or final stats completion
        }

        Ok(())
    }
}

impl Drop for MonitorUI {
    fn drop(&mut self) {
        // Cleanup terminal
        let _ = disable_raw_mode();
        let _ = self.terminal.backend_mut().execute(LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

pub struct HeadlessMonitor {
    listener: EventListener,
    latency_histogram: Histogram<u64>,
    completed_batches: usize,
    error_count: usize,
    start_time: Instant,
    start_time_utc: DateTime<Utc>,
    total_batches: usize,
    errors: Vec<String>,
    concurrency: usize,
    rows_per_transaction: Option<usize>,
    workload_name: String,
    always_rollback: bool,
}

impl HeadlessMonitor {
    pub fn new(
        rx: mpsc::Receiver<Message>,
        concurrency: usize,
        rows_per_transaction: Option<usize>,
        workload_name: String,
        always_rollback: bool,
    ) -> Self {
        let now = Instant::now();
        let now_utc = Utc::now();
        Self {
            listener: EventListener::new(rx),
            latency_histogram: Histogram::<u64>::new_with_bounds(1, 60_000 * 10, 3).unwrap(),
            completed_batches: 0,
            error_count: 0,
            start_time: now,
            start_time_utc: now_utc,
            total_batches: 0,
            errors: Vec::new(),
            concurrency,
            rows_per_transaction,
            workload_name,
            always_rollback,
        }
    }

    pub async fn run(&mut self, runner: WorkloadRunner) -> Result<()> {
        self.total_batches = runner.batches();
        let mut running = runner.spawn();

        // Output initial configuration as JSON
        let start_config = json!({
            "phase": "start",
            "start_time_utc": self.start_time_utc.to_rfc3339(),
            "workload": self.workload_name,
            "total_batches": self.total_batches,
            "concurrency": self.concurrency,
            "always_rollback": self.always_rollback,
            "rows_per_transaction": self.rows_per_transaction,
        });
        println!("{}", serde_json::to_string(&start_config)?);

        // Process messages until completion
        loop {
            tokio::select! {
                // Wait for messages
                message = self.listener.rx.recv() => {
                    match message {
                        Some(msg) => self.process_message(msg),
                        None => break, // Channel closed
                    }
                }
                // Check if runner finished
                result = &mut running => {
                    // Runner completed, process any remaining messages quickly
                    let _ = result; // Consume the result
                    break;
                }
            }

            // Break if we've received completion signal
            if self.listener.completed {
                // Ensure runner is done
                let _ = running.await;
                break;
            }
        }

        // Process any remaining messages with a timeout to avoid hanging
        for _ in 0..50 {
            // Max 50 attempts to drain remaining messages
            match self.listener.rx.try_recv() {
                Ok(msg) => self.process_message(msg),
                Err(_) => break, // No more messages
            }
        }

        // Explicitly drop the runner to cleanup connection pool
        drop(runner);

        Ok(())
    }

    fn process_message(&mut self, message: Message) {
        match message {
            Message::QueryResult(result) => match result {
                QueryResult::Ok(ok) => {
                    self.latency_histogram
                        .record(ok.duration.as_millis() as u64)
                        .unwrap_or(());
                    self.completed_batches += 1;
                }
                QueryResult::Err(err) => {
                    self.error_count += 1;
                    self.errors.push(err.msg);
                }
            },
            Message::WorkloadComplete => {
                self.listener.completed = true;
            }
            _ => {
                // Ignore other message types in headless mode
            }
        }
    }

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
            println!("{}", json_string);
        }
    }
}
