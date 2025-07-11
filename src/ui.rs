use std::io::stdout;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::{
    ExecutableCommand,
    event::{self, Event, KeyCode, KeyModifiers},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use hdrhistogram::Histogram;
use ratatui::prelude::*;
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
    total_batches: usize,
    errors: Vec<String>,
}

impl HeadlessMonitor {
    pub fn new(rx: mpsc::Receiver<Message>) -> Self {
        Self {
            listener: EventListener::new(rx),
            latency_histogram: Histogram::<u64>::new_with_bounds(1, 60_000 * 10, 3).unwrap(),
            completed_batches: 0,
            error_count: 0,
            start_time: Instant::now(),
            total_batches: 0,
            errors: Vec::new(),
        }
    }

    pub async fn run(&mut self, runner: WorkloadRunner) -> Result<()> {
        self.total_batches = runner.batches();
        let mut running = runner.spawn();

        println!("Starting workload with {} batches...", self.total_batches);

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

        println!("\n=== Workload Complete! ===");
        println!("Duration: {:.1}s", duration_secs);
        println!("Completed Batches: {}", self.completed_batches);
        println!("Errors: {}", self.error_count);

        if self.completed_batches > 0 {
            println!(
                "Throughput: {:.1} batches/sec",
                self.completed_batches as f64 / duration_secs
            );

            if self.latency_histogram.len() > 0 {
                println!("Latency Percentiles:");
                println!(
                    "  p50:  {:.1}ms",
                    self.latency_histogram.value_at_quantile(0.5) as f64
                );
                println!(
                    "  p95:  {:.1}ms",
                    self.latency_histogram.value_at_quantile(0.95) as f64
                );
                println!(
                    "  p99:  {:.1}ms",
                    self.latency_histogram.value_at_quantile(0.99) as f64
                );
                println!(
                    "  p99.9: {:.1}ms",
                    self.latency_histogram.value_at_quantile(0.999) as f64
                );
                println!("  max:  {:.1}ms", self.latency_histogram.max() as f64);
            }
        }

        if self.error_count > 0 {
            println!("\nRecent Errors:");
            for (i, error) in self.errors.iter().rev().take(5).enumerate() {
                println!("  {}: {}", i + 1, error);
            }
            if self.errors.len() > 5 {
                println!("  ... and {} more errors", self.errors.len() - 5);
            }
        }
    }
}
