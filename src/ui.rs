use anyhow::Result;
use byte_unit::Byte;
use crossterm::{
    event::{self, Event, KeyCode},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::prelude::*;
use std::io::stdout;
use std::time::Duration;
use tokio::time;

use crate::runner::WorkloadRunner;
use crate::tui::{self, Model};

pub struct MonitorUI {
    terminal: Terminal<CrosstermBackend<std::io::Stdout>>,
}

impl MonitorUI {
    pub fn new() -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = stdout();
        stdout.execute(EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let terminal = Terminal::new(backend)?;
        
        Ok(Self { terminal })
    }

    pub async fn run(&mut self, runner: &WorkloadRunner) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(1));
        let interval_secs = 1.0;

        let mut model = Model::new(runner.metrics.total_batches);
        let mut prev_usage = (*runner.usage_rx.borrow()).clone();

        loop {
            // Check for quit key
            if event::poll(Duration::from_millis(0))? {
                if let Event::Key(key) = event::read()? {
                    if key.code == KeyCode::Char('q') || key.code == KeyCode::Esc {
                        break;
                    }
                }
            }

            interval.tick().await;

            // Get TPS and reset counter
            let (completed, latency_histogram) = runner.metrics.get_and_reset();
            let tps = completed as f64 / interval_secs;
            
            // Update model with new data
            model.tps = tps;
            model.tps_history.push_back(tps);
            if model.tps_history.len() > 300 {  // 5 minutes at 1s intervals
                model.tps_history.pop_front();
            }

            model.metrics = runner.metrics.read();
            model.progress_pct = (model.metrics.completed_batches as f64 / model.total_batches as f64) * 100.0;
            model.rps = tps * runner.workload.rows_inserted as f64;
            let bps = (model.rps * runner.workload.per_row_logical_bytes_written as f64) as u64;
            let bps_formatted = Byte::from_u64(bps).get_appropriate_unit(byte_unit::UnitType::Binary);
            model.bps_formatted = format!("{:.2} {}/sec", bps_formatted.get_value(), bps_formatted.get_unit());

            // Store the histogram
            model.latest_latency_histogram = latency_histogram.clone();
            
            // Store histogram history for chart
            model.latency_histogram_history.push_back(latency_histogram);
            if model.latency_histogram_history.len() > 300 {
                model.latency_histogram_history.pop_front();
            }

            model.pool_size = runner.pool.size() as usize;
            model.pool_idle = runner.pool.num_idle() as usize;

            model.latest_usage = runner.usage_rx.borrow().clone();
            model.usage_diff = if runner.usage_rx.has_changed().unwrap() {
                let diff = model.latest_usage.clone() - prev_usage.clone();
                prev_usage = model.latest_usage.clone();
                diff
            } else {
                model.latest_usage.clone() - model.latest_usage.clone() // Zero diff
            };
            model.usage_diff_from_start = model.latest_usage.clone() - runner.initial_usage.clone();

            // Draw UI using the new component architecture
            self.terminal.draw(|f| {
                tui::draw(f, &model);
            })?;

            if model.metrics.completed_batches >= model.total_batches {
                break;
            }
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
