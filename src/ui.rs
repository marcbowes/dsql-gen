use anyhow::Result;
use byte_unit::Byte;
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::prelude::*;
use std::io::stdout;
use std::time::{Duration, Instant};
use tokio::time;

use crate::runner::WorkloadRunner;
use crate::tui::{self, Model};

#[derive(Debug, Clone, PartialEq)]
enum AppState {
    Running,
    Stopping,
    WaitingForFinalStats,
}

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
        let mut app_state = AppState::Running;
        let mut load_gen_stopped = false;
        let mut final_stats_start: Option<Instant> = None;

        loop {
            // Check for quit key
            if event::poll(Duration::from_millis(0))? {
                if let Event::Key(key) = event::read()? {
                    // Handle quit keys: 'q', ESC, or Ctrl-C
                    let should_quit = key.code == KeyCode::Char('q') 
                        || key.code == KeyCode::Esc
                        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL));
                    
                    if should_quit {
                        match app_state {
                            AppState::Running => {
                                // First quit: stop the load generator but keep TUI running
                                app_state = AppState::Stopping;
                                runner.stop_workload();
                                load_gen_stopped = true;
                            }
                            AppState::Stopping | AppState::WaitingForFinalStats => {
                                // Second quit: exit immediately
                                break;
                            }
                        }
                    }
                }
            }

            interval.tick().await;

            // Only get metrics if load gen is still running
            if !load_gen_stopped {
                // Get TPS and reset counter
                let (completed, errors, latency_histogram) = runner.metrics.get_and_reset();
                let tps = completed as f64 / interval_secs;
                let eps = errors as f64 / interval_secs; // errors per second
                
                // Update model with new data
                model.metrics = runner.metrics.read();
                model.progress_pct = (model.metrics.completed_batches as f64 / model.total_batches as f64) * 100.0;
                
                // Calculate performance metrics
                let rps = tps * runner.workload.rows_inserted as f64;
                let bps = (rps * runner.workload.per_row_logical_bytes_written as f64) as u64;
                let bps_formatted = Byte::from_u64(bps).get_appropriate_unit(byte_unit::UnitType::Binary);
                let bps_formatted_str = format!("{:.2} {}/sec", bps_formatted.get_value(), bps_formatted.get_unit());
                let pool_size = runner.pool.size() as usize;
                let pool_idle = runner.pool.num_idle() as usize;
                
                // Update widget states
                model.performance_state.update(tps, rps, bps_formatted_str, pool_size, pool_idle);
                model.latency_state.update(latency_histogram);
                model.error_state.update(eps);
            } else {
                // Load gen stopped, but keep updating usage stats
                model.metrics = runner.metrics.read();
            }

            model.latest_usage = runner.usage_rx.borrow().clone();
            model.usage_diff = if runner.usage_rx.has_changed().unwrap() {
                let diff = model.latest_usage.clone() - prev_usage.clone();
                prev_usage = model.latest_usage.clone();
                diff
            } else {
                model.latest_usage.clone() - model.latest_usage.clone() // Zero diff
            };
            model.usage_diff_from_start = model.latest_usage.clone() - runner.initial_usage.clone();

            // Update app state based on completion
            if !load_gen_stopped && model.metrics.completed_batches >= model.total_batches {
                // Load gen completed naturally
                load_gen_stopped = true;
                app_state = AppState::WaitingForFinalStats;
                final_stats_start = Some(Instant::now());
            } else if load_gen_stopped && app_state == AppState::Stopping {
                // Transition to waiting for final stats
                app_state = AppState::WaitingForFinalStats;
                final_stats_start = Some(Instant::now());
            }
            
            // Check if final stats collection has timed out (after 2 minutes)
            if let Some(start_time) = final_stats_start {
                if start_time.elapsed() > Duration::from_secs(120) {
                    // Final stats collection timed out, exit
                    break;
                }
            }

            // Draw UI using the new component architecture
            self.terminal.draw(|f| {
                tui::draw(f, &model);
                
                // Show stopping state overlay
                match app_state {
                    AppState::Stopping => {
                        let stopping_msg = ratatui::widgets::Paragraph::new("Stopping load generator... Press quit again to exit immediately.")
                            .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
                            .alignment(ratatui::layout::Alignment::Center);
                        
                        // Render at the very bottom (below quit message)
                        let area = f.area();
                        let stopping_area = ratatui::layout::Rect {
                            x: area.x,
                            y: area.height.saturating_sub(1),
                            width: area.width,
                            height: 1,
                        };
                        f.render_widget(stopping_msg, stopping_area);
                    }
                    AppState::WaitingForFinalStats => {
                        let waiting_msg = ratatui::widgets::Paragraph::new("Collecting final usage stats... Press quit to exit.")
                            .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
                            .alignment(ratatui::layout::Alignment::Center);
                        
                        // Render at the very bottom (below quit message)
                        let area = f.area();
                        let waiting_area = ratatui::layout::Rect {
                            x: area.x,
                            y: area.height.saturating_sub(1),
                            width: area.width,
                            height: 1,
                        };
                        f.render_widget(waiting_msg, waiting_area);
                    }
                    AppState::Running => {
                        // No overlay needed for running state
                    }
                }
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
