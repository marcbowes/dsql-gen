//! TUI components for the DSQL load generator monitor
//!
//! This module contains all the UI components for the terminal user interface,
//! organized as separate modules for each section of the UI.

use std::{collections::VecDeque, time::SystemTime};

use hdrhistogram::Histogram;
use ratatui::prelude::*;

pub mod errors;
pub mod latency;
pub mod performance;
pub mod progress;
pub mod usage_cost;
pub mod workload_modal;

use errors::{ErrorState, ErrorWidget};
use latency::{LatencyState, LatencyWidget};
use performance::{PerformanceState, PerformanceWidget};
use progress::ProgressWidget;
use usage_cost::UsageCostWidget;
use workload_modal::{WorkloadModalState, WorkloadModalWidget};

use crate::{runner::WorkloadRunner, usage::Usage};

/// The main model containing all UI state
pub struct Model {
    pub runner: WorkloadRunner,
    pub metrics: Metrics,
    pub progress_pct: f64,
    pub initial_usage: Usage,
    pub latest_usage: Usage,
    pub usage_diff_from_start: Usage,
    pub latency_state: LatencyState,
    pub performance_state: PerformanceState,
    pub error_state: ErrorState,
    pub workload_modal_state: WorkloadModalState,
}

impl Model {
    pub fn new(runner: WorkloadRunner) -> Self {
        let pool = runner.pool.clone();

        Self {
            runner,
            metrics: Metrics::default(),
            progress_pct: 0.0,
            initial_usage: Usage::default(),
            latest_usage: Usage::default(),
            usage_diff_from_start: Usage::default(),
            latency_state: LatencyState::new(),
            performance_state: PerformanceState::new(pool),
            error_state: ErrorState::new(),
            workload_modal_state: WorkloadModalState::new(),
        }
    }
}

#[derive(Clone)]
pub struct ErrorEntry {
    pub timestamp: SystemTime,
    pub message: String,
}

#[derive(Clone)]
pub struct Metrics {
    pub completed_batches: usize,
    pub completed_since_last_tick: usize,
    pub error_count: usize,
    pub errors_since_last_tick: usize,
    pub last_errors: VecDeque<ErrorEntry>,
    pub latency_histogram: Histogram<u64>,
}

impl Default for Metrics {
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

impl Metrics {
    pub fn record_success(&mut self, duration_ms: u64) {
        self.completed_batches += 1;
        self.completed_since_last_tick += 1;

        self.latency_histogram
            .record(duration_ms)
            .expect("histogram is correctly configured");
    }

    pub fn record_error(&mut self, error: String) {
        self.error_count += 1;
        self.errors_since_last_tick += 1;

        if self.last_errors.len() >= 5 {
            self.last_errors.pop_front();
        }
        self.last_errors.push_back(ErrorEntry {
            timestamp: SystemTime::now(),
            message: error,
        });
    }

    pub fn get_and_reset(&mut self) -> (usize, usize, Histogram<u64>) {
        let completed = self.completed_since_last_tick;
        let errors = self.errors_since_last_tick;
        self.completed_since_last_tick = 0;
        self.errors_since_last_tick = 0;
        let histogram = self.latency_histogram.clone();
        self.latency_histogram.reset();
        (completed, errors, histogram)
    }
}

/// The main UI function that renders all components
pub fn draw(f: &mut Frame, model: &Model) {
    // Calculate available height after fixed sections
    let total_height = f.area().height.saturating_sub(2); // Account for margin
    let fixed_height = 3 + 12 + 12 + 1; // Progress + Errors + Usage & Cost + Quit message
    let remaining_height = total_height.saturating_sub(fixed_height);

    // Split remaining height between Performance and Latency
    let perf_latency_height = remaining_height / 2;

    // Error if not enough space for charts
    if perf_latency_height < 12 {
        // Render error message if terminal too small
        let error_msg = format!(
            "Terminal too small! Need at least {} lines, have {}",
            fixed_height + 24, // 24 = 12 lines each for perf and latency
            total_height
        );
        let error_para = ratatui::widgets::Paragraph::new(error_msg)
            .block(
                ratatui::widgets::Block::default()
                    .title("Error")
                    .borders(ratatui::widgets::Borders::ALL),
            )
            .style(Style::default().fg(Color::Red));
        error_para.render(f.area(), f.buffer_mut());
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3),                   // Progress
            Constraint::Length(perf_latency_height), // Performance (dynamic)
            Constraint::Length(perf_latency_height), // Latency (dynamic)
            Constraint::Length(12),                  // Errors
            Constraint::Length(12),                  // Usage & Cost
            Constraint::Length(1),                   // Quit message
        ])
        .split(f.area());

    // Render each component
    f.render_widget(ProgressWidget::new(model), chunks[0]);

    // Use the new stateful performance widget with chart
    let performance_widget = PerformanceWidget::new(&model.performance_state);
    performance_widget.render_with_chart(chunks[1], f.buffer_mut());

    // Use the stateful latency widget with chart
    let latency_widget = LatencyWidget::new(&model.latency_state);
    latency_widget.render_with_chart(chunks[2], f.buffer_mut());

    // Use the new stateful error widget with chart
    let error_widget = ErrorWidget::new(&model.error_state);
    error_widget.render_with_chart(chunks[3], f.buffer_mut());

    f.render_widget(UsageCostWidget::new(model), chunks[4]);

    // Render quit message at the bottom without borders
    let quit_msg = ratatui::widgets::Paragraph::new(
        "Press 'q' or ESC to quit", // | Press 'w' for workload settings
    )
    .style(
        Style::default()
            .fg(Color::Gray)
            .add_modifier(Modifier::ITALIC),
    )
    .alignment(ratatui::layout::Alignment::Center);
    f.render_widget(quit_msg, chunks[5]);

    // Render the workload modal if visible
    if model.workload_modal_state.visible {
        let workload_modal = WorkloadModalWidget::new(&model.workload_modal_state);
        workload_modal.render(f.area(), f.buffer_mut(), &model.runner);
    }
}
