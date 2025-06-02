//! TUI components for the DSQL load generator monitor
//!
//! This module contains all the UI components for the terminal user interface,
//! organized as separate modules for each section of the UI.

use ratatui::prelude::*;

pub mod errors;
pub mod latency;
pub mod performance;
pub mod progress;
pub mod usage_cost;

use errors::{ErrorWidget, ErrorState};
use latency::{LatencyWidget, LatencyState};
use performance::{PerformanceWidget, PerformanceState};
use progress::ProgressWidget;
use usage_cost::UsageCostWidget;

use crate::runner::MetricsInner;
use crate::usage::Usage;

/// The main model containing all UI state
#[derive(Clone)]
pub struct Model {
    pub metrics: MetricsInner,
    pub total_batches: usize,
    pub progress_pct: f64,
    pub latest_usage: Usage,
    pub usage_diff: Usage,
    pub usage_diff_from_start: Usage,
    pub latency_state: LatencyState,
    pub performance_state: PerformanceState,
    pub error_state: ErrorState,
}

impl Model {
    /// Create a new model with default values
    pub fn new(total_batches: usize) -> Self {
        Self {
            metrics: MetricsInner::default(),
            total_batches,
            progress_pct: 0.0,
            latest_usage: Usage::default(),
            usage_diff: Usage::default(),
            usage_diff_from_start: Usage::default(),
            latency_state: LatencyState::new(),
            performance_state: PerformanceState::new(),
            error_state: ErrorState::new(),
        }
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
            .block(ratatui::widgets::Block::default().title("Error").borders(ratatui::widgets::Borders::ALL))
            .style(Style::default().fg(Color::Red));
        error_para.render(f.area(), f.buffer_mut());
        return;
    }
    
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3),                    // Progress
            Constraint::Length(perf_latency_height),  // Performance (dynamic)
            Constraint::Length(perf_latency_height),  // Latency (dynamic)
            Constraint::Length(12),                   // Errors
            Constraint::Length(12),                   // Usage & Cost
            Constraint::Length(1),                    // Quit message
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
    let error_widget = ErrorWidget::new(&model.error_state, &model.metrics);
    error_widget.render_with_chart(chunks[3], f.buffer_mut());
    
    f.render_widget(UsageCostWidget::new(model), chunks[4]);
    
    // Render quit message at the bottom without borders
    let quit_msg = ratatui::widgets::Paragraph::new("Press 'q' or ESC to quit")
        .style(Style::default().fg(Color::Gray).add_modifier(Modifier::ITALIC))
        .alignment(ratatui::layout::Alignment::Center);
    f.render_widget(quit_msg, chunks[5]);
}
