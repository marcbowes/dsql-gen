//! Errors display component for the TUI

use std::{
    collections::VecDeque,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ratatui::{
    prelude::*,
    symbols,
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, List, ListItem, Paragraph, Wrap},
};

use crate::history::{
    TimestampedHistory,
    bucketing::{BucketConfig, bucket_data},
};

use super::ErrorEntry;

/// State for the error widget containing its history data
#[derive(Clone)]
pub struct ErrorState {
    pub error_history: TimestampedHistory<f64>, // errors per second
    pub last_errors: VecDeque<ErrorEntry>,
    pub error_count: usize,
}

impl Default for ErrorState {
    fn default() -> Self {
        Self::new()
    }
}

impl ErrorState {
    /// Create a new error state with default values
    pub fn new() -> Self {
        Self {
            error_history: TimestampedHistory::new(Duration::from_secs(300)),
            last_errors: VecDeque::with_capacity(5),
            error_count: 0,
        }
    }

    /// Update the error state with new error rate data
    pub fn update(&mut self, errors_per_second: f64) {
        self.error_history.push(errors_per_second);
    }

    /// Record an error message from a batch failure
    pub fn record_error(&mut self, error_message: String) {
        self.error_count += 1;

        // Add to recent errors list
        if self.last_errors.len() >= 5 {
            self.last_errors.pop_front();
        }

        self.last_errors.push_back(ErrorEntry {
            timestamp: std::time::SystemTime::now(),
            message: error_message,
        });

        // Update error rate for chart
        self.update(1.0); // 1 error this second
    }
}

/// Stateful error widget that can render both simple stats and charts
pub struct ErrorWidget<'a> {
    state: &'a ErrorState,
}

impl<'a> ErrorWidget<'a> {
    /// Create a new error widget with the given state
    pub fn new(state: &'a ErrorState) -> Self {
        Self { state }
    }

    /// Render with split layout for chart integration
    pub fn render_with_chart(&self, area: Rect, buf: &mut Buffer) {
        // Split horizontally: left for error list, right for chart
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(30), // Error list (consistent with performance and latency)
                Constraint::Min(40),    // Error chart
            ])
            .split(area);

        // Render error list on the left
        self.render_error_list(chunks[0], buf);

        // Render chart on the right
        self.render_chart(chunks[1], buf);
    }

    fn render_error_list(&self, area: Rect, buf: &mut Buffer) {
        let mut items: Vec<ListItem> = vec![ListItem::new(format!(
            "Total errors: {}",
            self.state.error_count
        ))];

        if !self.state.last_errors.is_empty() {
            items.push(ListItem::new(""));
            items.push(ListItem::new("Recent errors:"));

            for error in self.state.last_errors.iter() {
                let now = SystemTime::now();
                let elapsed = now
                    .duration_since(error.timestamp)
                    .unwrap_or(Duration::ZERO);
                let time_str = if elapsed.as_secs() < 60 {
                    format!("{}s ago", elapsed.as_secs())
                } else if elapsed.as_secs() < 3600 {
                    format!("{}m ago", elapsed.as_secs() / 60)
                } else {
                    format!("{}h ago", elapsed.as_secs() / 3600)
                };

                let error_msg = if error.message.len() > 40 {
                    format!("{}...", &error.message[..40])
                } else {
                    error.message.clone()
                };

                items.push(ListItem::new(format!("[{}] {}", time_str, error_msg)));
            }
        }

        let list = List::new(items).block(
            Block::default()
                .title("Errors & Info")
                .borders(Borders::ALL),
        );

        ratatui::prelude::Widget::render(list, area, buf);
    }

    fn render_chart(&self, area: Rect, buf: &mut Buffer) {
        if self.state.error_history.is_empty() {
            // No data yet, just render empty block
            let block = Block::default()
                .title("Error Rate (5 min)")
                .borders(Borders::ALL);
            block.render(area, buf);
            return;
        }

        // Use bucketing to convert timestamped data to chart data
        let config = BucketConfig::new(Duration::from_secs(1), 300); // 300 seconds = 5 minutes
        let buckets = bucket_data(
            self.state.error_history.data(),
            config,
            |values| {
                // Average the values in each bucket
                if values.is_empty() {
                    0.0
                } else {
                    values.iter().map(|&&v| v).sum::<f64>() / values.len() as f64
                }
            },
            0.0, // default value for empty buckets
        );

        // Prepare data for chart
        let mut error_data = Vec::with_capacity(buckets.len());
        let mut max_errors: f64 = 0.0;

        for (i, &errors_per_sec) in buckets.iter().enumerate() {
            let x = i as f64;
            error_data.push((x, errors_per_sec));
            max_errors = max_errors.max(errors_per_sec);
        }

        // Add some padding to max value, ensure minimum of 1.0 for scale
        max_errors = (max_errors * 1.1).max(1.0);

        // Create dataset
        let datasets = vec![
            Dataset::default()
                .name("Errors/sec")
                .marker(symbols::Marker::Dot)
                .graph_type(GraphType::Line)
                .style(Style::default().fg(Color::Red))
                .data(&error_data),
        ];

        // Calculate X-axis labels (time)
        let x_labels = vec![
            Span::raw("-5m"),
            Span::raw("-4m"),
            Span::raw("-3m"),
            Span::raw("-2m"),
            Span::raw("-1m"),
            Span::raw("now"),
        ];

        // Calculate Y-axis labels
        let y_labels = vec![
            Span::raw("0"),
            Span::raw(format!("{:.1}", max_errors / 2.0)),
            Span::raw(format!("{:.1}", max_errors)),
        ];

        // Create chart
        let chart = Chart::new(datasets)
            .block(
                Block::default()
                    .title("Error Rate (5 min)")
                    .borders(Borders::ALL),
            )
            .x_axis(
                Axis::default()
                    .title("Time")
                    .style(Style::default().fg(Color::Gray))
                    .labels(x_labels)
                    .bounds([0.0, 300.0]),
            )
            .y_axis(
                Axis::default()
                    .title("Errors/sec")
                    .style(Style::default().fg(Color::Gray))
                    .labels(y_labels)
                    .bounds([0.0, max_errors]),
            )
            .hidden_legend_constraints((Constraint::Percentage(50), Constraint::Percentage(50)));

        chart.render(area, buf);
    }
}

impl<'a> Widget for ErrorWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let mut error_text = format!("Total errors: {}\n", self.state.error_count);

        if !self.state.last_errors.is_empty() {
            error_text.push_str("\nRecent errors:\n");
            for (i, error) in self.state.last_errors.iter().enumerate() {
                let elapsed = error
                    .timestamp
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO);
                let seconds = elapsed.as_secs() % 60;
                let minutes = (elapsed.as_secs() / 60) % 60;
                let hours = (elapsed.as_secs() / 3600) % 24;
                error_text.push_str(&format!(
                    "{}: [{}:{:02}:{:02}] {}\n",
                    i + 1,
                    hours,
                    minutes,
                    seconds,
                    if error.message.len() > 60 {
                        format!("{}...", &error.message[..60])
                    } else {
                        error.message.clone()
                    }
                ));
            }
        }

        error_text.push_str("\nPress 'q' or ESC to quit");

        let error_para = Paragraph::new(error_text)
            .block(
                Block::default()
                    .title("Errors & Info")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: true });

        error_para.render(area, buf);
    }
}
