//! Errors display component for the TUI

use ratatui::{
    prelude::*,
    symbols,
    widgets::{Axis, Block, Borders, Chart, Dataset, GraphType, List, ListItem, Paragraph, Wrap},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::tui::Model;

/// Widget for displaying error information
pub struct ErrorsWidget<'a> {
    model: &'a Model,
}

impl<'a> ErrorsWidget<'a> {
    /// Create a new errors widget with the given model
    pub fn new(model: &'a Model) -> Self {
        Self { model }
    }
}

impl<'a> Widget for ErrorsWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let m = &self.model.metrics;
        
        let mut error_text = format!("Total errors: {}\n", m.error_count);
        
        if !m.last_errors.is_empty() {
            error_text.push_str("\nRecent errors:\n");
            for (i, error) in m.last_errors.iter().enumerate() {
                let elapsed = error.timestamp.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
                let seconds = elapsed.as_secs() % 60;
                let minutes = (elapsed.as_secs() / 60) % 60;
                let hours = (elapsed.as_secs() / 3600) % 24;
                error_text.push_str(&format!("{}: [{}:{:02}:{:02}] {}\n", 
                    i + 1, hours, minutes, seconds, 
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
            .block(Block::default().title("Errors & Info").borders(Borders::ALL))
            .wrap(Wrap { trim: true });
        
        error_para.render(area, buf);
    }
}

/// Enhanced error widget for future improvements
pub struct StatefulErrorWidget<'a> {
    model: &'a Model,
}

impl<'a> StatefulErrorWidget<'a> {
    /// Create a new stateful error widget
    pub fn new(model: &'a Model) -> Self {
        Self { model }
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
        let m = &self.model.metrics;
        
        let mut items: Vec<ListItem> = vec![
            ListItem::new(format!("Total errors: {}", m.error_count)),
        ];
        
        if !m.last_errors.is_empty() {
            items.push(ListItem::new(""));
            items.push(ListItem::new("Recent errors:"));
            
            for error in m.last_errors.iter() {
                let now = SystemTime::now();
                let elapsed = now.duration_since(error.timestamp).unwrap_or(Duration::ZERO);
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
        
        items.push(ListItem::new(""));
        items.push(ListItem::new("Press 'q' or ESC to quit"));
        
        let list = List::new(items)
            .block(Block::default().title("Errors & Info").borders(Borders::ALL));
        
        ratatui::prelude::Widget::render(list, area, buf);
    }
    
    fn render_chart(&self, area: Rect, buf: &mut Buffer) {
        if self.model.error_history.is_empty() {
            // No data yet, just render empty block
            let block = Block::default()
                .title("Error Rate (5 min)")
                .borders(Borders::ALL);
            block.render(area, buf);
            return;
        }

        // Prepare data for chart
        let data_len = self.model.error_history.len();
        let mut error_data = Vec::with_capacity(data_len);
        let mut max_errors: f64 = 0.0;
        
        for (i, &errors_per_sec) in self.model.error_history.iter().enumerate() {
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
            .block(Block::default()
                .title("Error Rate (5 min)")
                .borders(Borders::ALL))
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
