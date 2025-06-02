//! Errors display component for the TUI

use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Paragraph, Wrap},
};

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
                error_text.push_str(&format!("{}: {}\n", i + 1, error));
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
    
    /// Render with split layout for future chart integration
    pub fn render_with_chart(&self, area: Rect, buf: &mut Buffer) {
        // For now, just render the basic widget
        // Future: split into left (error list) and right (error rate chart)
        ErrorsWidget::new(self.model).render(area, buf);
    }
}
