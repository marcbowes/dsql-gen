//! Performance stats component for the TUI

use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Paragraph},
};

use crate::tui::Model;

/// Widget for displaying performance statistics
pub struct PerformanceWidget<'a> {
    model: &'a Model,
}

impl<'a> PerformanceWidget<'a> {
    /// Create a new performance widget with the given model
    pub fn new(model: &'a Model) -> Self {
        Self { model }
    }
}

impl<'a> Widget for PerformanceWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let throughput_text = vec![
            format!("Transactions/sec: {}", self.model.tps as u64),
            format!("Rows/sec: {}", self.model.rps as u64),
            format!("Throughput: {}", self.model.bps_formatted),
            format!("Pool: {} open, {} idle", self.model.pool_size, self.model.pool_idle),
        ].join("\n");
        
        let throughput_para = Paragraph::new(throughput_text)
            .block(Block::default().title("Performance").borders(Borders::ALL));
        
        throughput_para.render(area, buf);
    }
}

/// Placeholder for future stateful performance widget with chart
pub struct StatefulPerformanceWidget<'a> {
    model: &'a Model,
}

impl<'a> StatefulPerformanceWidget<'a> {
    /// Create a new stateful performance widget
    pub fn new(model: &'a Model) -> Self {
        Self { model }
    }
    
    /// Render with split layout for future chart integration
    pub fn render_with_chart(&self, area: Rect, buf: &mut Buffer) {
        // For now, just render the basic widget
        // Future: split into left (stats) and right (chart)
        PerformanceWidget::new(self.model).render(area, buf);
    }
}
