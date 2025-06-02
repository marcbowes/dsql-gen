use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Clear, Paragraph},
};

/// Modal state to track which field is active and the form values
#[derive(Clone, Debug)]
pub struct WorkloadModalState {
    pub visible: bool,
    pub total_batches: usize,
    pub concurrency: u32,
    pub rows_per_transaction: usize,
    pub active_field: usize, // 0: batches, 1: concurrency, 2: rows
    pub editing: bool,       // Whether we're currently editing the active field
    pub buffer: String,      // Buffer for the field currently being edited
}

impl Default for WorkloadModalState {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkloadModalState {
    pub fn new() -> Self {
        Self {
            visible: false,
            total_batches: 100,
            concurrency: 10,
            rows_per_transaction: 1000,
            active_field: 0,
            editing: false,
            buffer: String::new(),
        }
    }

    /// Initialize the buffer with the current value of the active field
    pub fn start_editing(&mut self) {
        self.editing = true;
        self.buffer = match self.active_field {
            0 => self.total_batches.to_string(),
            1 => self.concurrency.to_string(),
            2 => self.rows_per_transaction.to_string(),
            _ => String::new(),
        };
    }

    /// Apply the buffer value to the active field
    pub fn apply_edit(&mut self) {
        match self.active_field {
            0 => {
                if let Ok(val) = self.buffer.parse() {
                    self.total_batches = val;
                }
            }
            1 => {
                if let Ok(val) = self.buffer.parse::<u32>() {
                    self.concurrency = val;
                }
            }
            2 => {
                if let Ok(val) = self.buffer.parse() {
                    self.rows_per_transaction = val;
                }
            }
            _ => {}
        }
        self.editing = false;
        self.buffer.clear();
    }

    /// Cancel the current edit
    pub fn cancel_edit(&mut self) {
        self.editing = false;
        self.buffer.clear();
    }

    /// Navigate to the previous field
    pub fn previous_field(&mut self) {
        if self.active_field > 0 {
            self.active_field -= 1;
        } else {
            self.active_field = 2; // Wrap around to last field
        }
    }

    /// Navigate to the next field
    pub fn next_field(&mut self) {
        self.active_field = (self.active_field + 1) % 3;
    }

    /// Get the current value of the specified field as a string
    pub fn field_value(&self, field_index: usize) -> String {
        match field_index {
            0 => self.total_batches.to_string(),
            1 => self.concurrency.to_string(),
            2 => self.rows_per_transaction.to_string(),
            _ => String::new(),
        }
    }

    /// Show the modal
    pub fn show(&mut self) {
        self.visible = true;
    }

    /// Hide the modal
    pub fn hide(&mut self) {
        self.visible = false;
        self.editing = false;
        self.buffer.clear();
    }
}

/// Workload Modal Widget
pub struct WorkloadModalWidget<'a> {
    state: &'a WorkloadModalState,
}

impl<'a> WorkloadModalWidget<'a> {
    pub fn new(state: &'a WorkloadModalState) -> Self {
        Self { state }
    }

    pub fn render(&self, area: Rect, buf: &mut Buffer) {
        // Only render if visible
        if !self.state.visible {
            return;
        }

        // Create a clear area to render modal on top
        Clear.render(area, buf);

        // Calculate modal size - fixed size for simplicity
        let modal_width = 50;
        let modal_height = 10;

        // Center the modal in the available area
        let modal_x = (area.width.saturating_sub(modal_width)) / 2;
        let modal_y = (area.height.saturating_sub(modal_height)) / 2;

        let modal_area = Rect {
            x: area.x + modal_x,
            y: area.y + modal_y,
            width: modal_width,
            height: modal_height,
        };

        // Create and render the modal block
        Block::default()
            .title("Workload Parameters")
            .borders(Borders::ALL)
            .render(modal_area, buf);

        // Calculate inner area manually - with a 1 cell border
        let inner = Rect {
            x: modal_area.x + 1,
            y: modal_area.y + 1,
            width: modal_area.width.saturating_sub(2),
            height: modal_area.height.saturating_sub(2),
        };

        // Create field areas - 3 rows, one for each parameter
        let field_areas = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2), // Total batches
                Constraint::Length(2), // Concurrency
                Constraint::Length(2), // Rows per transaction
                Constraint::Length(1), // Spacer
                Constraint::Length(1), // Controls hint
            ])
            .split(inner);

        // Render the fields
        self.render_field("Total batches:", 0, field_areas[0], buf);

        self.render_field("Concurrency:", 1, field_areas[1], buf);

        self.render_field("Rows per transaction:", 2, field_areas[2], buf);

        // Render controls hint
        let controls = "Tab: Next | Shift+Tab: Previous | Enter: Save | Esc: Cancel";
        let controls_para = Paragraph::new(controls)
            .alignment(Alignment::Center)
            .style(Style::default().fg(Color::Gray));

        controls_para.render(field_areas[4], buf);
    }

    fn render_field(&self, label: &str, field_index: usize, area: Rect, buf: &mut Buffer) {
        // Split the area into label and value
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(20), // Label
                Constraint::Min(5),     // Value
            ])
            .split(area);

        // Render label
        let label_para = Paragraph::new(label);
        label_para.render(chunks[0], buf);

        // Determine value to display (buffer if editing this field, otherwise the actual value)
        let display_value = if self.state.editing && self.state.active_field == field_index {
            self.state.buffer.clone()
        } else {
            self.state.field_value(field_index)
        };

        // Determine style based on whether this field is active
        let style = if self.state.active_field == field_index {
            if self.state.editing {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::UNDERLINED)
            } else {
                Style::default()
                    .fg(Color::Green)
                    .add_modifier(Modifier::UNDERLINED)
            }
        } else {
            Style::default()
        };

        // Render value
        let value_para = Paragraph::new(display_value).style(style);
        value_para.render(chunks[1], buf);
    }
}
