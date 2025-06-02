use std::io::stdout;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::{
    ExecutableCommand,
    event::{self, Event, KeyCode, KeyModifiers},
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::prelude::*;
use tokio::sync::mpsc;
use tokio::time;

use crate::events::{EventListener, Message};
use crate::runner::WorkloadRunner;
use crate::tui::{self, Model};

#[derive(Debug, Clone, PartialEq)]
enum AppState {
    Running,
    Stopping,
    WaitingForFinalStats,
    EditingWorkload,
}

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
        let mut app_state = AppState::Running;
        let mut final_stats_start: Option<Instant> = None;

        loop {
            // Check for key events
            if event::poll(Duration::from_millis(0))? {
                if let Event::Key(key) = event::read()? {
                    match app_state {
                        AppState::EditingWorkload => {
                            // Handle workload modal keyboard input
                            match key.code {
                                KeyCode::Esc => {
                                    // Cancel editing and close modal
                                    model.workload_modal_state.hide();
                                    app_state = AppState::Running;
                                }
                                KeyCode::Enter => {
                                    if model.workload_modal_state.editing {
                                        // Apply the current edit
                                        model.workload_modal_state.apply_edit();
                                    } else {
                                        // Save changes and close modal
                                        let new_total_batches =
                                            model.workload_modal_state.total_batches;

                                        // Update runner parameters if they've changed
                                        if model.runner.batches() != new_total_batches {
                                            model.runner.set_batches(new_total_batches);
                                        }

                                        // Note: concurrency and rows_per_transaction would be used here
                                        // to update the workload runner if changing mid-execution was supported

                                        // Close modal and return to running state
                                        model.workload_modal_state.hide();
                                        app_state = AppState::Running;
                                    }
                                }
                                KeyCode::Tab => {
                                    if key.modifiers.contains(KeyModifiers::SHIFT) {
                                        // Go to previous field
                                        model.workload_modal_state.previous_field();
                                    } else {
                                        // Go to next field
                                        model.workload_modal_state.next_field();
                                    }
                                }
                                KeyCode::Char(c)
                                    if c.is_ascii_digit() && model.workload_modal_state.editing =>
                                {
                                    // Add digit to buffer
                                    model.workload_modal_state.buffer.push(c);
                                }
                                KeyCode::Backspace if model.workload_modal_state.editing => {
                                    // Remove last character from buffer
                                    model.workload_modal_state.buffer.pop();
                                }
                                KeyCode::Char(' ') if !model.workload_modal_state.editing => {
                                    // Space to start editing the current field
                                    model.workload_modal_state.start_editing();
                                }
                                _ => {}
                            }
                        }
                        _ => {
                            // Handle normal mode keyboard input
                            match key.code {
                                KeyCode::Char('w') if app_state == AppState::Running => {
                                    // Show workload settings modal
                                    model.workload_modal_state.show();
                                    app_state = AppState::EditingWorkload;
                                }
                                _ => {
                                    // Handle quit keys: 'q', ESC, or Ctrl-C
                                    let should_quit = key.code == KeyCode::Char('q')
                                        || key.code == KeyCode::Esc
                                        || (key.code == KeyCode::Char('c')
                                            && key.modifiers.contains(KeyModifiers::CONTROL));

                                    if should_quit {
                                        match app_state {
                                            AppState::Running => {
                                                // First quit: stop the load generator but keep TUI running
                                                app_state = AppState::Stopping;
                                                running.abort();
                                            }
                                            AppState::Stopping | AppState::WaitingForFinalStats => {
                                                // Second quit: exit immediately
                                                break;
                                            }
                                            AppState::EditingWorkload => {
                                                // If in workload editing mode, just close the modal and return to running state
                                                model.workload_modal_state.hide();
                                                app_state = AppState::Running;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Wait for tick
            interval.tick().await;

            // Process available events
            self.listener.process_available_messages(&mut model).await;

            // Load gen stopped check based on events and completion
            if running.is_finished()
                && (self.listener.completed
                    || model.metrics.completed_batches >= model.runner.batches())
            {
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

            // Draw UI using the component architecture
            self.terminal.draw(|f| {
                tui::draw(f, &model);

                // Show stopping state overlay
                match app_state {
                    AppState::Stopping => {
                        let stopping_msg = ratatui::widgets::Paragraph::new(
                            "Stopping load generator... Press quit again to exit immediately.",
                        )
                        .style(
                            Style::default()
                                .fg(Color::Yellow)
                                .add_modifier(Modifier::BOLD),
                        )
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
                        let waiting_msg = ratatui::widgets::Paragraph::new(
                            "Collecting final usage stats... Press quit to exit.",
                        )
                        .style(
                            Style::default()
                                .fg(Color::Cyan)
                                .add_modifier(Modifier::BOLD),
                        )
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
                    AppState::EditingWorkload => {
                        // The workload modal is rendered separately in tui::draw
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
