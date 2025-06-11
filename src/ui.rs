use std::io::stdout;
use std::time::Duration;

use anyhow::Result;
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::prelude::*;
use tokio::sync::mpsc;
use tokio::time;

use crate::events::{EventListener, Message};
use crate::runner::WorkloadRunner;
use crate::tui::{self, Model};

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

        loop {
            // Check for key events
            if event::poll(Duration::from_millis(0))? {
                if let Event::Key(key) = event::read()? {
                    match key.code {
                        _ => {
                            // Handle quit keys: 'q', ESC, or Ctrl-C
                            let should_quit = key.code == KeyCode::Char('q')
                                || key.code == KeyCode::Esc
                                || (key.code == KeyCode::Char('c')
                                    && key.modifiers.contains(KeyModifiers::CONTROL));

                            if should_quit {
                                running.abort();
                                break;
                            }
                        }
                    }
                }
            }

            // Wait for tick
            interval.tick().await;

            // Process available events
            self.listener.process_available_messages(&mut model).await;

            if running.is_finished() || self.listener.completed {
                break;
            }

            // Draw UI using the component architecture
            self.terminal.draw(|f| {
                tui::draw(f, &mut model);
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
