use chrono::Utc;
use std::fs::OpenOptions;
use std::io::{self, Write};

#[derive(Debug)]
enum LogLevel {
    Info,
    Warn,
    Error,
}

pub struct Logger {
    file: Option<std::fs::File>,
    log_to_file: bool,
}

impl Logger {
    /// Creates a new `Logger` instance.
    ///
    /// # Parameters
    /// - `log_to_file`: Determines if the logger writes messages to a file (`true`) or to the console (`false`).
    /// - `log_file`: Optional file path for the log file. If `None`, defaults to "default.log".
    ///
    /// # Returns
    /// A new `Logger` instance.
    pub fn new(log_to_file: bool, log_file: Option<&str>) -> Self {
        let file = if log_to_file {
            Some(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_file.unwrap_or("default.log"))
                    .unwrap(),
            )
        } else {
            None
        };
        Logger { file, log_to_file }
    }

    // Generic method for writing log messages
    fn log(&mut self, level: LogLevel, message: &str) {
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let log_message = match level {
            LogLevel::Info => format!("[INFO] [{}]: {}\n", timestamp, message),
            LogLevel::Warn => format!("[WARN] [{}]: {}\n", timestamp, message),
            LogLevel::Error => format!("[ERROR] [{}]: {}\n", timestamp, message),
        };

        // If logging to console, apply colors
        if !self.log_to_file {
            let colored_message = match level {
                LogLevel::Info => format!("\x1b[96m{}\x1b[0m", log_message), // Turquoise
                LogLevel::Warn => format!("\x1b[93m{}\x1b[0m", log_message), // Bright Yellow
                LogLevel::Error => format!("\x1b[91m{}\x1b[0m", log_message), // Bright Red
            };
            println!("{}", colored_message);
            io::stdout().flush().unwrap();
        }

        // If logging to file
        if let Some(file) = &mut self.file {
            file.write_all(log_message.as_bytes()).unwrap();
            file.flush().unwrap();
        }
    }

    /// Logs an informational message.
    ///
    /// # Parameters
    /// - `message`: The informational message to log.
    pub fn info(&mut self, message: &str) {
        self.log(LogLevel::Info, message);
    }

    /// Logs a warning message.
    ///
    /// # Parameters
    /// - `message`: The warning message to log.
    pub fn warn(&mut self, message: &str) {
        self.log(LogLevel::Warn, message);
    }

    /// Logs an error message.
    ///
    /// # Parameters
    /// - `message`: The error message to log.
    pub fn error(&mut self, message: &str) {
        self.log(LogLevel::Error, message);
    }
}

fn main() {
    // Usage:

    // Console logging:
    let mut logger = Logger::new(false, None);

    logger.info("This is an info log message.");
    logger.warn("This is a warning log message.");
    logger.error("This is an error log message.");

    // File logging:
    let mut file_logger = Logger::new(true, Some("app.log"));

    file_logger.info("This is an info message in the file.");
    file_logger.warn("This is a warning message in the file.");
    file_logger.error("This is an error message in the file.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_console_logging() {
        let mut logger = Logger::new(false, None);
        logger.info("Test info message");
        logger.warn("Test warning message");
        logger.error("Test error message");
        // This test only verifies that logging doesn't panic in console mode.
    }

    #[test]
    fn test_file_logging() {
        let log_file = "test.log";
        let mut logger = Logger::new(true, Some(log_file));

        logger.info("Test info message in file");
        logger.warn("Test warning message in file");
        logger.error("Test error message in file");

        let contents = fs::read_to_string(log_file).unwrap();
        assert!(contents.contains("[INFO]"));
        assert!(contents.contains("Test info message in file"));
        assert!(contents.contains("[WARN]"));
        assert!(contents.contains("Test warning message in file"));
        assert!(contents.contains("[ERROR]"));
        assert!(contents.contains("Test error message in file"));

        // Cleanup
        fs::remove_file(log_file).unwrap();
    }

    #[test]
    fn test_file_append() {
        let log_file = "append_test.log";
        let mut logger = Logger::new(true, Some(log_file));

        logger.info("First log message");

        // Reopen logger to simulate appending to an existing file
        let mut logger2 = Logger::new(true, Some(log_file));
        logger2.info("Second log message");

        let contents = fs::read_to_string(log_file).unwrap();
        assert!(contents.contains("First log message"));
        assert!(contents.contains("Second log message"));

        // Cleanup
        fs::remove_file(log_file).unwrap();
    }
}
