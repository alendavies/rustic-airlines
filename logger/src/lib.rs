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
    // Crea un nuevo Logger
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

    // Método genérico para escribir en el log
    fn log(&mut self, level: LogLevel, message: &str) {
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let log_message = match level {
            LogLevel::Info => format!("[INFO] [{}]: {}\n", timestamp, message),
            LogLevel::Warn => format!("[WARN] [{}]: {}\n", timestamp, message),
            LogLevel::Error => format!("[ERROR] [{}]: {}\n", timestamp, message),
        };

        // Si se quiere mostrar por consola, aplicamos colores
        if !self.log_to_file {
            let colored_message = match level {
                LogLevel::Info => format!("\x1b[96m{}\x1b[0m", log_message), // Turquesa
                LogLevel::Warn => format!("\x1b[93m{}\x1b[0m", log_message), // Amarillo brillante
                LogLevel::Error => format!("\x1b[91m{}\x1b[0m", log_message), // Rojo brillante
            };
            print!("{}", colored_message);
            io::stdout().flush().unwrap();
        }

        // Si se quiere escribir en archivo
        if let Some(file) = &mut self.file {
            file.write_all(log_message.as_bytes()).unwrap();
            file.flush().unwrap();
        }
    }

    // Método para loguear mensajes de tipo "INFO"
    pub fn info(&mut self, message: &str) {
        self.log(LogLevel::Info, message);
    }

    // Método para loguear mensajes de tipo "WARN"
    pub fn warn(&mut self, message: &str) {
        self.log(LogLevel::Warn, message);
    }

    // Método para loguear mensajes de tipo "ERROR"
    pub fn error(&mut self, message: &str) {
        self.log(LogLevel::Error, message);
    }
}

fn main() {
    // Modo de uso:

    // Para imprimir por consola:
    let mut logger = Logger::new(false, None);

    logger.info("Este es un mensaje de log de info.");
    logger.warn("Este es un mensaje de log de advertencia.");
    logger.error("Este es un mensaje de log de error.");

    // Para escribir en archivo:
    let mut file_logger = Logger::new(true, Some("app.log"));

    file_logger.info("Este es un mensaje de info en archivo.");
    file_logger.warn("Este es un mensaje de advertencia en archivo.");
    file_logger.error("Este es un mensaje de error en archivo.");
}
