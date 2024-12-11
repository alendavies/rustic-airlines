use std::{
    sync::{Arc, Mutex, RwLock, atomic::{AtomicBool, Ordering}},
    thread,
    time::{Duration as StdDuration, Instant},
};
use chrono::{NaiveDateTime, Duration};

use crate::types::TICK_FREQUENCY_MILLIS;

use super::sim_error::SimError;

pub struct Timer {
    pub current_time: Mutex<NaiveDateTime>, // Tiempo protegido por Mutex
    pub tick_advance: RwLock<Duration>,    // Duración protegida por RwLock
    pub running: AtomicBool,               // Flag to indicate if the timer is running
}

impl Timer {
    /// Crea un nuevo Timer
    pub fn new(start_time: NaiveDateTime, tick_advance_minutes: i64) -> Arc<Self> {
        Arc::new(Self {
            current_time: Mutex::new(start_time),
            tick_advance: RwLock::new(Duration::minutes(tick_advance_minutes)),
            running: AtomicBool::new(true),
        })
    }

    /// Cambia el valor de tick_advance
    pub fn set_tick_advance(&self, new_tick_advance_minutes: i64) -> Result<(), SimError> {
        let mut tick_advance_lock = self
            .tick_advance
            .write()
            .map_err(|_| SimError::TimerLockError("Failed to acquire write lock for tick_advance.".to_string()))?;
        *tick_advance_lock = Duration::minutes(new_tick_advance_minutes);
        Ok(())
    }

    /// Detiene el Timer
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Inicia el timer y ejecuta el callback en cada tick
    pub fn start(
        self: Arc<Self>,
        tick_callback: impl Fn(NaiveDateTime, usize) + Send + 'static,
    ) -> Result<(), SimError> {
        thread::Builder::new()
            .name("timer-thread".to_string())
            .spawn(move || {
                let mut tick_count = 0;
                while self.running.load(Ordering::SeqCst) {
                    let now = Instant::now();

                    // Actualiza el tiempo del simulador
                    let current_time;
                    {
                        let mut time_lock = match self.current_time.lock() {
                            Ok(lock) => lock,
                            Err(_) => {
                                eprintln!("Failed to acquire lock on current_time. Skipping tick.");
                                continue;
                            }
                        };

                        let tick_advance = match self.tick_advance.read() {
                            Ok(duration) => *duration,
                            Err(_) => {
                                eprintln!("Failed to acquire read lock on tick_advance. Skipping tick.");
                                continue;
                            }
                        };

                        *time_lock += tick_advance;
                        current_time = *time_lock; // Copia el valor para usarlo fuera del Mutex
                    }

                    tick_count += 1;

                    // Ejecuta el callback
                    tick_callback(current_time, tick_count);

                    // Espera hasta el próximo tick
                    let elapsed = now.elapsed();
                    let sleep_duration =
                        StdDuration::from_millis(TICK_FREQUENCY_MILLIS).saturating_sub(elapsed);
                    thread::sleep(sleep_duration);
                }

                println!("Timer stopped.");
            })
            .map_err(|_| SimError::TimerStartError("Failed to start the timer thread.".to_string()))?;

        Ok(())
    }
}
