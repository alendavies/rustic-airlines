use chrono::{Duration, NaiveDateTime};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    thread,
    time::{Duration as StdDuration, Instant},
};

use crate::types::TICK_FREQUENCY_MILLIS;

use super::sim_error::SimError;

pub struct Timer {
    pub current_time: Mutex<NaiveDateTime>, 
    pub tick_advance: RwLock<Duration>,     
    pub running: AtomicBool,                // Flag to indicate if the timer is running
}

impl Timer {
    /// Creates new timer
    pub fn new(start_time: NaiveDateTime, tick_advance_minutes: i64) -> Arc<Self> {
        Arc::new(Self {
            current_time: Mutex::new(start_time),
            tick_advance: RwLock::new(Duration::minutes(tick_advance_minutes)),
            running: AtomicBool::new(true),
        })
    }

    /// Changes the value of time advanced per tick
    pub fn set_tick_advance(&self, new_tick_advance_minutes: i64) -> Result<(), SimError> {
        if new_tick_advance_minutes <= 0 || new_tick_advance_minutes > 10000 {
            return Err(SimError::InvalidDuration(new_tick_advance_minutes.to_string()));
        }

        let mut tick_advance_lock = self.tick_advance.write().map_err(|_| {
            SimError::TimerLockError("Failed to acquire write lock for tick_advance.".to_string())
        })?;
        *tick_advance_lock = Duration::minutes(new_tick_advance_minutes);
        Ok(())
    }

    /// Stops the timer
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Starts timer and executes the callback function on each tick.
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
                                eprintln!(
                                    "Failed to acquire read lock on tick_advance. Skipping tick."
                                );
                                continue;
                            }
                        };

                        *time_lock += tick_advance;
                        current_time = *time_lock;
                    }

                    tick_count += 1;

                    tick_callback(current_time, tick_count);

                    let elapsed = now.elapsed();
                    let sleep_duration =
                        StdDuration::from_millis(TICK_FREQUENCY_MILLIS).saturating_sub(elapsed);
                    thread::sleep(sleep_duration);
                }

                println!("Timer stopped.");
            })
            .map_err(|_| {
                SimError::TimerStartError("Failed to start the timer thread.".to_string())
            })?;

        Ok(())
    }
}
