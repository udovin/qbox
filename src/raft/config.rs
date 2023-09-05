use std::time::Duration;

use rand::{thread_rng, Rng};

use super::Error;

#[derive(Clone)]
pub struct Config {
    pub min_election_timeout: Duration,
    pub max_election_timeout: Duration,
    pub heartbeat_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min_election_timeout: Duration::from_millis(150),
            max_election_timeout: Duration::from_millis(300),
            heartbeat_timeout: Duration::from_millis(50),
        }
    }
}

impl Config {
    pub fn new_rand_election_timeout(&self) -> Duration {
        Duration::from_micros(
            thread_rng().gen_range(
                self.min_election_timeout.as_micros()..self.max_election_timeout.as_micros(),
            ) as u64,
        )
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.min_election_timeout > self.max_election_timeout {
            Err("min_election_timeout is greater than max_election_timeout")?
        }
        if self.heartbeat_timeout > self.min_election_timeout {
            Err("heartbeat_timeout is greater than min_election_timeout")?
        }
        Ok(())
    }
}
