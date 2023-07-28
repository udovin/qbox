use std::time::Duration;

use rand::{thread_rng, Rng};

pub struct Config {
    pub min_election_timeout: Duration,
    pub max_election_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self { 
            min_election_timeout: Duration::from_millis(150),
            max_election_timeout: Duration::from_millis(300),
        }
    }
}

impl Config {
    pub fn new_rand_election_timeout(&self) -> Duration {
        Duration::from_micros(thread_rng().gen_range(self.min_election_timeout.as_micros()..self.max_election_timeout.as_micros()) as u64)
    }
}
