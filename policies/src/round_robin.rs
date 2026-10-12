use simulator::{request::Request, worker::Worker};

use crate::RoutingPolicy;

pub struct RoundRobin {
    next: usize,
}

impl RoundRobin {
    pub fn new() -> Self {
        Self { next: 0 }
    }
}

impl RoutingPolicy for RoundRobin {
    fn choose_worker(&mut self, _request: &Request, workers: &mut [Worker]) -> usize {
        let selected = self.next % workers.len();
        self.next += 1;
        selected
    }
}
