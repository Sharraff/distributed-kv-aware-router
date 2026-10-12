use std::collections::VecDeque;

use crate::cache::PrefixCache;
use crate::request::Request;

#[derive(Debug)]
pub struct ActiveRequest {
    pub request: Request,

    // How much simulated processing time remains.
    pub remaining_ms: f64,

    // Simulation timestamp at which processing started.
    pub start_time_ms: f64,

    // Number of input tokens already present in the worker's cache.
    pub cached_tokens: usize,
}

#[derive(Debug)]
pub struct Worker {
    pub id: usize,

    pub queue: VecDeque<Request>,

    pub cache: PrefixCache,

    pub active: Option<ActiveRequest>,

    pub completed_requests: u64,

    pub total_busy_ms: f64,
}

impl Worker {
    pub fn new(id: usize, cache_capacity_tokens: usize) -> Self {
        Self {
            id,
            queue: VecDeque::new(),
            cache: PrefixCache::new(cache_capacity_tokens),
            active: None,
            completed_requests: 0,
            total_busy_ms: 0.0,
        }
    }

    pub fn queue_depth(&self) -> f64 {
        self.queue.len() as f64
    }

    pub fn is_idle(&self) -> bool {
        self.active.is_none()
    }

    pub fn load(&self) -> f64 {
        let queue_load = self.queue.len() as f64;

        let active_load = if self.active.is_some() { 1.0 } else { 0.0 };

        queue_load + active_load
    }
}
