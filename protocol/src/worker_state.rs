use serde::{Deserialize, Serialize};

use crate::WorkerId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerState {
    pub worker_id: WorkerId,

    pub active_requests: usize,

    pub queue_depth: usize,

    pub kv_cache_used: usize,

    pub kv_cache_capacity: usize,

    pub compute_utilization: f64,

    pub cached_prefixes: Vec<Vec<u32>>,
}
