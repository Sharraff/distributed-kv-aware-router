struct WorkerState {
    worker_id: WorkerId,
    active_requests: usize,
    queue_depth: usize,
    kv_cache_used: usize,
    kv_cache_capacity: usize,
    cache_entries: HashMap<Prefix, CacheEntry>,
    compute_utilization: f64,
}