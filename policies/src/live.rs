#[derive(Debug, Clone)]
pub struct LiveWorker {
    pub worker_id: u64,
    pub active_requests: usize,
    pub queue_depth: usize,
    pub kv_cache_used: usize,
    pub kv_cache_capacity: usize,
    pub cached_prefixes: Vec<Vec<u32>>,
}

impl LiveWorker {
    pub fn load(&self) -> usize {
        self.active_requests + self.queue_depth
    }
}

pub trait LiveRoutingPolicy: Send {
    fn choose_worker(&mut self, request_tokens: &[u32], workers: &[LiveWorker]) -> Option<u64>;
}

#[derive(Debug, Default)]
pub struct LiveRoundRobin {
    next_index: usize,
}

#[derive(Debug)]
pub struct LiveHybrid {
    pub cache_weight: f64,
    pub load_weight: f64,
}

impl LiveHybrid {
    pub fn new(cache_weight: f64, load_weight: f64) -> Self {
        Self {
            cache_weight,
            load_weight,
        }
    }
}

impl Default for LiveHybrid {
    fn default() -> Self {
        Self::new(0.7, 0.3)
    }
}

impl LiveRoutingPolicy for LiveRoundRobin {
    fn choose_worker(&mut self, _request_tokens: &[u32], workers: &[LiveWorker]) -> Option<u64> {
        if workers.is_empty() {
            return None;
        }

        let index = self.next_index % workers.len();
        self.next_index = (index + 1) % workers.len();

        Some(workers[index].worker_id)
    }
}

#[derive(Debug, Default)]
pub struct LiveLeastLoaded;

impl LiveRoutingPolicy for LiveLeastLoaded {
    fn choose_worker(&mut self, _request_tokens: &[u32], workers: &[LiveWorker]) -> Option<u64> {
        workers
            .iter()
            .min_by_key(|worker| worker.load())
            .map(|worker| worker.worker_id)
    }
}

#[derive(Debug)]
pub enum LiveScheduler {
    RoundRobin(LiveRoundRobin),
    LeastLoaded(LiveLeastLoaded),
    CacheAware(LiveCacheAware),
    Hybrid(LiveHybrid),
}

impl Default for LiveScheduler {
    fn default() -> Self {
        Self::RoundRobin(LiveRoundRobin::default())
    }
}

impl LiveScheduler {
    pub fn from_env() -> Self {
        match std::env::var("ROUTING_POLICY")
            .unwrap_or_else(|_| "round-robin".to_string())
            .as_str()
        {
            "least-loaded" => Self::LeastLoaded(LiveLeastLoaded),
            "cache-aware" => Self::CacheAware(LiveCacheAware),
            "hybrid" => Self::Hybrid(LiveHybrid::default()),
            _ => Self::RoundRobin(LiveRoundRobin::default()),
        }
    }

    pub fn select(&mut self, request_tokens: &[u32], workers: &[LiveWorker]) -> Option<u64> {
        match self {
            Self::RoundRobin(policy) => policy.choose_worker(request_tokens, workers),
            Self::LeastLoaded(policy) => policy.choose_worker(request_tokens, workers),
            Self::CacheAware(policy) => policy.choose_worker(request_tokens, workers),
            Self::Hybrid(policy) => policy.choose_worker(request_tokens, workers),
        }
    }
}

#[derive(Debug, Default)]
pub struct LiveCacheAware;

impl LiveRoutingPolicy for LiveCacheAware {
    fn choose_worker(&mut self, request_tokens: &[u32], workers: &[LiveWorker]) -> Option<u64> {
        workers
            .iter()
            .max_by_key(|worker| {
                let cached_tokens = worker
                    .cached_prefixes
                    .iter()
                    .map(|cached| {
                        cached
                            .iter()
                            .zip(request_tokens.iter())
                            .take_while(|(a, b)| a == b)
                            .count()
                    })
                    .max()
                    .unwrap_or(0);

                // Prefer the longest actual prefix match.
                // On equal cache hits, prefer the less-loaded worker.
                (cached_tokens, usize::MAX - worker.load())
            })
            .map(|worker| worker.worker_id)
    }
}

impl LiveRoutingPolicy for LiveHybrid {
    fn choose_worker(&mut self, request_tokens: &[u32], workers: &[LiveWorker]) -> Option<u64> {
        if workers.is_empty() {
            return None;
        }

        let max_load = workers
            .iter()
            .map(LiveWorker::load)
            .max()
            .unwrap_or(0)
            .max(1);

        workers
            .iter()
            .map(|worker| {
                let cached_tokens = worker
                    .cached_prefixes
                    .iter()
                    .map(|cached| {
                        cached
                            .iter()
                            .zip(request_tokens.iter())
                            .take_while(|(a, b)| a == b)
                            .count()
                    })
                    .max()
                    .unwrap_or(0);

                let cache_ratio = cached_tokens as f64 / request_tokens.len().max(1) as f64;

                let normalized_load = worker.load() as f64 / max_load as f64;

                let score = self.cache_weight * cache_ratio - self.load_weight * normalized_load;

                (worker.worker_id, score, worker.load())
            })
            .max_by(|a, b| {
                a.1.total_cmp(&b.1)
                    // On equal scores, prefer lower load.
                    .then_with(|| b.2.cmp(&a.2))
            })
            .map(|(worker_id, _, _)| worker_id)
    }
}
