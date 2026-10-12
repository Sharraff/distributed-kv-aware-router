use std::collections::HashMap;
use std::time::{Duration, Instant};

use protocol::{WorkerId, WorkerState};

const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(6);

#[derive(Debug)]
pub struct RegisteredWorker {
    pub state: WorkerState,
    pub last_heartbeat: Instant,
}

impl RegisteredWorker {
    pub fn is_healthy(&self) -> bool {
        self.last_heartbeat.elapsed() <= HEARTBEAT_TIMEOUT
    }
}

#[derive(Debug)]
pub struct WorkerRegistry {
    workers: HashMap<WorkerId, RegisteredWorker>,
}

impl WorkerRegistry {
    pub fn new() -> Self {
        Self {
            workers: HashMap::new(),
        }
    }

    pub fn register(&mut self, worker_id: WorkerId) {
        println!("Registering worker {}", worker_id);

        let state = WorkerState {
            worker_id,
            active_requests: 0,
            queue_depth: 0,
            kv_cache_used: 0,
            kv_cache_capacity: 100,
            compute_utilization: 0.0,
            cached_prefixes: vec![
                vec![10, 20, 30, 40],
                vec![50, 60, 70, 80],
            ],
        };

        self.workers.insert(
            worker_id,
            RegisteredWorker {
                state,
                last_heartbeat: Instant::now(),
            },
        );
    }

    pub fn unregister(&mut self, worker_id: WorkerId) {
        self.workers.remove(&worker_id);
    }

    pub fn update_state(&mut self, state: WorkerState) {
        let worker_id = state.worker_id;

        if let Some(worker) = self.workers.get_mut(&worker_id) {
            worker.state = state;
            worker.last_heartbeat = Instant::now();
        }
    }

    pub fn get(&self, worker_id: WorkerId) -> Option<&RegisteredWorker> {
        self.workers.get(&worker_id)
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn is_healthy_workers(&self) -> Vec<WorkerId> {
        self.workers
            .iter()
            .filter_map(|(&worker_id, worker)| {
                if worker.is_healthy() {
                    Some(worker_id)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn unhealthy_workers(&self) -> Vec<WorkerId> {
        self.workers
            .iter()
            .filter_map(|(&worker_id, worker)| {
                if !worker.is_healthy() {
                    Some(worker_id)
                } else {
                    None
                }
            })
            .collect()
    }
}
