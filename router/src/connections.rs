use std::collections::HashMap;
use std::io;

use protocol::{RouterMessage, WorkerId};
use tokio::sync::mpsc;

pub type WorkerSender = mpsc::Sender<RouterMessage>;

#[derive(Debug, Default)]
pub struct WorkerConnections {
    connections: HashMap<WorkerId, WorkerSender>,
}

impl WorkerConnections {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }

    pub fn insert(&mut self, worker_id: WorkerId, sender: WorkerSender) {
        self.connections.insert(worker_id, sender);
    }

    pub fn remove(&mut self, worker_id: WorkerId) {
        self.connections.remove(&worker_id);
    }

    pub fn worker_ids(&self) -> Vec<WorkerId> {
        self.connections.keys().copied().collect()
    }

    pub fn worker_count(&self) -> usize {
        self.connections.len()
    }

    pub async fn send(&self, worker_id: WorkerId, message: RouterMessage) -> io::Result<()> {
        let Some(sender) = self.connections.get(&worker_id) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Worker {} is not connected", worker_id),
            ));
        };

        sender.send(message).await.map_err(io::Error::other)
    }

    pub fn contains(&self, worker_id: WorkerId) -> bool {
        self.connections.contains_key(&worker_id)
    }
}
