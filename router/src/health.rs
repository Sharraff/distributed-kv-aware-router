use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::interval;

use crate::registry::WorkerRegistry;

pub fn spawn_health_monitor(registry: Arc<Mutex<WorkerRegistry>>) {
    tokio::spawn(async move {
        let mut timer = interval(Duration::from_secs(2));

        loop {
            timer.tick().await;

            let registry = registry.lock().await;

            let healthy = registry.is_healthy_workers();

            let unhealthy = registry.unhealthy_workers();

            println!(
                "Health check: {} healthy, {} unhealthy",
                healthy.len(),
                unhealthy.len()
            );

            if !unhealthy.is_empty() {
                println!("Unhealthy workers: {:?}", unhealthy);
            }
        }
    });
}
