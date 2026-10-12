use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use crate::connections::{WorkerConnections, WorkerSender};
use crate::registry::WorkerRegistry;
use crate::scheduler::LiveScheduler;
use protocol::{InferenceRequest, InferenceResponse, RouterMessage, WorkerId};
use tokio::sync::{Mutex, oneshot};

type ResponseSender = oneshot::Sender<InferenceResponse>;

struct InFlightRequest {
    worker_id: WorkerId,
    response_sender: ResponseSender,
}

type InFlightRequests = HashMap<protocol::RequestId, InFlightRequest>;

#[derive(Clone)]
pub struct Router {
    registry: Arc<Mutex<WorkerRegistry>>,

    connections: Arc<Mutex<WorkerConnections>>,

    scheduler: Arc<Mutex<LiveScheduler>>,

    in_flight: Arc<Mutex<InFlightRequests>>,
}

impl Router {
    const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

    pub fn new() -> Self {
        Self {
            registry: Arc::new(Mutex::new(WorkerRegistry::new())),

            connections: Arc::new(Mutex::new(WorkerConnections::new())),

            scheduler: Arc::new(Mutex::new(LiveScheduler::from_env())),

            in_flight: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn registry(&self) -> Arc<Mutex<WorkerRegistry>> {
        Arc::clone(&self.registry)
    }

    pub async fn register_worker(&self, worker_id: WorkerId, sender: WorkerSender) {
        {
            let mut registry = self.registry.lock().await;

            registry.register(worker_id);

            println!("Total workers: {}", registry.worker_count());
        }

        {
            let mut connections = self.connections.lock().await;

            connections.insert(worker_id, sender);

            println!("Active connections: {}", connections.worker_count());
        }
    }

    pub async fn unregister_worker(&self, worker_id: WorkerId) {
        {
            let mut connections = self.connections.lock().await;

            connections.remove(worker_id);
        }

        {
            let mut registry = self.registry.lock().await;

            registry.unregister(worker_id);
        }

        {
            let mut in_flight = self.in_flight.lock().await;

            let before = in_flight.len();

            in_flight.retain(|_, pending| pending.worker_id != worker_id);

            let removed = before - in_flight.len();

            if removed > 0 {
                println!(
                    "Removed {} pending request for disconnected worker {}",
                    removed, worker_id
                );
            }
        }

        println!("Worker {} disconnected", worker_id);
    }

    pub async fn update_worker_state(&self, state: protocol::WorkerState) {
        let worker_id = state.worker_id;

        let mut registry = self.registry.lock().await;

        registry.update_state(state);

        println!("Heartbeat received from worker {}", worker_id);
    }

    async fn select_worker(&self, request_tokens: &[u32]) -> io::Result<WorkerId> {
        /*
         * Get workers.
         */
        let workers = {
            let registry = self.registry.lock().await;

            let connections = self.connections.lock().await;

            registry
                .is_healthy_workers()
                .into_iter()
                .filter(|worker_id| connections.contains(*worker_id))
                .filter_map(|worker_id| {
                    registry.get(worker_id).map(|registered| {
                        let state = &registered.state;

                        crate::scheduler::LiveWorker {
                            worker_id,
                            active_requests: state.active_requests,
                            queue_depth: state.queue_depth,
                            kv_cache_used: state.kv_cache_used,
                            kv_cache_capacity: state.kv_cache_capacity,
                            cached_prefixes: state.cached_prefixes.clone(),
                        }
                    })
                })
                .collect::<Vec<_>>()
        };

        let mut scheduler = self.scheduler.lock().await;

        scheduler.select(request_tokens, &workers).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "No healthy workers available")
        })
    }

    pub async fn infer(&self, request: InferenceRequest) -> io::Result<InferenceResponse> {
        let request_id = request.request_id;

        /*
         * First choose a worker.
         */
        let worker_id = self.select_worker(&request.tokens).await?;

        /*
         * Create a private response
         * channel for this request.
         */
        let response_rx = {
            let mut in_flight = self.in_flight.lock().await;

            /*
             * Reject duplicate request IDs.
             */
            if in_flight.contains_key(&request_id) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("Request {} is already in flight", request_id),
                ));
            }

            let (tx, rx) = oneshot::channel();

            in_flight.insert(
                request_id,
                InFlightRequest {
                    worker_id,
                    response_sender: tx,
                },
            );

            rx
        };

        println!("Dispatching request {} to worker {}", request_id, worker_id);

        /*
         * Send the request.
         */
        let send_result = {
            let connections = self.connections.lock().await;

            connections
                .send(worker_id, RouterMessage::Request { request })
                .await
        };

        if let Err(error) = send_result {
            let mut in_flight = self.in_flight.lock().await;

            in_flight.remove(&request_id);

            return Err(error);
        }

        println!("Waiting for response to request {}", request_id);

        /*
         * Wait for the response handler
         * to complete this request.
         */

        match timeout(Self::REQUEST_TIMEOUT, response_rx).await {
            Ok(Ok(response)) => Ok(response),

            Ok(Err(_)) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!("Response channel closed for request {}", request_id),
            )),

            Err(_) => {
                let mut inflight = self.in_flight.lock().await;
                inflight.remove(&request_id);

                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "Request {} timed out after {:?}",
                        request_id,
                        Self::REQUEST_TIMEOUT
                    ),
                ))
            }
        }
    }

    pub async fn handle_response(&self, response: InferenceResponse) -> io::Result<()> {
        let request_id = response.request_id;
        let response_worker_id = response.worker_id;

        let sender = {
            let mut in_flight = self.in_flight.lock().await;

            let Some(pending) = in_flight.get(&request_id) else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("No inflight request {}", request_id),
                ));
            };

            if pending.worker_id != response_worker_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Request {} was assigned to worker {}, \
                        but worker {} sent the response",
                        request_id, pending.worker_id, response_worker_id,
                    ),
                ));
            }

            in_flight
                .remove(&request_id)
                .expect("request was checked above")
                .response_sender
        };

        println!(
            "Response received for request {} from worker {}",
            request_id, response_worker_id
        );
        println!("Generated {} output tokens", response.output_tokens.len());

        sender.send(response).map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                format!(
                    "Requester for {} \
                         is gone",
                    request_id
                ),
            )
        })
    }
}
