use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

// tcp::OwnedReadHalf,
use tokio::net::{TcpStream, tcp::OwnedWriteHalf};

use tokio::sync::{Mutex, mpsc};
use tokio::time::interval;

use protocol::{
    InferenceRequest, InferenceResponse, RouterMessage, WorkerId, WorkerMessage, WorkerState, codec,
};

const KV_CACHE_CAPACITY: usize = 100;
const REQUEST_QUEUE_CAPACITY: usize = 32;

#[derive(Debug)]
struct RuntimeState {
    active_requests: usize,
    queue_depth: usize,
    kv_cache_used: usize,
    compute_utilization: f64,
    cached_prefixes: HashMap<Vec<u32>, usize>,
    cache_order: VecDeque<Vec<u32>>,
    started_at: Instant,
}

impl RuntimeState {
    fn new() -> Self {
        Self {
            active_requests: 0,
            queue_depth: 0,
            kv_cache_used: 0,
            compute_utilization: 0.0,
            cached_prefixes: HashMap::new(),
            cache_order: VecDeque::new(),
            started_at: Instant::now(),
        }
    }

    fn snapshot(&self, worker_id: WorkerId) -> WorkerState {
        WorkerState {
            worker_id,
            active_requests: self.active_requests,
            queue_depth: self.queue_depth,
            kv_cache_used: self.kv_cache_used,
            kv_cache_capacity: KV_CACHE_CAPACITY,
            compute_utilization: self.compute_utilization,
            cached_prefixes: self.cached_prefixes.keys().cloned().collect(),
        }
    }

    fn touch_cached_prefix(&mut self, tokens: &[u32]) -> bool {
        if !self.cached_prefixes.contains_key(tokens) {
            return false;
        }

        // Move the entry to the most-recently-used end.
        if let Some(position) = self
            .cache_order
            .iter()
            .position(|entry| entry.as_slice() == tokens)
        {
            self.cache_order.remove(position);
        }

        self.cache_order.push_back(tokens.to_vec());
        true
    }

    fn cache_prefix(&mut self, tokens: Vec<u32>) {
        if self.touch_cached_prefix(&tokens) {
            return;
        }

        // The simulation accounts for atleast one token per entry.
        let cost = tokens.len().max(1);

        // An entry larger than the entire cache cannot be stored.
        if cost > KV_CACHE_CAPACITY {
            return;
        }

        // Evict least-recently-used entries untill this one fits.
        while self.kv_cache_used + cost > KV_CACHE_CAPACITY {
            let Some(oldest) = self.cache_order.pop_front() else {
                break;
            };

            if let Some(old_cost) = self.cached_prefixes.remove(&oldest) {
                self.kv_cache_used = self.kv_cache_used.saturating_sub(old_cost);

                println!(
                    "Evicted cached prompt ({} tokens); cache usage: {}/{}",
                    old_cost, self.kv_cache_used, KV_CACHE_CAPACITY,
                );
            }
        }

        self.kv_cache_used += cost;
        self.cache_order.push_back(tokens.clone());
        self.cached_prefixes.insert(tokens, cost);
    }
}

pub struct RouterClient {
    router_address: String,
}

impl RouterClient {
    pub fn new(router_address: impl Into<String>) -> Self {
        Self {
            router_address: router_address.into(),
        }
    }

    pub async fn connect_and_run(&self, worker_id: WorkerId) -> io::Result<()> {
        println!(
            "Worker {} connecting to router at {}...",
            worker_id, self.router_address
        );

        let stream = TcpStream::connect(&self.router_address).await?;
        println!("Connected to router!");

        let (reader, writer) = stream.into_split();

        // All worker -> router messages go through this channel.
        let (tx, rx) = mpsc::channel::<WorkerMessage>(32);

        let (request_tx, request_rx) = mpsc::channel::<InferenceRequest>(REQUEST_QUEUE_CAPACITY);

        let state = Arc::new(Mutex::new(RuntimeState::new()));

        // Start the single writer task.
        let writer_task = tokio::spawn(writer_loop(writer, rx));

        tx.send(WorkerMessage::Register { worker_id })
            .await
            .map_err(io::Error::other)?;

        let heartbeat_state = Arc::clone(&state);
        let heartbeat_tx = tx.clone();

        let heartbeat_task = tokio::spawn(async move {
            heartbeat_loop(worker_id, heartbeat_state, heartbeat_tx).await;
        });

        let processing_state = Arc::clone(&state);
        let processing_tx = tx.clone();

        let processing_task = tokio::spawn(async move {
            processing_loop(worker_id, processing_state, request_rx, processing_tx).await
        });

        let connection_result = receive_loop(reader, worker_id, request_tx, state).await;

        // Stop the background tasks when the connection closes.
        heartbeat_task.abort();
        processing_task.abort();

        // Closing the sender eventually causes the writer task to finish.
        drop(tx);

        let _ = writer_task.await;
        connection_result
    }
}

async fn writer_loop(
    mut writer: OwnedWriteHalf,

    mut rx: mpsc::Receiver<WorkerMessage>,
) -> io::Result<()> {
    while let Some(message) = rx.recv().await {
        let bytes = codec::encode(&message)?;

        writer.write_all(&bytes).await?;
    }

    Ok(())
}

async fn heartbeat_loop(
    worker_id: WorkerId,
    state: Arc<Mutex<RuntimeState>>,
    tx: mpsc::Sender<WorkerMessage>,
) {
    let mut timer = interval(Duration::from_secs(2));

    loop {
        timer.tick().await;

        let snapshot = state.lock().await.snapshot(worker_id);

        if tx
            .send(WorkerMessage::Heartbeat { state: snapshot })
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn receive_loop(
    reader: tokio::net::tcp::OwnedReadHalf,
    worker_id: WorkerId,
    request_rx: mpsc::Sender<protocol::InferenceRequest>,
    state: Arc<Mutex<RuntimeState>>,
) -> io::Result<()> {
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();

        let bytes_read = reader.read_line(&mut line).await?;

        if bytes_read == 0 {
            println!("Router disconnected from worker {}", worker_id);
            return Ok(());
        }

        let message: RouterMessage = codec::decode(line.as_bytes())?;

        match message {
            RouterMessage::RegisterAck { worker_id } => {
                println!("Worker {} successfully registered!", worker_id);
            }

            RouterMessage::Request { request } => {
                {
                    let mut state = state.lock().await;
                    state.queue_depth += 1;
                }

                match request_rx.try_send(request) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(request)) => {
                        state.lock().await.queue_depth -= 1;

                        eprintln!(
                            "Worker {} queue full; rejecting request {}",
                            worker_id, request.request_id
                        );

                        // This simulator has no explicit error-response variant
                        // in RouterMessage/WorkerMessage yet. We close neither
                        // connection nor silently start unbounded work.
                        drop(request);
                    }

                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "Request processing queue closed",
                        ));
                    }
                }
            }
        }
    }
}

async fn processing_loop(
    worker_id: WorkerId,
    state: Arc<Mutex<RuntimeState>>,
    mut request_rx: mpsc::Receiver<InferenceRequest>,
    tx: mpsc::Sender<WorkerMessage>,
) -> io::Result<()> {
    while let Some(request) = request_rx.recv().await {
        {
            let mut state = state.lock().await;
            state.queue_depth = state.queue_depth.saturating_sub(1);
            state.active_requests += 1;
            state.compute_utilization = 1.0;
        }

        let result = handle_request(worker_id, request, Arc::clone(&state), tx.clone()).await;
        {
            let mut state = state.lock().await;
            state.active_requests = state.active_requests.saturating_sub(1);
            state.compute_utilization = if state.active_requests == 0 { 0.0 } else { 1.0 };
        }

        result?;
    }

    Ok(())
}

async fn handle_request(
    worker_id: WorkerId,
    request: protocol::InferenceRequest,
    state: Arc<Mutex<RuntimeState>>,
    tx: mpsc::Sender<WorkerMessage>,
) -> io::Result<()> {
    println!(
        "Worker {} processing request {}",
        worker_id, request.request_id
    );

    // Simulate compute so active worker is observable in heartbeats.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // A simple simulated prefix cache. Cache keys are the complete input
    // token sequences; this is not a real model KV cache.
    {
        let mut state = state.lock().await;

        state.cache_prefix(request.tokens.clone());

        // let key = request.tokens.clone();

        // if !state.cached_prefixes.contains_key(&key) {
        //     let cache_cost = request.tokens.len().max(1);

        //     if state.kv_cache_used + cache_cost <= KV_CACHE_CAPACITY {
        //         state.kv_cache_used += cache_cost;
        //         state.cached_prefixes.insert(key, cache_cost);
        //     }
        // }
    }

    // Temporary fake inference
    let output_tokens = (0..request.output_tokens).map(|i| i as u32).collect();

    let response = InferenceResponse {
        request_id: request.request_id,

        worker_id,

        output_tokens,
    };

    tx.send(WorkerMessage::Response { response })
        .await
        .map_err(io::Error::other)?;

    println!(
        "Worker {} sent response for request {}",
        worker_id, request.request_id
    );

    Ok(())
}