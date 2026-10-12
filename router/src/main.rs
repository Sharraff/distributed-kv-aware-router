mod connections;
mod health;
mod registry;
mod router;
mod scheduler;
mod server;

use std::io;
use std::time::Instant;

use protocol::InferenceRequest;
use server::RouterServer;
use tokio::time::{Duration, sleep};

struct WorkloadItem {
    arrival_ms: u64,
    request_id: u64,
    prompt_len: usize,
    output_len: usize,
}

fn generate_workload() -> Vec<WorkloadItem> {
    vec![
        WorkloadItem {
            arrival_ms: 0,
            request_id: 1,
            prompt_len: 16,
            output_len: 8,
        },
        WorkloadItem {
            arrival_ms: 50,
            request_id: 2,
            prompt_len: 64,
            output_len: 16,
        },
        WorkloadItem {
            arrival_ms: 80,
            request_id: 3,
            prompt_len: 16,
            output_len: 32,
        },
        WorkloadItem {
            arrival_ms: 100,
            request_id: 4,
            prompt_len: 128,
            output_len: 16,
        },
        WorkloadItem {
            arrival_ms: 150,
            request_id: 5,
            prompt_len: 32,
            output_len: 8,
        },
        WorkloadItem {
            arrival_ms: 200,
            request_id: 6,
            prompt_len: 64,
            output_len: 32,
        },
        WorkloadItem {
            arrival_ms: 250,
            request_id: 7,
            prompt_len: 256,
            output_len: 16,
        },
        WorkloadItem {
            arrival_ms: 300,
            request_id: 8,
            prompt_len: 16,
            output_len: 8,
        },
        WorkloadItem {
            arrival_ms: 350,
            request_id: 9,
            prompt_len: 128,
            output_len: 32,
        },
        WorkloadItem {
            arrival_ms: 400,
            request_id: 10,
            prompt_len: 32,
            output_len: 16,
        },
        WorkloadItem {
            arrival_ms: 450,
            request_id: 11,
            prompt_len: 64,
            output_len: 8,
        },
        WorkloadItem {
            arrival_ms: 500,
            request_id: 12,
            prompt_len: 256,
            output_len: 32,
        },
    ]
}

fn make_request(item: &WorkloadItem) -> InferenceRequest {
    // Requests 1, 3, and 8 share a token prefix.
    // Requests 2, 6, and 11 share another token prefix.
    let prefix: &[u32] = match item.request_id {
        1 | 3 | 8 => &[10, 20, 30, 40],
        2 | 6 | 11 => &[50, 60, 70, 80],
        _ => &[item.request_id as u32, 900, 901, 902],
    };

    let tokens = (0..item.prompt_len)
        .map(|i| prefix.get(i).copied().unwrap_or(1000 + i as u32))
        .collect();

    InferenceRequest {
        request_id: item.request_id,
        tokens,
        output_tokens: item.output_len,
    }
}

async fn run_workload(router: router::Router) {
    let workload = generate_workload();
    let start = Instant::now();

    println!("Workload started: {} requests", workload.len());

    let mut tasks = Vec::with_capacity(workload.len());

    for item in workload {
        // Schedule arrivals relative to workload start, not the previous request.
        let target = Duration::from_millis(item.arrival_ms);
        let elapsed = start.elapsed();

        if target > elapsed {
            sleep(target - elapsed).await;
        }

        let request = make_request(&item);
        let prompt_len = request.tokens.len();
        let output_len = request.output_tokens;
        let request_id = request.request_id;
        let router = router.clone();
        let submitted_at = Instant::now();

        println!(
            "[+{} ms] Submit request {} (prompt={}, output={})",
            start.elapsed().as_millis(),
            request_id,
            prompt_len,
            output_len,
        );

        tasks.push(tokio::spawn(async move {
            match router.infer(request).await {
                Ok(response) => {
                    println!(
                        "Request {} completed: {} output tokens, latency={} ms",
                        response.request_id,
                        response.output_tokens.len(),
                        submitted_at.elapsed().as_millis(),
                    );
                }
                Err(error) => {
                    eprintln!(
                        "Request {} failed after {} ms: {}",
                        request_id,
                        submitted_at.elapsed().as_millis(),
                        error,
                    );
                }
            }
        }));
    }

    let mut completed_tasks = 0;

    for task in tasks {
        match task.await {
            Ok(()) => completed_tasks += 1,
            Err(error) => eprintln!("Request task failed: {}", error),
        }
    }

    println!(
        "Workload finished: {}/{} request tasks completed",
        completed_tasks, completed_tasks,
    );
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let server = RouterServer::new("127.0.0.1:8080");
    let router = server.router();

    tokio::spawn(async move {
        // Allow workers time to connect and register.
        sleep(Duration::from_secs(3)).await;
        run_workload(router).await;
    });

    server.run().await
}
