mod client;

use std::io;

use client::RouterClient;

#[tokio::main]
async fn main() -> io::Result<()> {
    //let worker_id = 0;
    let worker_id: u64 = std::env::args()
        .nth(1)
        .expect("Usage: worker <WORKER_ID>")
        .parse()
        .expect("WORKER_ID must be an unsigned integer");

    let client = RouterClient::new("127.0.0.1:8080");
    client.connect_and_run(worker_id).await?;

    Ok(())
}
