mod client;

use std::io;

use client::RouterClient;

#[tokio::main]
async fn main() -> io::Result<()> {
    let client =
        RouterClient::new(
            "127.0.0.1:8080",
        )
    let _stream = 
        client.connect().await?;

    Ok(())
}