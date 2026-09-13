mod server;

use std::io;

use server::RouterServer;

#[tokio::main]
async fn main() -> io::Result<()> {
    let server = 
        RouterServer::new("127.0.0.1:8080");
    
    server.run().await
}