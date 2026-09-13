use std::io;

use tokio::net::TcpStream;


pub struct RouterClient {
    router_address: String,
}

impl RouterClient {
    pub fn new(
        router_address: impl Into<String>,
    ) -> Self {
        Self {
            router_address:
                router_address.into(),
        }
    }

    pub async fn connect(
        &self,
    ) -> io::Result<TcpStream> {
        println!(
            "Connecting to router at {}...",
            self.router_address
        );

        let stream = 
            TcpStream::connect(
                &self.router_address,
            )
            .await?;
        
        println!("Connecting to router!");

        Ok(stream)
    }
}