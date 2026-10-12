use std::io;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use tokio::net::{TcpListener, TcpStream};

use tokio::sync::mpsc;

use protocol::{RouterMessage, WorkerMessage, codec};

use crate::health;
use crate::router::Router;

#[derive(Clone)]
pub struct RouterServer {
    address: String,

    router: Router,
}

impl RouterServer {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),

            router: Router::new(),
        }
    }

    pub fn router(&self) -> Router {
        self.router.clone()
    }

    pub async fn run(&self) -> io::Result<()> {
        let listener = TcpListener::bind(&self.address).await?;

        println!("Router listening on {}", self.address);

        // Health monitoring is owned by health.rs
        health::spawn_health_monitor(self.router.registry());

        loop {
            let (socket, address) = listener.accept().await?;

            println!("Connection recieved from {}", address);

            let router = self.router().clone();

            tokio::spawn(async move {
                if let Err(error) = handle_connection(socket, router).await {
                    eprintln!("Connection error: {}", error);
                }
            });
        }
    }
}

async fn handle_connection(socket: TcpStream, router: Router) -> io::Result<()> {
    let (reader, writer) = socket.into_split();
    let (tx, mut rx) = mpsc::channel::<RouterMessage>(32);

    let writer_task = tokio::spawn(async move {
        let mut writer = writer;

        while let Some(message) = rx.recv().await {
            let bytes = codec::encode(&message)?;
            writer.write_all(&bytes).await?;
        }

        Ok::<(), io::Error>(())
    });

    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let mut registered_worker: Option<u64> = None;

    // Keep the result so cleanup runs even if reading or decoding fails.
    let connection_result: io::Result<()> = async {
        loop {
            line.clear();

            let bytes_read = reader.read_line(&mut line).await?;

            if bytes_read == 0 {
                println!("Connection closed by peer");
                break;
            }

            let message: WorkerMessage = codec::decode(line.as_bytes())?;

            match message {
                WorkerMessage::Register { worker_id } => {
                    if registered_worker.is_some() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Worker connection attempted to register more than once",
                        ));
                    }

                    println!("Registering worker {}", worker_id);

                    router.register_worker(worker_id, tx.clone()).await;
                    registered_worker = Some(worker_id);

                    tx.send(RouterMessage::RegisterAck { worker_id })
                        .await
                        .map_err(io::Error::other)?;

                    println!("Worker {} registered", worker_id);
                }

                WorkerMessage::Heartbeat { state } => {
                    router.update_worker_state(state).await;
                }

                WorkerMessage::Response { response } => {
                    if let Err(error) = router.handle_response(response).await {
                        eprintln!("Failed to handle response: {}", error);
                    }
                }
            }
        }

        Ok(())
    }
    .await;

    // Always attempt to unregister, including on malformed input or I/O errors.
    if let Some(worker_id) = registered_worker {
        router.unregister_worker(worker_id).await;
    }

    // Closing our sender allows the writer task to finish after queued messages.
    drop(tx);

    let writer_result = writer_task.await.map_err(io::Error::other)?;

    // Preserve the original connection error if reading or decoding failed.
    connection_result?;

    // Otherwise, report any error encountered by the writer task.
    writer_result?;

    Ok(())
}
