use rust_6824::mr::coordinator::{Coordinator, CoordinatorServer};
use std::env;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();
    let port = args
        .get(1)
        .expect("Usage: ./mrCoordinator <port> <input_file>...")
        .parse::<u16>()
        .expect("Invalid port number");

    let coordinator = Arc::new(Coordinator::new(&args[2..], 10));

    let server = CoordinatorServer::new(coordinator.clone());
    tokio::spawn(async move {
        server.start(port).await.expect("Failed to start server");
    });

    while !coordinator.done() {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    tokio::time::sleep(Duration::from_secs(1)).await;

    Ok(())
}
