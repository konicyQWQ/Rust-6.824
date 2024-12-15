use rust_6824::mr::worker::Worker;
use rust_6824::mrapps::get_map_reduce;
use std::env;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();

    let map_reduce_name = args
        .get(1)
        .expect("Usage: ./mrWorker <mapreduce_name> <port>");
    let map_reduce_func = get_map_reduce(map_reduce_name).expect("Unknown map reduce name");

    let port = args
        .get(2)
        .expect("Usage: ./mrWorker <mapreduce_name> <port>")
        .parse::<u16>()
        .expect("Invalid port number");

    Worker::new(port, map_reduce_func).await?.start().await?;

    Ok(())
}
