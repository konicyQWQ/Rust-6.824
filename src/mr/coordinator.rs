use futures::{future, prelude::*};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use tarpc::context::Context;
use tarpc::server::{self, incoming::Incoming, Channel};
use tarpc::tokio_serde::formats::Json;

#[derive(Clone)]
pub struct CoordinatorServer {
    coordinator: Arc<Coordinator>,
}

impl CoordinatorServer {
    pub fn new(coordinator: Arc<Coordinator>) -> Self {
        CoordinatorServer { coordinator }
    }

    pub async fn start(&self, port: u16) -> anyhow::Result<()> {
        async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
            tokio::spawn(fut);
        }

        let server_addr = (IpAddr::V4(Ipv4Addr::LOCALHOST), port);

        let mut listener = tarpc::serde_transport::tcp::listen(&server_addr, Json::default).await?;
        listener.config_mut().max_frame_length(usize::MAX);

        listener
            .filter_map(|r| future::ready(r.ok()))
            .map(server::BaseChannel::with_defaults)
            .max_channels_per_key(10, |t| t.transport().peer_addr().unwrap().ip())
            .map(|channel| {
                let server = self.clone();
                channel.execute(server.serve()).for_each(spawn)
            })
            .buffer_unordered(10)
            .for_each(|_| async {})
            .await;

        Ok(())
    }
}

#[tarpc::service]
pub trait CoordinatorRPCService {
    async fn call_example(args: String) -> String;

    // add your rpc functions here
}

impl CoordinatorRPCService for CoordinatorServer {
    async fn call_example(self, _: Context, args: String) -> String {
        format!("Hello, {}!", args)
    }

    // implement your rpc functions here
}

pub struct Coordinator {
    // your coordinator implementation here

    // !!!NOTICE!!!
    // the CoordinatorServer contains Arc<Coordinator> which means the Coordinator is immutable.
    // you can use Mutex<...> or atomic::... to ensure the Coordinator is thread-safe and interior mutable.
}

impl Coordinator {
    pub fn new(files: &[String], n_reduce: u32) -> Self {
        // your coordinator implementation here
        unimplemented!()
    }

    pub fn done(&self) -> bool {
        // your coordinator implementation here
        unimplemented!()
    }

    // add other functions, you can only use immutable self
}
