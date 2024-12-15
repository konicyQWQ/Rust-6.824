use crate::mr::coordinator::CoordinatorRPCServiceClient;
use crate::mrapps::wc::MapReduce;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::net::{IpAddr, Ipv4Addr};
use tarpc::context::Context;
use tarpc::tokio_serde::formats::Json;
use tarpc::{client, serde_transport};

fn i_hash(key: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(key.as_bytes());
    hasher.finish()
}

pub struct Worker {
    rpc_client: CoordinatorRPCServiceClient,
    map_reduce: Box<dyn MapReduce>,
}

impl Worker {
    pub async fn new(port: u16, map_reduce: Box<dyn MapReduce>) -> anyhow::Result<Self> {
        // connect to server,
        // create rpc client
        let server_addr = (IpAddr::V4(Ipv4Addr::LOCALHOST), port);

        let mut transport = serde_transport::tcp::connect(server_addr, Json::default);
        transport.config_mut().max_frame_length(usize::MAX);

        let rpc_client = CoordinatorRPCServiceClient::new(
            client::Config::default(),
            transport.await.expect("can not connect to server"),
        )
        .spawn();

        Ok(Self {
            rpc_client,
            map_reduce,
        })
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        // your worker implementation here
        unimplemented!()

        // self.map_reduce contains the map function and the reduce function.
        // you should use them to implement worker process.
        //
        // see trait MapReduce

        // rpc call example
        //
        // self.rpc_client.call_example(Context::current(), args).await?;
    }
}
