use crate::mr::coordinator::{CoordinatorRPCServiceClient, Task, WorkerStatus};
use crate::mrapps::wc::{KeyValue, MapReduce};
use itertools::Itertools;
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

        let worker_id = std::process::id();
        let mut status = WorkerStatus::Empty;
        loop {
            let task = self
                .rpc_client
                .get_task(Context::current(), worker_id, status.clone())
                .await?;

            match task {
                Task::Map(task) => {
                    let content = tokio::fs::read_to_string(&task.input_file).await?;
                    let kvs = self.map_reduce.map(&task.input_file, &content);
                    let mut buckets: Vec<Vec<KeyValue>> =
                        vec![Vec::new(); task.reduce_number as usize];
                    for kv in kvs {
                        buckets[(i_hash(kv.key.as_str()) % task.reduce_number as u64) as usize]
                            .push(kv);
                    }
                    for i in 0..task.reduce_number {
                        let file_name = format!("{}-{}-{}-map-tmp", i, task.input_file, worker_id);
                        let content = buckets[i as usize]
                            .iter()
                            .map(|kv| format!("{} {}", kv.key, kv.value))
                            .join("\n");
                        tokio::fs::write(file_name, content.as_bytes()).await?;
                    }

                    status = WorkerStatus::Finish
                }
                Task::Reduce(task) => {
                    let dir_entries = tokio::fs::read_dir(".").await?;
                    let mut files = Vec::new();

                    tokio::pin!(dir_entries);
                    while let Some(entry) = dir_entries.next_entry().await? {
                        let file_name = entry.file_name();
                        if let Some(name) = file_name.to_str() {
                            if name.starts_with(&format!("{}-", task.reduce_idx))
                                && name.ends_with("-map")
                            {
                                files.push(entry.path());
                            }
                        }
                    }

                    let mut kvs: Vec<KeyValue> = Vec::new();
                    for path in files {
                        let content = tokio::fs::read_to_string(path).await?;
                        content.lines().for_each(|line| {
                            let kv: Vec<&str> = line.split_whitespace().collect();
                            kvs.push(KeyValue {
                                key: kv[0].to_string(),
                                value: kv[1].to_string(),
                            })
                        })
                    }

                    let mut out_kvs: Vec<KeyValue> = Vec::new();

                    kvs.sort();
                    for (key, chunk) in &kvs.iter().chunk_by(|kv| &kv.key) {
                        let values: Vec<String> = chunk.map(|kv| kv.value.clone()).collect();
                        let reduce_output = self.map_reduce.reduce(key, &values);

                        out_kvs.push(KeyValue {
                            key: key.to_string(),
                            value: reduce_output,
                        })
                    }

                    let file_name = format!("{}-{}-reduce-tmp", task.reduce_idx, worker_id);
                    let content = out_kvs
                        .iter()
                        .map(|kv| format!("{} {}", kv.key, kv.value))
                        .join("\n");
                    tokio::fs::write(file_name, content.as_bytes()).await?;

                    status = WorkerStatus::Finish
                }
                Task::Wait => {
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    status = WorkerStatus::Empty
                }
                Task::Finish => break,
            }
        }

        Ok(())
    }
}
