use futures::{future, prelude::*};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::{atomic, Arc, Mutex};
use std::time::{Duration, Instant};
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
    async fn get_task(id: u32, status: WorkerStatus) -> Task;
}

impl CoordinatorRPCService for CoordinatorServer {
    async fn call_example(self, _: Context, args: String) -> String {
        format!("Hello, {}!", args)
    }

    // implement your rpc functions here
    async fn get_task(self, _: Context, id: u32, status: WorkerStatus) -> Task {
        match status {
            WorkerStatus::Finish => self.coordinator.finish_task(id),
            _ => {}
        }

        if self.coordinator.done() {
            Task::Finish
        } else {
            self.coordinator.allocate_task(id)
        }
    }
}

pub struct Coordinator {
    // your coordinator implementation here

    // !!!NOTICE!!!
    // the CoordinatorServer contains Arc<Coordinator> which means the Coordinator is immutable.
    // you can use Mutex<...> or atomic::... to ensure the Coordinator is thread-safe and interior mutable.
    n_reduce: u32,
    done: atomic::AtomicBool,
    tasks: Mutex<Vec<WaitTaskWithWorker>>,
}

impl Coordinator {
    pub fn new(files: &[String], n_reduce: u32) -> Self {
        // your coordinator implementation here
        // unimplemented!()

        Coordinator {
            n_reduce,
            done: atomic::AtomicBool::new(false),
            tasks: Mutex::new(
                files
                    .iter()
                    .map(|file| WaitTaskWithWorker {
                        task: WaitTask::Map(file.to_string()),
                        worker_id: None,
                        start_time: None,
                    })
                    .collect(),
            ),
        }
    }

    pub fn done(&self) -> bool {
        // your coordinator implementation here
        // unimplemented!()

        self.done.load(atomic::Ordering::SeqCst)
    }

    // add other functions, you can only use immutable self

    pub fn set_done(&self) {
        self.done.store(true, atomic::Ordering::SeqCst);
    }

    pub fn finish_task(&self, worker_id: u32) {
        let mut tasks = self.tasks.lock().unwrap();

        // find this task
        if let Some(pos) = tasks
            .iter()
            .position(|task| task.worker_id == Some(worker_id))
        {
            // remove it
            let task = tasks.swap_remove(pos);
            let task_name = match &task.task {
                WaitTask::Map(_) => "map",
                WaitTask::Reduce(_) => "reduce",
            };

            // rename tmp_file to file
            for i in 0..self.n_reduce {
                match &task.task {
                    WaitTask::Map(filename) => {
                        let tmp_file =
                            format!("{}-{}-{}-{}-tmp", i, filename, worker_id, task_name);
                        let file = format!("{}-{}-{}-{}", i, filename, worker_id, task_name);
                        std::fs::rename(&tmp_file, &file).unwrap_or_default();
                    }
                    WaitTask::Reduce(_) => {
                        let tmp_file = format!("{}-{}-{}-tmp", i, worker_id, task_name);
                        let file = format!("mr-out-{}-{}-{}", i, worker_id, task_name);
                        std::fs::rename(&tmp_file, &file).unwrap_or_default();
                    }
                }
            }

            // if map task all done, add reduce task
            if task_name == "map" && tasks.is_empty() {
                (0..self.n_reduce)
                    .map(|i| WaitTaskWithWorker {
                        task: WaitTask::Reduce(i),
                        worker_id: None,
                        start_time: None,
                    })
                    .for_each(|task| tasks.push(task));
            }

            // if reduce task all done, whole task done
            if task_name == "reduce" && tasks.is_empty() {
                self.set_done();
            }
        }
    }

    pub fn allocate_task(&self, worker_id: u32) -> Task {
        let mut tasks = self.tasks.lock().unwrap();

        tasks
            .iter_mut()
            .find(|task| {
                task.worker_id == None
                    || task.start_time.unwrap().elapsed() > Duration::from_secs(10)
            })
            .map_or(Task::Wait, |task| {
                task.worker_id = Some(worker_id);
                task.start_time = Some(Instant::now());
                match &task.task {
                    WaitTask::Map(file) => Task::Map(MapTask {
                        input_file: file.to_string(),
                        reduce_number: self.n_reduce,
                    }),
                    WaitTask::Reduce(idx) => Task::Reduce(ReduceTask { reduce_idx: *idx }),
                }
            })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WorkerStatus {
    Empty,
    Finish,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MapTask {
    pub input_file: String,
    pub reduce_number: u32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ReduceTask {
    pub reduce_idx: u32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Task {
    Map(MapTask),
    Reduce(ReduceTask),
    Wait,
    Finish,
}

enum WaitTask {
    Map(String),
    Reduce(u32),
}

struct WaitTaskWithWorker {
    task: WaitTask,
    worker_id: Option<u32>,
    start_time: Option<Instant>,
}
