use crate::kvsrv::common::{GetArgs, GetReply, PutAppendArgs, PutAppendReply};
use labrpc::labrpc;
use std::collections::HashMap;
use tokio::sync::Mutex;

#[labrpc]
trait KVService {
    async fn get(args: GetArgs) -> GetReply;
    async fn put(args: PutAppendArgs) -> PutAppendReply;
    async fn put_append(args: PutAppendArgs) -> PutAppendReply;
}

pub struct KVServer {
    // your definitions here
    map: Mutex<HashMap<String, String>>,
}

impl KVServer {
    pub fn new() -> Self {
        // your code here
        // unimplemented!();
        KVServer {
            map: Mutex::new(HashMap::new()),
        }
    }
}

impl KVService for KVServer {
    async fn get(&self, args: GetArgs) -> GetReply {
        // your code here
        // unimplemented!();
        GetReply {
            value: self.map.lock().await.get(&args.key).cloned(),
        }
    }

    async fn put(&self, args: PutAppendArgs) -> PutAppendReply {
        // your code here
        // unimplemented!();
        PutAppendReply {
            value: self.map.lock().await.insert(args.key, args.value),
        }
    }

    async fn put_append(&self, args: PutAppendArgs) -> PutAppendReply {
        // your code here
        // unimplemented!();
        let mut map = self.map.lock().await;

        let old_value = map.get(&args.key).cloned();

        map.entry(args.key)
            .and_modify(|v| *v = format!("{}{}", v, args.value))
            .or_insert(args.value);

        PutAppendReply { value: old_value }
    }
}
