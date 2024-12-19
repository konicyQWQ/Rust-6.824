use crate::kvsrv::common::{GetArgs, PutAppendArgs};
use crate::kvsrv::server::KVServiceInterface;

pub struct Clerk {
    server: KVServiceInterface,
    // your definitions here
}

impl Clerk {
    pub fn new(server: KVServiceInterface) -> Clerk {
        // your code here
        // unimplemented!();
        Clerk { server }
    }

    pub async fn get(&self, key: String) -> Result<Option<String>, &str> {
        // your code here
        // unimplemented!();
        self.server
            .get(GetArgs { key })
            .await
            .map(|reply| reply.value)
    }

    pub async fn put(&self, key: String, value: String) -> Result<(), &str> {
        // your code here
        // unimplemented!();
        self.server
            .put(PutAppendArgs { key, value })
            .await
            .map(|_| ())
    }

    /// append value to key's value and return that value
    pub async fn put_append(&self, key: String, value: String) -> Result<Option<String>, &str> {
        // your code here
        // unimplemented!();
        self.server
            .put_append(PutAppendArgs { key, value })
            .await
            .map(|reply| reply.value)
    }
}
