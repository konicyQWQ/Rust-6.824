#[cfg(test)]
mod tests {
    use crate::kvsrv::client::Clerk;
    use crate::kvsrv::server::{KVServer, KVServicePeer};
    use std::sync::Arc;

    #[tokio::test]
    async fn simple_test() {
        let server = Arc::new(KVServer::new());

        let (interface, _, _) = KVServicePeer::create(server.clone());

        let client = Clerk::new(interface);

        let reply = client.get("1234".to_string()).await;
        assert!(reply.is_ok());
        assert_eq!(reply.unwrap(), None);

        let reply = client.put("1234".to_string(), "1234".to_string()).await;
        assert!(reply.is_ok());

        let reply = client.put_append("1234".to_string(), "1234".to_string()).await;
        assert!(reply.is_ok());
        assert_eq!(reply.unwrap(), Some("1234".to_string()));

        let reply = client.get("1234".to_string()).await;
        assert!(reply.is_ok());
        assert_eq!(reply.unwrap(), Some("12341234".to_string()));
    }
}
