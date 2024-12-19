use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PutAppendArgs {
    pub key: String,
    pub value: String,
    // you'll have to add definitions here
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PutAppendReply {
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetArgs {
    pub key: String,
    // you'll have to add definitions here
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetReply {
    pub value: Option<String>,
}