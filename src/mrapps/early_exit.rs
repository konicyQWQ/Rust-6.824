use crate::mrapps::wc::{KeyValue, MapReduce};
use std::thread::sleep;
use std::time::Duration;

pub struct EarlyExitMapReduce;

impl MapReduce for EarlyExitMapReduce {
    fn map(&self, filename: &str, _contents: &str) -> Vec<KeyValue> {
        vec![KeyValue {
            key: filename.to_string(),
            value: "1".to_string(),
        }]
    }

    fn reduce(&self, key: &str, values: &[String]) -> String {
        if key.contains("sherlock") || key.contains("tom") {
            sleep(Duration::from_secs(3));
        }

        values.len().to_string()
    }
}
