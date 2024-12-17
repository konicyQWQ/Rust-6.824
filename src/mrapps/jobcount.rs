use crate::mrapps::wc::{KeyValue, MapReduce};
use std::fs;
use std::sync::Mutex;
use std::thread::sleep;
use std::time::Duration;

pub struct JobCountMapReduce;

static COUNT: Mutex<i32> = Mutex::new(0);

impl MapReduce for JobCountMapReduce {
    fn map(&self, _filename: &str, _contents: &str) -> Vec<KeyValue> {
        let mut count = COUNT.lock().unwrap();

        let me = std::process::id();
        let file = format!("mr-worker-jobcount-{}-{}", me, count);
        *count += 1;

        fs::write(file, "x").expect("Unable to write file");
        sleep(Duration::from_millis(2000 + rand::random::<u64>() % 3000));

        vec![KeyValue {
            key: "a".to_string(),
            value: "x".to_string(),
        }]
    }

    fn reduce(&self, _key: &str, _values: &[String]) -> String {
        fs::read_dir(".")
            .expect("Unable to read dir")
            .into_iter()
            .map(|f| f.expect("Unable to read dir entry"))
            .map(|entry| entry.file_name().to_str().unwrap().to_string())
            .filter(|f| f.starts_with("mr-worker-jobcount-"))
            .count()
            .to_string()
    }
}
