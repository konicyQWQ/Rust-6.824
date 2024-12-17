use crate::mrapps::wc::{KeyValue, MapReduce};
use nix::unistd::Pid;
use std::fs;
use std::thread::sleep;
use std::time::Duration;

pub struct RTimingMapReduce;

fn nparallel(phase: &str) -> u32 {
    let pid = std::process::id();
    let filename = format!("mr-worker-{}-{}", phase, pid);
    fs::write(&filename, "x").expect("Unable to write file");

    let mut alive_process = 0;
    let mut dir = fs::read_dir(".").expect("Unable to read dir");
    while let Some(entry) = dir.next() {
        let entry = entry.expect("Unable to read entry");
        let filename = entry.file_name().to_str().unwrap().to_string();
        if filename.starts_with("mr-worker-") {
            let pid = filename
                .split("-")
                .last()
                .unwrap()
                .parse::<i32>()
                .expect("Unable to parse pid");

            match nix::sys::signal::kill(Pid::from_raw(pid), None) {
                Ok(_) => {
                    alive_process += 1;
                }
                Err(_) => {}
            }
        }
    }

    sleep(Duration::from_secs(1));
    fs::remove_file(&filename).expect("Unable to remove file");

    alive_process
}

impl MapReduce for RTimingMapReduce {
    fn map(&self, _filename: &str, _contents: &str) -> Vec<KeyValue> {
        ('a'..='j')
            .map(|c| KeyValue {
                key: c.to_string(),
                value: "1".to_string(),
            })
            .collect()
    }

    fn reduce(&self, _key: &str, _values: &[String]) -> String {
        nparallel("reduce").to_string()
    }
}
