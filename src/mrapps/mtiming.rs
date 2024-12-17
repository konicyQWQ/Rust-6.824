use crate::mrapps::wc::{KeyValue, MapReduce};
use std::fs;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use nix::unistd::Pid;

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

pub struct MTimingMapReduce;

impl MapReduce for MTimingMapReduce {
    fn map(&self, _filename: &str, _contents: &str) -> Vec<KeyValue> {
        let t0 = SystemTime::now();
        let ts = t0.duration_since(SystemTime::UNIX_EPOCH).unwrap();
        let ts = ts.as_secs() as f64 + ts.as_nanos() as f64 / 1000000000.0;
        let pid = std::process::id();

        let n = nparallel("map");

        vec![
            KeyValue {
                key: format!("times-{}", pid),
                value: format!("{:.1}", ts),
            },
            KeyValue {
                key: format!("parallel-{}", pid),
                value: format!("{}", n),
            },
        ]
    }

    fn reduce(&self, _key: &str, values: &[String]) -> String {
        let mut vv = values.to_vec();
        vv.sort();

        vv.join(" ")
    }
}
