use crate::mrapps::wc::{KeyValue, MapReduce};
use itertools::Itertools;
use rand::rngs::OsRng;
use rand::Rng;
use std::process;
use std::thread::sleep;
use std::time::Duration;

pub struct CrashMapReduce;

fn maybe_crash() {
    let mut rng = OsRng;
    let rr: u64 = rng.gen_range(0..1000);

    if rr < 330 {
        process::exit(1);
    } else if rr < 660 {
        let ms = rng.gen_range(0..10000);
        sleep(Duration::from_millis(ms));
    }
}

impl MapReduce for CrashMapReduce {
    fn map(&self, filename: &str, contents: &str) -> Vec<KeyValue> {
        maybe_crash();

        let mut kva: Vec<KeyValue> = vec![];

        kva.push(KeyValue {
            key: "a".to_string(),
            value: filename.to_string(),
        });

        kva.push(KeyValue {
            key: "b".to_string(),
            value: filename.len().to_string(),
        });

        kva.push(KeyValue {
            key: "c".to_string(),
            value: contents.len().to_string(),
        });

        kva.push(KeyValue {
            key: "d".to_string(),
            value: "xyzzy".to_string(),
        });

        kva
    }

    fn reduce(&self, _key: &str, values: &[String]) -> String {
        maybe_crash();

        let mut vv = values.to_vec();
        vv.sort();
        vv.iter().join(" ")
    }
}
