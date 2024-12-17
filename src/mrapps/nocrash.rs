use crate::mrapps::wc::{KeyValue, MapReduce};
use itertools::Itertools;

pub struct NoCrashMapReduce;

impl MapReduce for NoCrashMapReduce {
    fn map(&self, filename: &str, contents: &str) -> Vec<KeyValue> {
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
        let mut vv = values.to_vec();
        vv.sort();
        vv.iter().join(" ")
    }
}
