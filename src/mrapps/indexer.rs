use crate::mrapps::wc::{KeyValue, MapReduce};
use std::collections::HashMap;

pub struct IndexerMapReduce;

impl MapReduce for IndexerMapReduce {
    fn map(&self, filename: &str, contents: &str) -> Vec<KeyValue> {
        let words: Vec<&str> = contents
            .split(|c: char| !c.is_alphabetic())
            .filter(|s| !s.is_empty())
            .collect();
        let mut m: HashMap<&str, bool> = HashMap::new();

        for word in words {
            m.insert(word, true);
        }

        m.keys()
            .map(|k| KeyValue {
                key: k.to_string(),
                value: filename.to_string(),
            })
            .collect()
    }

    fn reduce(&self, _key: &str, values: &[String]) -> String {
        let mut vv = values.to_vec();
        vv.sort();

        format!("{} {}", vv.len(), values.join(","))
    }
}
