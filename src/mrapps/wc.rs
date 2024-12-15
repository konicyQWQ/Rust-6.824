#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

pub trait MapReduce {
    fn map(&self, filename: &str, contents: &str) -> Vec<KeyValue>;

    fn reduce(&self, key: &str, values: &[String]) -> String;
}

pub struct WcMapReduce;

impl MapReduce for WcMapReduce {
    fn map(&self, _filename: &str, contents: &str) -> Vec<KeyValue> {
        contents
            .split(|c: char| !c.is_alphabetic())
            .filter(|&s| !s.is_empty())
            .map(|s| KeyValue {
                key: s.to_string(),
                value: "1".to_string(),
            })
            .collect()
    }

    fn reduce(&self, _key: &str, values: &[String]) -> String {
        values.len().to_string()
    }
}
