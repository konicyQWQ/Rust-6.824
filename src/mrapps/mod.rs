use crate::mrapps::wc::{MapReduce, WcMapReduce};

pub mod wc;

pub fn get_map_reduce(name: &str) -> Option<Box<dyn MapReduce>> {
    match name {
        "wc" => Some(Box::new(WcMapReduce)),
        _ => None,
    }
}
