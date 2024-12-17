use crate::mrapps::crash::CrashMapReduce;
use crate::mrapps::early_exit::EarlyExitMapReduce;
use crate::mrapps::indexer::IndexerMapReduce;
use crate::mrapps::jobcount::JobCountMapReduce;
use crate::mrapps::mtiming::MTimingMapReduce;
use crate::mrapps::nocrash::NoCrashMapReduce;
use crate::mrapps::rtiming::RTimingMapReduce;
use crate::mrapps::wc::{MapReduce, WcMapReduce};

pub mod crash;
pub mod early_exit;
pub mod indexer;
pub mod jobcount;
pub mod mtiming;
pub mod nocrash;
pub mod rtiming;
pub mod wc;

pub fn get_map_reduce(name: &str) -> Option<Box<dyn MapReduce>> {
    match name {
        "wc" => Some(Box::new(WcMapReduce)),
        "nocrash" => Some(Box::new(NoCrashMapReduce)),
        "crash" => Some(Box::new(CrashMapReduce)),
        "early_exit" => Some(Box::new(EarlyExitMapReduce)),
        "indexer" => Some(Box::new(IndexerMapReduce)),
        "jobcount" => Some(Box::new(JobCountMapReduce)),
        "rtiming" => Some(Box::new(RTimingMapReduce)),
        "mtiming" => Some(Box::new(MTimingMapReduce)),
        _ => None,
    }
}
