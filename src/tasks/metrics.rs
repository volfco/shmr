// Metric Collection Workers

use crate::config::ShmrFsConfig;
use crate::tasks::WorkerTask;
use crate::databunny::DataBunny;
use crate::types::SuperblockEntry;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct MetricCollector {
    config: ShmrFsConfig,
    superblock: DataBunny<u64, SuperblockEntry>,
}

impl WorkerTask for MetricCollector {
    fn execute(&self) {


    }
}

