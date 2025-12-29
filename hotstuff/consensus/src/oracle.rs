use std::collections::{BTreeMap, HashSet};
use std::env;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use sc_basic_authorship::{BlockExecuteInfo, BlockOracle, GroupInfo};
use sp_api::{BlockT, HeaderT};
use sp_runtime::Percent;

pub trait HotsOracle<B: BlockT> {
    /// update state by group info
    fn update_group_info(&self, info: &GroupInfo);
    /// update verify speed by verify results(transaction number and time(micros)).
    fn update_verify_times(&self, verify_times: &Vec<(usize, Duration)>);
    /// estimate verify transaction time limit.
    fn verify_time_limit(&self) -> Duration;
    /// estimate transaction verify number(which is used for transaction propose).
    fn thread_verify_limit(&self) -> Option<usize>;
    /// transactions hash should be filtered when propose.
    fn filter_transactions(&self) -> HashSet<B::Hash>;
}

#[derive(Default)]
pub struct HotstuffOracle<B: BlockT, O: BlockOracle<B>> {
    pub inner: Arc<O>,
    /// max hotstuff time decide extrinsic verify time tolerance(default 200 millis).
    pub hotstuff_duration: Duration,
    /// `hotstuff_duration * verify_percent` estimates extrinsic verify time.
    pub verify_percent: Arc<Mutex<Percent>>,
    /// state recording last verify extrinsic speed.
    pub verify_time_per_tx: Arc<Mutex<Duration>>,
    /// config filter blocks.
    pub filter_blocks: u32,
    /// filter all applied transaction.
    pub transaction_filter: Arc<Mutex<BTreeMap<<B::Header as HeaderT>::Number, HashSet<B::Hash>>>>,
    /// Network config hotstuff protocol size limit, which will influence block transaction size.
    /// Default 5M Bytes(1024 *1024 * 5).
    pub network_notification_limit: usize,
    phantom: PhantomData<B>,
}

impl<B: BlockT, O: BlockOracle<B>> HotstuffOracle<B, O> {
    pub fn new(inner: Arc<O>, network_notification_limit: Option<usize>) -> Self {
        // default same with block_duration. only necessary set for Hotstuff consensus.
        let mut hotstuff_duration = inner.block_duration();
        if let Ok(value) = env::var("HOTSTUFF_DURATION") {
            if let Ok(duration) = value.parse::<u64>() {
                hotstuff_duration = Duration::from_millis(duration);
            }
        }
        let consensus_time = hotstuff_duration.saturating_sub(inner.pool_time());
        let consensus_percent = Percent::from_rational(consensus_time.as_micros(), hotstuff_duration.as_micros());
        let mut verify_percent = Percent::from_percent(75);
        if let Ok(value) = env::var("HOTSTUFF_VERIFY_PERCENT") {
            if let Ok(percent) = value.parse::<u8>() {
                verify_percent = Percent::from_percent(percent);
            }
        }
        verify_percent = verify_percent * consensus_percent;

        let mut filter_blocks = 50u32;
        if let Ok(value) = env::var("HOTSTUFF_FILTER_BLOCKS") {
            if let Ok(blocks) = value.parse::<u32>() {
                filter_blocks = blocks.max(1);
            }
        }
        Self {
            inner,
            hotstuff_duration,
            verify_percent: Arc::new(Mutex::new(verify_percent)),
            verify_time_per_tx: Arc::new(Mutex::new(Duration::from_micros(200))),
            filter_blocks,
            transaction_filter: Arc::new(Mutex::new(BTreeMap::new())),
            network_notification_limit: network_notification_limit.unwrap_or(1024 * 1024 * 5),
            phantom: PhantomData,
        }
    }
}

impl <B: BlockT, O: BlockOracle<B>> HotstuffOracle<B, O> {
    pub fn update_block_transactions(&self, info: &BlockExecuteInfo<B>) {
        let transactions: HashSet<_> = info.threads.iter().map(|t| t.1.transactions.clone()).flatten().collect();
        let mut filter =  self.transaction_filter.lock().unwrap();
        let mut blocks: Vec<_> = filter.keys().cloned().collect();
        blocks.sort_by(|a, b| b.cmp(&a));
        if blocks.len() >= self.filter_blocks as usize {
            for index in self.filter_blocks as usize - 1..blocks.len() - 1 {
                filter.remove(&blocks[index]);
            }
        }
        filter.insert(info.number, transactions);
    }
}

impl<B: BlockT, O: BlockOracle<B>> HotsOracle<B> for HotstuffOracle<B, O> {
    fn update_group_info(&self, info: &GroupInfo) {
        let group_info = info.info();
        if !group_info.is_empty() {
            log::debug!(target: "oracle_hots", "[Update] {group_info}");
        }
    }

    // each value at input should be (verify_number, verify_micros)
    fn update_verify_times(&self, verify_times: &Vec<(usize, Duration)>) {
        // TODO update verify_percent by full_time and hotstuff_duration?
        let mut total_number = 0;
        let mut total_time = Duration::default();
        for (number, time) in verify_times {
            if *number == 0 {
                continue;
            }
            total_number += *number;
            total_time += *time;
        }
        if total_number > 0 {
            let time_per_verify = total_time / total_number as u32;
            let pre_verify_time_per_tx = *self.verify_time_per_tx.lock().unwrap();
            if pre_verify_time_per_tx != time_per_verify {
                *self.verify_time_per_tx.lock().unwrap() = time_per_verify;
                log::debug!(target: "oracle_hots", "[Update] verify_time_per_tx: {}μs", time_per_verify.as_micros());
            }
        }
    }

    fn verify_time_limit(&self) -> Duration {
        // Default we use 75%  `HotstuffDuration` for verify extrinsic
        Duration::from_micros(self.verify_percent.lock().unwrap().mul_floor(self.hotstuff_duration.as_micros() as u64))
    }

    /// verify time should < slot_duration to finish consensus
    /// limited extrinsic number for thread verify time.
    fn thread_verify_limit(&self) -> Option<usize> {
        let verify_time_per_tx = *self.verify_time_per_tx.lock().unwrap();
        if verify_time_per_tx == Duration::default() {
            return None;
        }
        // During full consensus process, verify extrinsic takes most time.
        Some(self.verify_time_limit().as_micros() as usize / verify_time_per_tx.as_micros() as usize)
    }

    fn filter_transactions(&self) -> HashSet<B::Hash> {
        self.transaction_filter.lock().unwrap().values().cloned().flatten().collect()
    }
}

impl<B: BlockT, O: BlockOracle<B>> BlockOracle<B> for  HotstuffOracle<B, O> {
    fn update_block_duration(&self, time: Duration) {
        self.inner.update_block_duration(time);
    }

    fn update_execute_info(&self, info: &BlockExecuteInfo<B>) {
        self.inner.update_execute_info(info);
        self.update_block_transactions(info);
    }

    fn block_duration(&self) -> Duration {
        self.inner.block_duration()
    }

    fn thread_limit(&self) -> usize {
        self.inner.thread_limit()
    }

    fn round_tx(&self) -> usize {
        self.inner.round_tx()
    }

    fn linear_execute_time(&self) -> Duration {
        self.inner.linear_execute_time()
    }

    fn min_single_tx(&self) -> usize {
        self.inner.min_single_tx()
    }

    fn pool_time(&self) -> Duration {
        self.inner.pool_time()
    }

    fn merge_time(&self) -> Duration {
        self.inner.merge_time()
    }

    fn thread_execution_limit(&self) -> Option<usize> {
        self.inner.thread_execution_limit()
    }

    fn linear_tx_limit(&self) -> usize {
        match (self.thread_verify_limit(), self.thread_execution_limit()) {
            (None, execution_limit) => execution_limit,
            (verify_limit, None) => verify_limit,
            (Some(verify_limit), Some(execution_limit)) => {
                Some(verify_limit.min(execution_limit))
            }
        }
            .unwrap_or(200)
    }

    fn block_size_limit(&self) -> usize {
        // both decide by execute block size limit and `network_notification_limit`
        self.inner.block_size_limit().min(self.network_notification_limit - 1000)
    }
}
