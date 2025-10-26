use std::env;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use log::{debug, warn};
use sp_api::BlockT;
use sp_runtime::{PerThing, Percent, Rounding};
use crate::BlockExecuteInfo;

pub const DEFAULT_BLOCK_SIZE_LIMIT: usize = 4 * 1024 * 1024 + 512;

pub trait BlockOracle<B: BlockT> {
    /// update block_duration.
    fn update_block_duration(&self, time: Duration);
    /// update oracle by last block execute info.
    fn update_execute_info(&self, info: &BlockExecuteInfo<B>);
    /// get expected block_duration.
    fn block_duration(&self) -> Duration;
    /// estimate thread_limit.
    fn thread_limit(&self) -> usize;
    /// each round execute transaction number(for BlockBuilder::put_batch).
    fn round_tx(&self) -> usize;
    /// estimate linear execute time.
    fn linear_execute_time(&self) -> Duration;
    /// estimate minimal single thread transactions(in case no time for single thread).
    fn min_single_tx(&self) -> usize;
    /// estimate pool time for get transaction from pool.
    fn pool_time(&self) -> Duration;
    /// estimate merge time.
    fn merge_time(&self) -> Duration;
    /// estimate one thread execution limit(also limit transactions from pool).
    fn thread_execution_limit(&self) -> Option<usize>;
    /// estimate linear transaction execute limit.
    fn linear_tx_limit(&self) -> usize;
    /// return block size limit.
    fn block_size_limit(&self) -> usize;
    /// estimate total transaction limit when get from pool, it can be greater than actual value.
    fn total_tx_limit(&self) -> usize {
        let linear_tx_limit = self.linear_tx_limit();
        linear_tx_limit * self.thread_limit() * 3 / 2
    }
}

#[derive(Default)]
pub struct ExecutionOracle<B: BlockT> {
    pub thread_limit: usize,
    /// minimal single thread tx number(if exists, ensure some unhandled transaction to be handled, default 10).
    pub min_single_tx: usize,
    /// each round execute transactions(for BlockBuilder::put_batch, default 500).
    pub round_tx: usize,
    /// expected block execute time(default 200 millis, g.e than slot_duration).
    pub block_duration: Arc<Mutex<Duration>>,
    /// max time for get transactions from pool(default 10%).
    pub pool_percent: Arc<Mutex<Percent>>,
    /// `max_block_duration * execution_percent` estimates `linear_execution_time`(default 55%).
    pub execution_percent: Arc<Mutex<Percent>>,
    /// `linear_execution_time * merge_percent` estimates execution results `merge_time`(default 15%).
    pub merge_percent: Arc<Mutex<Percent>>,
    /// average transaction execute time with weight.
    pub execute_time_per_tx: Arc<Mutex<(Duration, usize)>>,
    pub block_size_limit: usize,
    phantom: PhantomData<B>,
}

impl<B: BlockT> ExecutionOracle<B> {
    pub fn new(block_size_limit: Option<usize>) -> Self {
        let thread_limit: usize = match env::var("ORACLE_THREAD_LIMIT") {
            Ok(limit) => limit.parse().expect("`ORACLE_THREAD_LIMIT` should be a usize"),
            Err(_) => num_cpus::get(),
        };
        let mut block_duration = 200;
        if let Ok(value) = env::var("ORACLE_BLOCK_DURATION") {
            if let Ok(duration) = value.parse::<u64>() {
                block_duration = duration;
            }
        }
        let min_single_tx: usize = match env::var("ORACLE_SINGLE_MIN") {
            Ok(tx) => tx.parse().expect("`ORACLE_SINGLE_MIN` should be a usize"),
            Err(_) => 10,
        };
        let round_tx: usize = match env::var("ORACLE_ROUND_TX") {
            Ok(tx) => tx.parse().expect("`ORACLE_ROUND_TX` should be a usize"),
            Err(_) => 500,
        };
        let mut pool_percent = Percent::from_percent(10);
        if let Ok(value) = env::var("ORACLE_POOL_PERCENT") {
            if let Ok(percent) = value.parse::<u8>() {
                pool_percent = Percent::from_percent(percent);
            }
        }
        let mut execution_percent = Percent::from_percent(55);
        if let Ok(value) = env::var("ORACLE_EXECUTION_PERCENT") {
            if let Ok(percent) = value.parse::<u8>() {
                execution_percent = Percent::from_percent(percent);
            }
        }
        let mut merge_percent = Percent::from_percent(15);
        if let Ok(value) = env::var("ORACLE_MERGE_PERCENT") {
            if let Ok(percent) = value.parse::<u8>() {
                merge_percent = Percent::from_percent(percent);
            }
        }
        Self {
            thread_limit,
            min_single_tx,
            round_tx,
            block_duration: Arc::new(Mutex::new(Duration::from_millis(block_duration))),
            pool_percent: Arc::new(Mutex::new(pool_percent)),
            execution_percent: Arc::new(Mutex::new(execution_percent)),
            merge_percent: Arc::new(Mutex::new(merge_percent)),
            execute_time_per_tx: Arc::new(Mutex::new((Duration::from_micros(400), 0))),
            block_size_limit: block_size_limit.unwrap_or(DEFAULT_BLOCK_SIZE_LIMIT),
            phantom: PhantomData,
        }
    }

    pub fn block_duration(&self) -> Duration {
        self.block_duration.lock().unwrap().clone()
    }

    pub fn min_single_tx(&self) -> usize {
        self.min_single_tx
    }

    fn update_execute_time_per_tx(&self, info: &BlockExecuteInfo<B>) -> Option<(Duration, Duration)> {
        let mut execute_time_per_tx = None;
        let (ave_execute_time, weight) = info.avg_time(true);
        if ave_execute_time > Duration::default() {
            let (pre_execute_time_per_tx, pre_weight) = *self.execute_time_per_tx.lock().unwrap();
            if weight > 0 {
                let mut total_weight = (pre_weight + weight) as u128;
                let new_exe_cute_time_per_tx = ave_execute_time.as_micros() * weight as u128 / total_weight
                    + pre_execute_time_per_tx.as_micros() * pre_weight as u128 / total_weight;
                let new_avg_execute_time = Duration::from_micros(new_exe_cute_time_per_tx as u64);
                // limit weight
                if total_weight >> 31 > 0 {
                    total_weight = total_weight >> 1;
                }
                *self.execute_time_per_tx.lock().unwrap() = (new_avg_execute_time, total_weight as usize);
                execute_time_per_tx = Some((ave_execute_time, new_avg_execute_time));
            }
        }
        execute_time_per_tx
    }

    fn update_percents(&self, info: &BlockExecuteInfo<B>) -> Option<String> {
        let block = info.number;
        if info.is_empty_block() {
            return None;
        }
        let pool_percent = Percent::from_rational(info.group.time.as_micros(), info.time.as_micros());
        let merge_percent = Percent::from_rational_with_rounding(info.merge.extra_merge_time.as_micros(), info.time.as_micros(), Rounding::NearestPrefUp).ok()?;
        let finalize_percent = Percent::from_rational(info.finalize.as_micros(), info.time.as_micros());
        let except_execute_percent = pool_percent + merge_percent + finalize_percent;
        if except_execute_percent < Percent::one() {
            let execute_percent = Percent::one() - except_execute_percent;
            let mut update = "".to_string();
            if !pool_percent.is_zero() {
                *self.pool_percent.lock().unwrap() = pool_percent;
                update += &format!(" pool {:?}", pool_percent);
            }
            if !execute_percent.is_zero() {
                *self.execution_percent.lock().unwrap() = execute_percent;
                update += &format!(" execute {:?}", execute_percent);
            }
            if !merge_percent.is_zero() {
                *self.merge_percent.lock().unwrap() = merge_percent;
                update += &format!(" merge {:?}", merge_percent);
            }
            if !update.is_empty() {
                update += &format!(" finalize {:?}", finalize_percent);
            }
            if !update.is_empty() { return Some(update); }
        } else {
            warn!(target: "oracle_exec", "[Update] Block {block} (pool({pool_percent:?}) + merge({merge_percent:?}) + finalize({finalize_percent:?})) > 100% with no time for execute!!!");
        }
        None
    }
}

impl<B: BlockT> BlockOracle<B> for ExecutionOracle<B> {
    fn update_block_duration(&self, time: Duration) {
        *self.block_duration.lock().unwrap() = time;
        debug!(target: "oracle_exec", "[Update] block_duration {:?} micros", time.as_micros());
    }

    fn update_execute_info(&self, info: &BlockExecuteInfo<B>) {
        // 1. update average transaction execute time.
        let update_avg = self.update_execute_time_per_tx(&info);
        // 2. updated pool_percent/execute_percent/merge_percent by execute info
        let update_percent = self.update_percents(&info).unwrap_or_default();

        // debug info
        let mut update_info = "".to_string();
        if let Some((exe_avg, new_avg_tx)) = update_avg {
            update_info += &format!(" exe_avg {} micros, new_avg_tx {} micros", exe_avg.as_micros(), new_avg_tx.as_micros());
        }
        if !update_percent.is_empty() || !update_info.is_empty() {
            let full_millis = info.time.as_millis();
            let block_duration = self.block_duration().as_millis();
            let (mth_t, mth_n) = info.mth_applied();
            let mth_info = if mth_t > 0 || mth_n > 0 {
                format!(" {mth_n}({mth_t})")
            } else {
                "".to_string()
            };
            let single_info = if let Some(applied) = info.single_applied() {
                format!(" {applied}")
            } else {
                "".to_string()
            };
            debug!(target: "oracle_exec", "[Update] Block {} ({full_millis}/{block_duration} ms{mth_info}{single_info}{update_percent}){update_info}", info.number);
        }
    }

    fn block_duration(&self) -> Duration {
        self.block_duration()
    }

    fn thread_limit(&self) -> usize { self.thread_limit }

    fn round_tx(&self) -> usize { self.round_tx }

    /// we allow full block execution time is slot_duration * 2.
    /// and default 70 percent block time to execute extrinsic.
    fn linear_execute_time(&self) -> Duration {
        Duration::from_micros(self.execution_percent.lock().unwrap().mul_floor(self.block_duration().as_micros()) as u64)
    }

    fn min_single_tx(&self) -> usize {
        self.min_single_tx
    }

    fn pool_time(&self) -> Duration {
        Duration::from_micros(self.pool_percent.lock().unwrap().mul_floor(self.block_duration().as_micros()) as u64)
    }

    /// estimate merge time.
    /// default 20 percent of `linear_execute_time`.
    fn merge_time(&self) -> Duration {
        Duration::from_micros(self.merge_percent.lock().unwrap().mul_floor(self.linear_execute_time().as_micros()) as u64)
    }

    /// one thread execution limit (multi_max + single) to limit get extrinsic from pool.
    fn thread_execution_limit(&self) -> Option<usize> {
        let (execute_time_per_tx, _) = *self.execute_time_per_tx.lock().unwrap();
        if execute_time_per_tx == Duration::default() {
            return None;
        }
        Some(self.linear_execute_time().as_micros() as usize / execute_time_per_tx.as_micros() as usize)
    }

    fn linear_tx_limit(&self) -> usize {
        self.thread_execution_limit().unwrap_or(200)
    }

    fn block_size_limit(&self) -> usize {
        self.block_size_limit
    }
}