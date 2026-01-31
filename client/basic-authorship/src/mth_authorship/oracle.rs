use std::env;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use log::{debug, warn};
use sp_api::BlockT;
use sp_runtime::{PerThing, Permill, Rounding};
use crate::BlockExecuteInfo;

pub const DEFAULT_BLOCK_SIZE_LIMIT: usize = 4 * 1024 * 1024 + 512;

pub const EXECUTE_WINDOW_SIZE: usize = 30000;

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
    pub pool_permill: Arc<Mutex<Permill>>,
    /// `max_block_duration * execution_permill` estimates `linear_execution_time`(default 55%).
    pub execution_permill: Arc<Mutex<Permill>>,
    /// `linear_execution_time * merge_permill` estimates execution results `merge_time`(default 15%).
    pub merge_permill: Arc<Mutex<Permill>>,
    /// estimates time for import.
    pub import_permill: Arc<Mutex<Permill>>,
    /// average transaction execute time with weight.
    ///
    /// window `[(tx_number, Duration)]`, sum of `tx_number` should not grater than total window number sum limit
    pub execute_time_per_tx: Arc<Mutex<(Duration, Vec<(usize, Duration)>)>>,
    /// Limit of window number, total window number sum limit.
    pub execute_avg_window: (usize, usize),
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
        let mut pool_permill = Permill::from_percent(10);
        if let Ok(value) = env::var("ORACLE_POOL_PERCENT") {
            if let Ok(percent) = value.parse::<u32>() {
                pool_permill = Permill::from_percent(percent);
            }
        }
        let mut execution_permill = Permill::from_percent(80);
        if let Ok(value) = env::var("ORACLE_EXECUTION_PERCENT") {
            if let Ok(percent) = value.parse::<u32>() {
                execution_permill = Permill::from_percent(percent);
            }
        }
        let mut merge_permill = Permill::from_percent(5);
        if let Ok(value) = env::var("ORACLE_MERGE_PERCENT") {
            if let Ok(percent) = value.parse::<u32>() {
                merge_permill = Permill::from_percent(percent);
            }
        }
        let mut execute_avg_window_num = 5;
        if let Ok(num) = env::var("ORACLE_EXECUTE_WINDOW_NUM") {
            if let Ok(num) = num.parse::<usize>() {
                execute_avg_window_num = num;
            }
        }
        let mut execute_avg_window_size = 30000;
        if let Ok(size) = env::var("ORACLE_EXECUTE_WINDOW_SIZE") {
            if let Ok(size) = size.parse::<usize>() {
                execute_avg_window_size = size;
            }
        }
        Self {
            thread_limit,
            min_single_tx,
            round_tx,
            block_duration: Arc::new(Mutex::new(Duration::from_millis(block_duration))),
            pool_permill: Arc::new(Mutex::new(pool_permill)),
            execution_permill: Arc::new(Mutex::new(execution_permill)),
            merge_permill: Arc::new(Mutex::new(merge_permill)),
            import_permill: Arc::new(Mutex::new(Permill::from_percent(15))),
            execute_time_per_tx: Arc::new(Mutex::new((Duration::from_micros(400), vec![]))),
            block_size_limit: block_size_limit.unwrap_or(DEFAULT_BLOCK_SIZE_LIMIT),
            execute_avg_window: (execute_avg_window_num, execute_avg_window_size),
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
        let (avg_execute_time, tx_number) = info.avg_time();
        if avg_execute_time > Duration::default() {
            let (_, prev_window) = self.execute_time_per_tx.lock().unwrap().clone();
            let (new_avg_execute_time, new_window) = if tx_number >= self.execute_avg_window.1 {
                (avg_execute_time, vec![(self.execute_avg_window.1, avg_execute_time)])
            } else {
                let mut total_time = avg_execute_time.as_nanos() * tx_number as u128;
                let mut new_window = vec![(tx_number, avg_execute_time)];
                let mut remain = self.execute_avg_window.1 - tx_number;
                for (number, avg_time) in prev_window {
                    if new_window.len() >= self.execute_avg_window.0 {
                        new_window.truncate(self.execute_avg_window.0);
                        break;
                    }
                    if number >= remain {
                        total_time += avg_time.as_nanos() * remain as u128;
                        new_window.push((remain, avg_time));
                        break;
                    } else {
                        remain = remain.saturating_sub(number);
                        total_time += avg_time.as_nanos() * number as u128;
                        new_window.push((number, avg_time));
                    }
                }
                let new_avg_nanos = total_time / self.execute_avg_window.1.saturating_sub(remain) as u128;
                (Duration::from_nanos(new_avg_nanos as u64), new_window)
            };
            *self.execute_time_per_tx.lock().unwrap() = (new_avg_execute_time, new_window);
            execute_time_per_tx = Some((avg_execute_time, new_avg_execute_time));
        }
        execute_time_per_tx
    }

    fn update_permills(&self, info: &BlockExecuteInfo<B>) -> Option<String> {
        let block = info.number;
        if info.is_empty_block() {
            return None;
        }
        let full_time = info.time.as_nanos() + info.import.as_nanos();
        let import_permill = Permill::from_rational(info.import.as_nanos(), full_time);
        let mut update_import = false;
        if full_time >= self.block_duration.lock().unwrap().as_micros() / 5 {
            *self.import_permill.lock().unwrap() = import_permill;
            update_import = true;
        }

        let pool_permill = Permill::from_rational(info.group.time.as_micros(), info.time.as_micros());
        let merge_permill = Permill::from_rational_with_rounding(info.merge.extra_merge_time.as_micros(), info.time.as_micros(), Rounding::NearestPrefUp).ok()?;
        let finalize_permill = Permill::from_rational(info.finalize.as_micros(), info.time.as_micros());
        let except_execute_permill = pool_permill + merge_permill + finalize_permill;
        if except_execute_permill < Permill::one() {
            let mut update = "".to_string();
            let full_execute_permill = Permill::one() - import_permill;
            let execute_permill = Permill::one() - except_execute_permill;
            if !pool_permill.is_zero() {
                *self.pool_permill.lock().unwrap() = pool_permill;
                update += &format!(" pool {:?}", pool_permill * full_execute_permill);
            }
            if !execute_permill.is_zero() {
                *self.execution_permill.lock().unwrap() = execute_permill;
                update += &format!(" execute {:?}", execute_permill * full_execute_permill);
            }
            if !merge_permill.is_zero() {
                *self.merge_permill.lock().unwrap() = merge_permill;
                update += &format!(" merge {:?}", merge_permill * full_execute_permill);
            }
            if !update.is_empty() {
                update += &format!(" finalize {:?}", finalize_permill * full_execute_permill);
            }
            update += &format!(" import {:?}({update_import})", import_permill);
            if !update.is_empty() { return Some(update); }
        } else {
            warn!(target: "oracle_exec", "[Update] Block {block} (pool({pool_permill:?}) + merge({merge_permill:?}) + finalize({finalize_permill:?})) > 100% with no time for execute!!!");
        }
        None
    }
}

impl<B: BlockT> BlockOracle<B> for ExecutionOracle<B> {
    fn update_block_duration(&self, time: Duration) {
        *self.block_duration.lock().unwrap() = time;
        debug!(target: "oracle_exec", "[Update] block_duration {:?}μs", time.as_micros());
    }

    fn update_execute_info(&self, info: &BlockExecuteInfo<B>) {
        // 1. update average transaction execute time.
        let update_avg = self.update_execute_time_per_tx(&info);
        // 2. updated pool_permill/execute_permill/merge_permill by execute info
        let update_permill = self.update_permills(&info).unwrap_or_default();

        // debug info
        let mut update_info = "".to_string();
        if let Some((exe_avg, new_avg_tx)) = update_avg {
            update_info += &format!(" exe_avg {}μs, new_avg_tx {}μs", exe_avg.as_micros(), new_avg_tx.as_micros());
        }
        if !update_permill.is_empty() || !update_info.is_empty() {
            let full_millis = info.time.as_millis() + info.import.as_millis();
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
            debug!(target: "oracle_exec", "[Update] Block {} ({full_millis}/{block_duration} ms{mth_info}{single_info}{update_permill}){update_info}", info.number);
        }
    }

    fn block_duration(&self) -> Duration {
        self.block_duration()
    }

    fn thread_limit(&self) -> usize { self.thread_limit.max(1) }

    fn round_tx(&self) -> usize { self.round_tx }

    /// we allow full block execution time is slot_duration * 2.
    /// and default 80 percent block time to execute extrinsic.
    fn linear_execute_time(&self) -> Duration {
        let execute_time = self.block_duration().as_micros().saturating_sub(self.import_permill.lock().unwrap().mul_floor(self.block_duration().as_micros()));
        Duration::from_micros(self.execution_permill.lock().unwrap().mul_floor(execute_time) as u64)
    }

    fn min_single_tx(&self) -> usize {
        self.min_single_tx
    }

    fn pool_time(&self) -> Duration {
        Duration::from_micros(self.pool_permill.lock().unwrap().mul_floor(self.block_duration().as_micros()) as u64)
    }

    /// estimate merge time.
    /// default 5 permill of `linear_execute_time`.
    fn merge_time(&self) -> Duration {
        Duration::from_micros(self.merge_permill.lock().unwrap().mul_floor(self.linear_execute_time().as_micros()) as u64)
    }

    /// one thread execution limit (multi_max + single) to limit get extrinsic from pool.
    fn thread_execution_limit(&self) -> Option<usize> {
        let (execute_time_per_tx, _) = *self.execute_time_per_tx.lock().unwrap();
        if execute_time_per_tx == Duration::default() {
            return None;
        }
        Some((self.linear_execute_time().as_micros() as usize / execute_time_per_tx.as_micros() as usize).max(1))
    }

    fn linear_tx_limit(&self) -> usize {
        self.thread_execution_limit().unwrap_or(200)
    }

    fn block_size_limit(&self) -> usize {
        self.block_size_limit
    }
}