use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use codec::{Decode, Encode};
use sc_transaction_pool_api::TransactionPool;
use sp_runtime::traits::{Block as BlockT, Header as HeaderT};

pub struct InfoRecorder<B: BlockT> {
    pub number: <B::Header as HeaderT>::Number,
    pub max_time: Duration,
    pub block_start: Option<Instant>,
    pub finalize_start: Option<Instant>,
    pub group: GroupInfo,
    pub inherent: Option<ThreadExecutionInfo<B>>,
    /// Record all thread execute info(single thread number should be 0).
    pub threads: HashMap<usize, ThreadExecutionInfo<B>>,
    pub merge: Option<MergeInfo>,
    pub native: bool,
}

impl<B: BlockT> InfoRecorder<B> {
    pub fn new() -> Self {
        InfoRecorder {
            number: 0u32.into(),
            max_time: Default::default(),
            block_start: None,
            finalize_start: None,
            inherent: None,
            group: Default::default(),
            threads: HashMap::new(),
            merge: None,
            native: true,
        }
    }

    pub fn number(mut self, number: <B::Header as HeaderT>::Number) -> Self {
        self.number = number;
        self
    }

    pub fn max_time(mut self, time: Duration) -> Self {
        self.max_time = time;
        self
    }
    
    pub fn start(mut self) -> Self {
        self.block_start = Some(Instant::now());
        self
    }

    pub fn set_group_info(&mut self, info: GroupInfo) {
        self.group = info;
    }
    
    pub fn finalize_start(&mut self) {
        self.finalize_start = Some(Instant::now());
    }
    
    pub fn finalize(mut self) -> BlockExecuteInfo<B> {
        BlockExecuteInfo {
            number: self.number,
            max_time: self.max_time,
            finalize: self.finalize_start.take().expect("Finalize time not initialized").elapsed(),
            time: self.block_start.take().expect("BlockStart time not initialized").elapsed(),
            group: self.group,
            inherent: self.inherent.unwrap_or_default(),
            threads: self.threads,
            merge: self.merge.unwrap_or_default(),
            native: self.native,
        }
    }

    pub fn inherent_finish(&mut self, info: ThreadExecutionInfo<B>) {
        self.inherent = Some(info);
    }

    pub fn thread_finish(&mut self, info: ThreadExecutionInfo<B>) {
        self.threads.insert(info.thread, info);
    }

    pub fn merge_finish(&mut self, info: MergeInfo) {
        self.merge = Some(info);
    }
}

#[derive(Debug, Clone)]
pub struct BlockExecuteInfo<B: BlockT> {
    pub number: <B::Header as HeaderT>::Number,
    pub max_time: Duration,
    /// block finalize time(finalize + build).
    pub finalize: Duration,
    /// full block execute time.
    pub time: Duration,
    /// group transactions info from pool, not necessary.
    pub group: GroupInfo,
    pub inherent: ThreadExecutionInfo<B>,
    /// Record all thread execute info(single thread number should be 0).
    pub threads: HashMap<usize, ThreadExecutionInfo<B>>,
    pub merge: MergeInfo,
    pub native: bool,
}

impl<B: BlockT> BlockExecuteInfo<B> {
    pub fn set_group_info(&mut self, group: GroupInfo) {
        self.group = group;
    }

    pub fn is_empty_block(&self) -> bool {
        if self.threads.is_empty() {
            return true;
        }
        for info in self.threads.values() {
            if info.total > 0 {
                return false;
            }
        }
        true
    }

    /// Return each thread applied transactions.
    /// First should be inherent(must exist)
    /// Thread 0 should at last(must exist).
    pub fn groups(&self) -> Vec<u32> {
        let mut group_infos: Vec<_> = self.threads.values().cloned().collect();
        group_infos.sort_by(|a, b| a.thread.cmp(&b.thread));
        if self.threads.get(&0).is_some() {
            // move thread 0 to last.
            let thread_zero = group_infos.remove(0);
            group_infos.push(thread_zero);
        } else {
            // insert default empty thread 0.
            group_infos.push(ThreadExecutionInfo::default());
        }
        let group_infos = [vec![self.inherent.clone()], group_infos].concat();
        group_infos.iter().map(|info| info.total as u32).collect()
    }

    pub fn avg_time(&self) -> (Duration, usize) {
        if !self.threads.is_empty() {
            let mut weight = 0;
            let times: Vec<_> = self.threads
                .iter()
                .map(|(_, info)| info.avg_time())
                .filter(|(t, _)| t > &Duration::default())
                .map(|(t, w)| { weight += w; t })
                .collect();
            if !times.is_empty() {
                return (times.iter().sum::<Duration>() / times.len() as u32, weight);
            }
        }
        (Default::default(), 0)
    }

    pub fn applied(&self) -> usize {
        self.single_applied().unwrap_or_default() + self.mth_applied().1
    }

    pub fn mth_applied(&self) -> (usize, usize) {
        let mut mth_threads = self.threads.len();
        let mut mth_applied = self.threads.iter().map(|(_, info)| info.total).sum::<usize>();
        if self.threads.get(&0).is_some() {
            mth_threads -= 1;
            mth_applied -= self.single_applied().unwrap_or_default();
        }
        (mth_threads, mth_applied)
    }

    pub fn single_applied(&self) -> Option<usize> {
        self.threads.get(&0).map(|info| if info.time != Default::default() {
            Some(info.total)
        } else {
            None
        })?
    }

    pub fn group_info(&self) -> String {
        self.group.info()
    }

    pub fn time_info(&self, limit_time: bool) -> String {
        let limit_time = if limit_time { format!("/{}", self.max_time.as_millis()) } else { "".to_string() };
        let mut group_info = "".to_string();
        if self.group.time > Default::default() {
            group_info = format!(
                "G{}{} ",
                self.group.time.as_millis(),
                if self.group.wait > Default::default() {
                    format!("(W{})", self.group.wait.as_millis())
                } else {
                    "".into()
                }
            );
        }
        let mut merge_info = "".to_string();
        if self.merge.mth_time > Default::default() {
            merge_info = format!(
                "M{}{} ",
                self.merge.mth_time.as_millis(),
                if self.merge.extra_merge_time > Default::default() {
                    format!("(E{})", self.merge.extra_merge_time.as_millis())
                } else {
                    "".into()
                }
            );
        }
        let mut single_info = "".to_string();
        let single_thread_time = self.threads.get(&0).map(|i| i.time).unwrap_or_default();
        if single_thread_time > Default::default() {
            single_info = format!("S{} ", single_thread_time.as_millis());
        }
        format!(
            "{}{limit_time}ms({group_info}{merge_info}{single_info}F{})ms",
            self.time.as_millis(),
            self.finalize.as_millis(),
        )
    }

    pub fn time_debug(&self, limit_time: bool) -> String {
        let limit_time = if limit_time { format!("/{:?}", self.max_time) } else { "".to_string() };
        let mut group_info = "".to_string();
        if self.group.time > Default::default() {
            group_info = format!(
                "G{:?}{} ",
                self.group.time,
                if self.group.wait > Default::default() {
                    format!("(W{:?})", self.group.wait)
                } else {
                    "".into()
                }
            );
        }
        let mut merge_info = "".to_string();
        if self.merge.mth_time > Default::default() {
            merge_info = format!(
                "M{:?}{} ",
                self.merge.mth_time,
                if self.merge.extra_merge_time > Default::default() {
                    format!("(E{:?})", self.merge.extra_merge_time)
                } else {
                    "".into()
                }
            );
        }
        let mut single_info = "".to_string();
        let single_thread_time = self.threads.get(&0).map(|i| i.time).unwrap_or_default();
        if single_thread_time > Default::default() {
            single_info = format!("S{:?} ", single_thread_time);
        }
        format!(
            "{:?}{limit_time}({group_info}{merge_info}{single_info}F{:?})",
            self.time,
            self.finalize,
        )
    }

    pub fn tx_info(&self) -> String {
        let (mut single_applied, single_invalid) = self.threads
            .get(&0)
            .map(|i| (i.total, i.invalid + i.future_or_exhausted))
            .unwrap_or_default();
        single_applied = single_applied.saturating_sub(single_invalid);
        let mut total_applied = 0;
        let mut total_invalid = 0;
        self.threads
            .values()
            .for_each(|i| {
                total_applied += i.total;
                total_invalid += i.invalid;
                total_invalid += i.future_or_exhausted;
            });
        total_applied = total_applied.saturating_sub(total_invalid);
        let total_extrinsic = total_applied + total_invalid + self.inherent.total;
        let mth_applied = total_applied.saturating_sub(single_applied);
        let mth_invalid = total_invalid.saturating_sub(single_invalid);
        format!("extrinsics [({mth_applied}/{mth_invalid})/({single_applied}/{single_invalid})/{total_extrinsic} native: {}]", self.native)
    }
}

#[derive(Debug, Clone, Default)]
pub struct GroupTxInput {
    /// Max time to wait for pool ready.
    pub wait_pool: Duration,
    /// Time limit for full time getting transaction from pool.
    pub max_time: Duration,
    /// Group max threads target.
    pub thread_limit: usize,
    /// linear transaction limit based on time(groups(max) + single).
    pub linear_tx_limit: usize,
    /// Minimal single thread transaction number(if exists).
    pub single_tx_min: usize,
    /// Limit total transactions get from pool.
    pub total_tx_limit: usize,
    /// Limit block size when group transactions.
    pub block_size_limit: usize,
}

impl GroupTxInput {
    pub fn info(&self) -> String {
        format!(
            "thread {} linear {} single {} total {}",
            self.thread_limit,
            self.linear_tx_limit,
            self.single_tx_min,
            self.total_tx_limit,
        )
    }
}

pub struct GroupTxOutput<A: TransactionPool> {
    /// Parallel execute transactions.
    pub groups: Vec<Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>>,
    /// Tailing single thread execute transactions.
    pub single: Vec<(usize, Arc<<A as TransactionPool>::InPoolTransaction>)>,
    pub info: GroupInfo,
}

#[derive(Debug, Default, Clone)]
pub struct GroupInfo {
    pub input: GroupTxInput,
    /// Total transaction number(groups and single).
    pub tx_count: usize,
    /// Filtered transaction
    pub filtered: usize,
    /// Raw groups number for parallel groups(then it is merged to `groups` by `thread_limit`)  .
    pub raw_groups: usize,
    /// Final groups length.
    pub groups: usize,
    /// Time for get transactions full process.
    pub time: Duration,
    /// time for waiting transaction pool response before start groups.
    pub wait: Duration,
    /// time for groups sort.
    pub sort: Duration,
}

impl GroupInfo {
    pub fn info(&self) -> String {
        if self.tx_count == 0 && self.filtered == 0 {
            return "".to_string();
        }
        let filter_info = if self.filtered > 0 {
            format!("(filtered {})", self.filtered)
        } else {
            "".to_string()
        };
        format!(
            "Group {} tx in {}ms(W{}μs S{}μs){}(groups {}->{})(Input: {})",
            self.tx_count,
            self.time.as_millis(),
            self.wait.as_micros(),
            self.sort.as_micros(),
            filter_info,
            self.raw_groups,
            self.groups,
            self.input.info()
        )
    }
}

#[derive(Debug, Clone)]
pub struct ThreadExecutionInfo<B: BlockT> {
    pub thread: usize,
    /// total transactions.
    pub total: usize,
    /// failed but valid transactions.
    pub future_or_exhausted: usize,
    /// failed an invalid transactions.
    pub invalid: usize,
    /// thread execute time.
    pub time: Duration,
    /// thread extra execution time.
    pub extend_time: Duration,
    /// Finish thread time.
    pub finish_time: Duration,
    /// transaction hashes(future transactions are not filtered).
    pub transactions: Vec<B::Hash>,
}

impl<B: BlockT> Default for ThreadExecutionInfo<B> {
    fn default() -> Self {
        Self {
            thread: 0,
            total: 0,
            invalid: 0,
            future_or_exhausted: 0,
            time: Default::default(),
            extend_time: Default::default(),
            finish_time: Default::default(),
            transactions: vec![],
        }
    }
}

impl<B: BlockT> ThreadExecutionInfo<B> {
    pub fn new(thread: usize) -> Self {
        Self { thread, ..Default::default() }
    }

    pub fn applied(&self) -> usize {
        self.total.saturating_sub(self.invalid).saturating_sub(self.future_or_exhausted)
    }

    pub fn avg_time(&self) -> (Duration, usize) {
        let applied = self.applied();
        if applied == 0 {
            (Default::default(), 0)
        } else {
            // raw each transaction apply time
            let avg_tme = Duration::from_micros(self.time.as_micros() as u64 / applied as u64);
            let weight = applied;
            (avg_tme, weight)
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Encode, Decode)]
pub struct MergeInfo {
    /// Average merge time(merge time / transaction number).Just for estimate time.
    pub avg_merge_micros: u128,
    pub mth_time: Duration,
    /// Record merge_time since all parallel threads execute finished.
    pub extra_merge_time: Duration
}
